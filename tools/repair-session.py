#!/usr/bin/env python3
"""Repair DSH v4 session logs whose tool-call ids are missing or inconsistent.

THE FAILURE
    Some OpenAI-compatible providers stream `tool_calls` without an `id`
    (observed most often on large prompts); pi-ai degrades a
    missing id to `""` (its `block.id = toolCall.id || ""`), and DSH's stream
    validator only enforces `typeof id === "string"`, which `""` satisfies. The
    empty id is then persisted in three places, and each is independently fatal:

      1. assistant/message -> message.content[].id
         Rejected by the format validator:
           "tool call id requires a nonempty string"

      2. assistant/message -> data.stream[].chunk (tool-call-delta / block-end)
         Rejected when the session is next appended to, because persistence
         replays the embedded stream and requires it to reproduce the stored
         message exactly:
           "seed assistant/message at index N content disagrees with its embedded stream"

      3. tool/call -> data.callId
         Makes the matching tool/result unappendable:
           "format v4 tool/result at seq N requires toolCallId matching its tool source"

    A run that fixed only (1) and (3) leaves (2) blank, and the log is then
    accepted on read but rejected on the next append — the worst kind of
    half-repair. This tool therefore treats the three as ONE invariant.

    `message.content[].id` is authoritative. Where it is missing, an id is
    synthesized; where the embedded stream disagrees with it, the stream is
    rewritten to match. A `tool/call` is paired to its block by
    (turn, step, name, arguments) — all four, so parallel calls with equal names
    still pair by their distinct argument strings.

    Synthesized ids are deterministic: `call_` + 24 hex chars from
    (session id, turn, step, block position), shaped like the gateway's own ids
    (`call_e493f1652f7645318d46b405`). Deterministic so a second run is a no-op and
    two runs cannot disagree.

    Nothing else may change, and that is checked rather than asserted: the set of
    mutated rows must equal the set of rows whose ids were inconsistent, every
    row must round-trip byte-identically through this script's serializer, and a
    synthesized id must not collide with an existing one.

Usage:
    python3 repair_session.py [--apply] <session-dir> [<session-dir> ...]
    python3 repair_session.py --apply --all [<sessions-root>]

Without --apply it prints the plan and writes nothing.
"""
import argparse
import hashlib
import json
import os
import shutil
import sys
import time

import zstandard as zstd

def dsh_home() -> str:
    """The harness home, honouring DSH_HOME exactly as the harness does."""
    return os.environ.get("DSH_HOME") or os.path.expanduser("~/.dsh")


BACKUP_DIR = os.path.join(dsh_home(), "session-repair-backups")
LOG_NAME = "session.v4.jsonl.zstd"
SESSIONS_ROOT = os.path.join(dsh_home(), "sessions")

# A `tool/call` may claim a block's id only if all of these agree. Order is
# irrelevant; `arguments` is what separates parallel calls that share a name.
PAIR_FIELDS = ("name", "arguments")


def synth_id(session_id: str, turn, step, position: int) -> str:
    """OpenAI-shaped id: `call_` + 24 hex chars, unique per (session, coords)."""
    seed = f"{session_id}\x1f{turn}\x1f{step}\x1f{position}"
    return "call_" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24]


def empty_id(value) -> bool:
    """True when a tool-call id field is absent or blank."""
    return not (isinstance(value, str) and value)


def load(path: str):
    """Read a multi-frame zstd log, proving every row round-trips byte-exactly."""
    reader = zstd.ZstdDecompressor().stream_reader(open(path, "rb"))
    lines = reader.read().split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    rows = []
    for index, line in enumerate(lines):
        obj = json.loads(line)
        reserialized = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if reserialized != line:
            raise SystemExit(
                f"[fatal] {path}: row {index} does not round-trip; refusing to rewrite "
                "a log whose serialization I do not reproduce exactly"
            )
        rows.append(obj)
    return rows


def dump(rows) -> bytes:
    return b"".join(
        json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
        for row in rows
    )


def effective_ids(session_id, data):
    """Authoritative id per tool-call block position, plus which positions were blank.

    Position is the index in `message.content`. A blank id is synthesized; the
    synthesized value is returned so the stream and any `tool/call` can use the
    same one.
    """
    content = (data.get("message") or {}).get("content")
    if not isinstance(content, list):
        return {}, set()
    turn, step = data.get("turn"), data.get("step")
    effective = {}
    synthesized = set()
    for position, block in enumerate(content):
        if not isinstance(block, dict) or block.get("type") != "tool-call":
            continue
        if empty_id(block.get("id")):
            effective[position] = synth_id(session_id, turn, step, position)
            synthesized.add(position)
        else:
            effective[position] = block["id"]
    return effective, synthesized


def stream_position_map(stream):
    """Map a stream block index to its position among content blocks.

    The assembler emits one content block per `block-start`, in the order the
    block-starts appear, so the k-th block-start (counting ALL types, not just
    tool calls) is content position k. Counting only tool calls would shift every
    id by the number of preceding text/reasoning blocks.
    """
    by_index = {}
    next_position = 0
    for member in stream:
        if not isinstance(member, dict):
            continue
        if member.get("type") == "tool-call-chunks":
            index = member.get("index")
            if index not in by_index:
                by_index[index] = next_position
                next_position += 1
            continue
        chunk = member.get("chunk")
        if not isinstance(chunk, dict):
            continue
        if chunk.get("type") == "block-start":
            if chunk.get("index") not in by_index:
                by_index[chunk["index"]] = next_position
                next_position += 1
    return by_index


def stream_id_sites(stream, by_index):
    """Yield (container, key, effective-position) for every stream id carrier."""
    for member in stream:
        if not isinstance(member, dict):
            continue
        if member.get("type") == "tool-call-chunks":
            # Compact form: one id covers a run of deltas at its block index.
            yield member, "id", by_index.get(member.get("index"))
            continue
        chunk = member.get("chunk")
        if not isinstance(chunk, dict):
            continue
        if chunk.get("type") == "tool-call-delta":
            yield chunk, "id", by_index.get(chunk.get("index"))
            continue
        if chunk.get("type") == "block-end":
            block = chunk.get("block")
            if isinstance(block, dict) and block.get("type") == "tool-call":
                yield block, "id", by_index.get(chunk.get("index"))


def plan(path: str):
    """Return (rows, new_rows, changed, summary); raise when a call cannot be paired."""
    session_id = os.path.basename(os.path.dirname(path))
    rows = load(path)
    if rows[0].get("type") != "session":
        raise SystemExit(f"[fatal] {path}: row 0 is not the session header")
    events = rows[1:]
    if [event["seq"] for event in events] != list(range(len(events))):
        raise SystemExit(f"[fatal] {path}: seq is not dense")

    # Pass 1: per assistant message, the authoritative ids and what disagrees.
    repairs = []  # (row index, effective, synthesized, content, stream, by_index)
    for index, event in enumerate(events):
        if event["type"] != "assistant/message":
            continue
        data = event.get("data") or {}
        effective, synthesized = effective_ids(session_id, data)
        if not effective:
            continue
        stream = data.get("stream")
        by_index = stream_position_map(stream) if isinstance(stream, list) else {}
        content = data["message"]["content"]

        # A row needs rewriting when a content id was blank, or when any stream
        # id disagrees with the authoritative id for its position.
        stream_dirty = False
        if isinstance(stream, list):
            for container, key, position in stream_id_sites(stream, by_index):
                want = effective.get(position)
                if want is not None and container.get(key) != want:
                    stream_dirty = True
                    break
        if not synthesized and not stream_dirty:
            continue
        repairs.append((index + 1, effective, synthesized, content, stream, by_index))

    # Guard: a synthesized id must not collide with an id already in the log.
    existing = set()
    for event in events:
        data = event.get("data") or {}
        if event["type"] == "assistant/message":
            for block in (data.get("message") or {}).get("content") or []:
                if isinstance(block, dict) and block.get("type") == "tool-call" and block.get("id"):
                    existing.add(block["id"])
        elif event["type"] == "tool/call" and data.get("callId"):
            existing.add(data["callId"])
    for _index, effective, synthesized, _content, _stream, _by_index in repairs:
        for position in synthesized:
            if effective[position] in existing:
                raise SystemExit(
                    f"[fatal] {path}: synthesized id {effective[position]} collides with an existing id"
                )

    # Pass 2: pair every blank `tool/call` with the block that advertised it.
    by_coords = {}
    for index, event in enumerate(events):
        if event["type"] != "assistant/message":
            continue
        data = event.get("data") or {}
        effective, synthesized = effective_ids(session_id, data)
        if effective:
            by_coords[(data.get("turn"), data.get("step"))] = (effective, data["message"]["content"])

    call_fixes = []
    for index, event in enumerate(events):
        if event["type"] != "tool/call":
            continue
        data = event.get("data") or {}
        if not empty_id(data.get("callId")):
            continue
        key = (data.get("turn"), data.get("step"))
        entry = by_coords.get(key)
        if entry is None:
            raise SystemExit(
                f"[fatal] {path}: tool/call seq {event['seq']} has no assistant block to pair with"
            )
        effective, content = entry
        matched = [
            position
            for position in effective
            if all(content[position].get(field) == data.get(field) for field in PAIR_FIELDS)
        ]
        if len(matched) != 1:
            raise SystemExit(
                f"[fatal] {path}: tool/call seq {event['seq']} matches {len(matched)} blocks, need exactly 1"
            )
        call_fixes.append((index + 1, effective[matched[0]]))

    if not repairs and not call_fixes:
        return rows, rows, [], {"session_id": session_id, "ids": 0, "calls": 0, "rows": [], "stream": 0}

    new_rows = [json.loads(json.dumps(row, ensure_ascii=False)) for row in rows]
    stream_rewrites = 0

    for row_index, effective, synthesized, _content, _stream, _by_index in repairs:
        data = new_rows[row_index]["data"]
        content = data["message"]["content"]
        for position in synthesized:
            content[position]["id"] = effective[position]
        # Re-derive the stream sites from the COPY. Holding references taken
        # during planning would mutate the original rows instead, leaving the
        # written file inconsistent with its own embedded stream.
        stream = data.get("stream")
        if not isinstance(stream, list):
            continue
        by_index = stream_position_map(stream)
        for container, key, position in stream_id_sites(stream, by_index):
            want = effective.get(position)
            if want is not None and container.get(key) != want:
                container[key] = want
                stream_rewrites += 1

    for row_index, new_id in call_fixes:
        new_rows[row_index]["data"]["callId"] = new_id

    # Self-check: the repaired rows must now be internally consistent. This is
    # the exact agreement persistence demands, so a bug here cannot reach disk.
    for row_index, effective, *_rest in repairs:
        data = new_rows[row_index]["data"]
        for position, block in enumerate(data["message"]["content"]):
            if isinstance(block, dict) and block.get("type") == "tool-call" and position in effective:
                if block.get("id") != effective[position]:
                    raise SystemExit(f"[fatal] {path}: content id at position {position} not applied")
        stream = data.get("stream")
        if not isinstance(stream, list):
            continue
        by_index = stream_position_map(stream)
        for container, key, position in stream_id_sites(stream, by_index):
            want = effective.get(position)
            if want is not None and container.get(key) != want:
                raise SystemExit(
                    f"[fatal] {path}: stream id at position {position} is "
                    f"{container.get(key)!r}, expected {want!r}"
                )

    # Guard: exactly the rows that carried an inconsistent id may differ.
    changed = [index for index, (before, after) in enumerate(zip(rows, new_rows)) if before != after]
    expected = sorted([row_index for row_index, *_rest in repairs] + [row_index for row_index, _id in call_fixes])
    if changed != expected:
        raise SystemExit(f"[fatal] {path}: unexpected changed rows {changed} != {expected}")

    summary = {
        "session_id": session_id,
        "ids": sum(len(synthesized) for _i, _e, synthesized, *_rest in repairs),
        "calls": len(call_fixes),
        "rows": changed,
        "stream": stream_rewrites,
    }
    return rows, new_rows, changed, summary


def repair(path: str, apply: bool) -> int:
    rows, new_rows, changed, summary = plan(path)
    if not summary["rows"]:
        print(f"[skip] {summary['session_id']}: tool-call ids already consistent")
        return 0
    print(
        f"[plan] {summary['session_id']}: {summary['ids']} id(s) synthesized, "
        f"{summary['calls']} tool/call fix(es), {summary['stream']} stream field(s), "
        f"rows changed {summary['rows']}"
    )
    if not apply:
        return summary["calls"]

    os.makedirs(BACKUP_DIR, exist_ok=True)
    backup = os.path.join(
        BACKUP_DIR, f"{summary['session_id']}.v4.jsonl.zstd.bak-{int(time.time() * 1000)}"
    )
    shutil.copy2(path, backup)

    # One frame, like the harness writes per flush; the reader walks frames, so a
    # single frame is structurally valid and simpler to verify.
    out = dump(new_rows)
    tmp = f"{path}.repair-tmp"
    with open(tmp, "wb") as handle:
        handle.write(zstd.ZstdCompressor(level=10).compress(out))
    os.replace(tmp, path)
    os.chmod(path, 0o600)

    if load(path) != new_rows:
        raise SystemExit(f"[fatal] {path}: verification re-read differs; backup at {backup}")
    print(f"[done] {summary['session_id']}: written, backup {os.path.basename(backup)}")
    return summary["calls"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="write; without it, plan only")
    parser.add_argument("--all", action="store_true", help="walk the whole sessions root")
    parser.add_argument("dirs", nargs="*")
    args = parser.parse_args()

    dirs = list(args.dirs)
    if args.all:
        root = args.dirs[0] if args.dirs else SESSIONS_ROOT
        for dirpath, _dirnames, filenames in os.walk(root):
            if LOG_NAME in filenames:
                dirs.append(dirpath)
    if not dirs:
        parser.error("pass at least one session directory, or --all")

    total = 0
    for directory in dirs:
        path = os.path.join(directory, LOG_NAME)
        if not os.path.isfile(path):
            continue
        total += repair(path, args.apply)
    print(("[applied] " if args.apply else "[dry-run] ") + f"{total} tool/call id(s) fixed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
