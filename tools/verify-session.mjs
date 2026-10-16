#!/usr/bin/env node
/**
 * Validate DSH v4 session logs against the harness's own invariants.
 *
 * Two independent acceptance gates, because they catch different failures:
 *
 *  1. STRUCTURE — the log restores through `sessionFormatCatalog.createRestore`,
 *     the same read path persistence uses before a session may be resumed. This
 *     catches a log that is internally inconsistent.
 *
 *  2. APPEND SAFETY — no tool-call id anywhere is empty. This is the invariant
 *     that actually bricked the sessions: the stream validator only requires
 *     `typeof id === "string"`, so `""` flows into the assistant block and the
 *     `tool/call` event, and then the harness refuses to append the matching
 *     `tool/result`:
 *
 *       format v4 tool/result at seq N requires toolCallId matching its tool source
 *
 *     A log can pass gate 1 (an incomplete tail is legal) and still be
 *     permanently stuck, which is exactly the state the user hit.
 *
 * Decompression uses the `zstdcat` CLI: DSH logs are concatenated zstd frames
 * (one per flush), and Node's `zstdDecompressSync` decodes only the first frame.
 * An earlier sweep used it and read 0 events out of a 646-event session while
 * reporting "ALL CLEAN".
 *
 * Usage:
 *   node verify-session.mjs <session-dir> [...]
 *   node verify-session.mjs --all [<sessions-root>]
 *
 * Exit 0 when every log passes both gates.
 */
import { execFileSync } from 'node:child_process';
import { existsSync, readdirSync, realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';

const LOG_NAME = 'session.v4.jsonl.zstd';

/**
 * Locate the harness's own session-format package.
 *
 * Resolved rather than hard-coded so this works on any install layout (npm
 * global, pnpm, Homebrew, an Electron bundle). `createRequire` walks the normal
 * Node resolution chain, which finds the package exactly where the harness
 * loads it from.
 */
function resolveCatalog() {
	const require = createRequire(import.meta.url);
	try {
		return require.resolve('@deepseek-ai/dsh-session-format-catalog/lib/index.js');
	} catch {}
	// Fall back to the caller's DSH install, following the `dsh` binary if present.
	const candidates = [];
	const fromPath = process.env.DSH_INSTALL_ANCHOR;
	if (fromPath) candidates.push(fromPath);
	try {
		const dshBin = execFileSync('which', ['dsh'], { encoding: 'utf8' }).trim();
		if (dshBin) {
			// <prefix>/bin/dsh -> <prefix>/lib/node_modules/@deepseek-ai/dsh
			candidates.push(dirname(dirname(realpathSync(dshBin))));
		}
	} catch {}
	for (const candidate of candidates) {
		const guess = join(
			candidate,
			'node_modules/@deepseek-ai/dsh-session-format-catalog/lib/index.js',
		);
		if (existsSync(guess)) return guess;
	}
	throw new Error(
		'cannot locate @deepseek-ai/dsh-session-format-catalog; set DSH_INSTALL_ANCHOR to the ' +
			'directory containing node_modules/@deepseek-ai/dsh-session-format-catalog',
	);
}

/** Decompress every zstd frame of a DSH log via the zstd CLI. */
function readLog(path) {
	return execFileSync('zstdcat', [path], { maxBuffer: 512 * 1024 * 1024 }).toString('utf8');
}

/** Restore one log through the harness's own read path. */
function restore(catalog, text) {
	const rows = text
		.split('\n')
		.filter((line) => line.length > 0)
		.map((line) => JSON.parse(line));
	const [headerRow, ...eventRows] = rows;
	if (headerRow === undefined) throw new Error('empty log');
	catalog.readHeader(headerRow);
	const restoreState = catalog.createRestore(headerRow, { validation: 'current' });
	for (const row of eventRows) restoreState.decodeRow(row);
	return restoreState.finish();
}

/**
 * Collect every empty tool-call id, plus the open tool calls at the tail.
 *
 * An "open" tool call is one advertised by an assistant message (or emitted as
 * `tool/call`) that has no matching `tool/result`. DSH closes those on resume,
 * and each closer carries the call's id — so an empty id here is fatal.
 */
function inspectAppendSafety(events) {
	const problems = [];
	const advertised = new Map(); // turn:step -> Map(id -> name)
	const started = new Set(); // ids with a tool/call
	const settled = new Set(); // ids with a tool/result
	for (const event of events) {
		const data = event.data ?? {};
		if (event.type === 'assistant/message') {
			const key = `${data.turn}:${data.step}`;
			if (!advertised.has(key)) advertised.set(key, new Map());
			for (const block of data.message?.content ?? []) {
				if (block?.type !== 'tool-call') continue;
				if (typeof block.id !== 'string' || block.id.length === 0) {
					problems.push(`seq ${event.seq}: assistant tool-call block has an empty id (name=${block.name})`);
					continue;
				}
				advertised.get(key).set(block.id, block.name);
			}
			continue;
		}
		if (event.type === 'tool/call') {
			if (typeof data.callId !== 'string' || data.callId.length === 0) {
				problems.push(`seq ${event.seq}: tool/call has an empty callId (name=${data.name})`);
				continue;
			}
			if (started.has(data.callId)) problems.push(`seq ${event.seq}: repeat tool/call ${data.callId}`);
			started.add(data.callId);
			const block = advertised.get(`${data.turn}:${data.step}`)?.get(data.callId);
			if (block === undefined) problems.push(`seq ${event.seq}: callId ${data.callId} is not advertised`);
			else if (block !== data.name) problems.push(`seq ${event.seq}: callId ${data.callId} names ${block}, call names ${data.name}`);
			continue;
		}
		if (event.type === 'tool/result') {
			const message = data.message ?? {};
			const id = message.toolCallId;
			if (typeof id !== 'string' || id.length === 0) {
				problems.push(`seq ${event.seq}: tool/result has an empty toolCallId`);
				continue;
			}
			if (id !== message.source?.callId) problems.push(`seq ${event.seq}: toolCallId != source.callId`);
			if (!started.has(id)) problems.push(`seq ${event.seq}: toolCallId ${id} has no tool/call`);
			settled.add(id);
		}
	}
	const open = [...started].filter((id) => !settled.has(id));
	return { problems, open };
}

const { sessionFormatCatalog } = await import(resolveCatalog());

/** The harness home, honouring DSH_HOME exactly as the harness does. */
function dshHome() {
	return process.env.DSH_HOME ?? join(homedir(), '.dsh');
}

const args = process.argv.slice(2);
let dirs;
if (args[0] === '--all') {
	const root = args[1] ?? join(dshHome(), 'sessions');
	dirs = [];
	const walk = (dir) => {
		for (const entry of readdirSync(dir, { withFileTypes: true })) {
			if (entry.isDirectory()) walk(join(dir, entry.name));
		}
		if (existsSync(join(dir, LOG_NAME))) dirs.push(dir);
	};
	walk(root);
} else {
	dirs = args;
}
if (dirs.length === 0) {
	console.error('usage: verify-session.mjs <session-dir> [...] | --all [<root>]');
	process.exit(2);
}

let failed = 0;
let checked = 0;
for (const dir of dirs.sort()) {
	const path = join(dir, LOG_NAME);
	if (!existsSync(path)) {
		console.log(`skip ${dir.split('/').pop()} (no log)`);
		continue;
	}
	checked += 1;
	const name = dir.split('/').pop();
	try {
		const text = readLog(path);
		const artifact = restore(sessionFormatCatalog, text);
		const { problems, open } = inspectAppendSafety(artifact.events);
		const tail = artifact.events.at(-1) ?? { seq: -1, type: '(empty)' };
		const summary = `events=${artifact.events.length} lastSeq=${tail.seq} type=${tail.type} openCalls=${open.length}`;
		if (problems.length === 0) {
			console.log(`OK   ${name}  ${summary}`);
		} else {
			failed += 1;
			console.log(`FAIL ${name}  ${summary}\n       ${problems.slice(0, 6).join('\n       ')}`);
		}
	} catch (error) {
		failed += 1;
		console.log(`FAIL ${name}  ${error.constructor.name}: ${error.message}`);
	}
}
console.log(
	failed === 0
		? `ALL CLEAN — ${checked} log(s) pass structure + append-safety`
		: `${failed} of ${checked} log(s) still blocked`,
);
process.exit(failed === 0 ? 0 : 1);
