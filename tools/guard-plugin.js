/**
 * dsh-toolcall-id-guard — fill in tool-call ids the provider never sent.
 *
 * Why: some OpenAI-compatible upstreams stream `tool_calls` without an `id`
 * (some OpenAI-compatible gateways and free-tier routes do this, typically on
 * large prompts). Pi-ai then stores `id: ""`, DSH propagates that empty string
 * into the assistant message block, the `tool/call` event and the awaiting
 * `tool/result`, and `@deepseek-ai/dsh-session-format-v3-to-v4` rejects the
 * append with
 *   `format v4 tool/result at seq N requires toolCallId matching its tool source`
 * which aborts the whole turn ("本轮运行失败").
 *
 * Fix: rewrite `tool-call-delta` and `block-end` chunks that carry an empty
 * tool-call id, inside the `llm/stream` waterfall — the documented interception
 * point for provider streams (the harness's own invariant uses it the same way,
 * see `@deepseek-ai/dsh-llm/lib/invariant.js`). One id per block, shared by the
 * deltas and the closing block, so the assembled assistant message, the
 * `tool/call` event, the `tool/result` event and the provider-facing transcript
 * all agree. Non-empty ids are never touched, and nothing is mutated in place.
 *
 * @module dsh-toolcall-id-guard
 */
import { createHash, randomUUID } from 'node:crypto';
import { appendFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

/** Cordis plugin name used by Loader diagnostics. */
export const name = 'dsh-toolcall-id-guard';

/**
 * Synthesize an OpenAI-shaped tool-call id: `call_` + 24 hex chars, the same
 * shape the providers themselves emit (`call_e493f1652f7645318d46b405`).
 *
 * @param seed - per-stream seed, so two streams never collide.
 * @param index - stream block index, so two calls in one message never collide.
 * @returns a non-empty, log-safe tool call id.
 */
export function synthesizeToolCallId(seed, index) {
	return `call_${createHash('sha256').update(`${seed}:${index}`).digest('hex').slice(0, 24)}`;
}

/** @param value - candidate id. @returns whether it is a usable id. */
const isId = (value) => typeof value === 'string' && value.length > 0;

/**
 * Wrap one provider chunk stream, filling in missing tool-call ids.
 *
 * @param source - provider chunks (the `next()` value of the `llm/stream` waterfall).
 * @param options - `seed` for id synthesis, `onFix` observer called once per filled id.
 * @returns the same stream with every tool-call id present and consistent.
 */
export async function* normalizeStream(source, options = {}) {
	const seed = options.seed ?? randomUUID();
	const onFix = typeof options.onFix === 'function' ? options.onFix : undefined;
	/** block index -> id already used for that block. */
	const ids = new Map();
	const synthesize = (index) => {
		let id = ids.get(index);
		if (id === undefined) {
			id = synthesizeToolCallId(seed, index);
			ids.set(index, id);
		}
		return id;
	};
	for await (const chunk of source) {
		if (chunk === null || typeof chunk !== 'object') {
			yield chunk;
			continue;
		}
		if (chunk.type === 'tool-call-delta') {
			const known = ids.get(chunk.index);
			// a real id from the provider always wins, and is remembered for the block
			if (isId(chunk.id)) {
				if (known === undefined) {
					ids.set(chunk.index, chunk.id);
					yield chunk;
					continue;
				}
				yield known === chunk.id ? chunk : { ...chunk, id: known };
				continue;
			}
			const synthesized = known === undefined;
			const id = known ?? synthesize(chunk.index);
			if (synthesized) onFix?.({ stage: 'tool-call-delta', index: chunk.index, name: chunk.name, id });
			yield { ...chunk, id };
			continue;
		}
		if (chunk.type === 'block-end' && chunk.block?.type === 'tool-call') {
			const block = chunk.block;
			// the closing block is what DSH persists: keep a real id, otherwise fill it in
			const id = isId(block.id) ? block.id : (ids.get(chunk.index) ?? synthesize(chunk.index));
			ids.set(chunk.index, id);
			if (!isId(block.id)) onFix?.({ stage: 'block-end', index: chunk.index, name: block.name, id });
			yield block.id === id ? chunk : { ...chunk, block: { ...block, id } };
			continue;
		}
		yield chunk;
	}
}

/** Audit trail path: `$DSH_HOME/toolcall-id-guard.log`. */
function auditPath() {
	return join(process.env.DSH_HOME ?? join(homedir(), '.dsh'), 'toolcall-id-guard.log');
}

/** Record one filled id. Never throws: bookkeeping must not break a stream. */
function reportFix(ctx, info) {
	try {
		ctx?.logger?.info?.(`[${name}] filled tool-call id ${info.id} (block ${info.index}${info.name ? `, ${info.name}` : ''})`);
	} catch {}
	try {
		appendFileSync(
			auditPath(),
			`${new Date().toISOString()} filled ${info.id} block=${info.index} name=${info.name ?? '?'} stage=${info.stage}\n`,
		);
	} catch {}
}

/**
 * Record that the plugin was mounted.
 *
 * Reported separately from `reportFix` because the two answer different
 * questions: this line proves the entry is live in the loader, while a `filled`
 * line proves a stream actually carried a missing id. A silent audit file is
 * ambiguous without it — no line may mean "never loaded" or "loaded, nothing to
 * fix", and those need opposite responses.
 */
function reportMounted(ctx) {
	try {
		ctx?.logger?.info?.(`[${name}] mounted: filling empty tool-call ids on llm/stream`);
	} catch {}
	try {
		appendFileSync(
			auditPath(),
			`${new Date().toISOString()} mounted pid=${process.pid}\n`,
		);
	} catch {}
}

/**
 * Install the normalizer around every provider stream.
 *
 * @param ctx - cordis context of this plugin.
 */
export function apply(ctx) {
	ctx.on(
		'llm/stream',
		(_options, next) => {
			const seed = randomUUID();
			return normalizeStream(next(), { seed, onFix: (info) => reportFix(ctx, info) });
		},
		{ global: true },
	);
	reportMounted(ctx);
}
