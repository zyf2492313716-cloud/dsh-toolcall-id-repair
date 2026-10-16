/**
 * Unit tests for the `llm/stream` tool-call id normalizer.
 *
 * The interesting case is the one that bricked real sessions: the provider
 * streamed two tool calls
 * whose `tool-call-delta` and `block-end` both carried `id: ""`.
 */
import test from 'node:test';
import assert from 'node:assert/strict';

import { normalizeStream, synthesizeToolCallId } from './guard-plugin.js';

const ID_SHAPE = /^call_[0-9a-f]{24}$/;

async function collect(stream) {
	const out = [];
	for await (const chunk of stream) out.push(chunk);
	return out;
}

async function* streamOf(chunks) {
	for (const chunk of chunks) yield chunk;
}

const delta = (index, id, name = 'bash', argumentsDelta = '{}') => ({
	type: 'tool-call-delta',
	index,
	id,
	name,
	argumentsDelta,
});

const blockEnd = (index, block) => ({ type: 'block-end', index, block });

/** Mirror of the DSH assembler's contract: the closing block is what persists. */
function assembleToolCalls(chunks) {
	return chunks
		.filter((c) => c.type === 'block-end' && c.block?.type === 'tool-call')
		.map((c) => c.block);
}

test('a real provider id is never touched (idempotent, same objects)', async () => {
	const chunks = [
		{ type: 'block-start', index: 0, blockType: 'tool-call' },
		delta(0, 'call_e493f1652f7645318d46b405', 'bash', '{"command":"ls"}'),
		blockEnd(0, { type: 'tool-call', id: 'call_e493f1652f7645318d46b405', name: 'bash', arguments: '{"command":"ls"}' }),
		{ type: 'usage', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
		{ type: 'finish', reason: { kind: 'stop' } },
	];
	const out = await collect(normalizeStream(streamOf(chunks), { seed: 'seed-a' }));
	assert.equal(out.length, chunks.length);
	out.forEach((chunk, i) => assert.equal(chunk, chunks[i], `chunk ${i} must pass through untouched`));
});

test('empty ids in both the delta and the closing block get one shared id', async () => {
	const chunks = [
		{ type: 'block-start', index: 1, blockType: 'tool-call' },
		delta(1, ''),
		{ type: 'block-start', index: 2, blockType: 'tool-call' },
		delta(2, ''),
		blockEnd(1, { type: 'tool-call', id: '', name: 'bash', arguments: '{"command":"a"}' }),
		blockEnd(2, { type: 'tool-call', id: '', name: 'bash', arguments: '{"command":"b"}' }),
		{ type: 'finish', reason: { kind: 'stop' } },
	];
	const fixes = [];
	const out = await collect(normalizeStream(streamOf(chunks), { seed: 'seed-b', onFix: (i) => fixes.push(i) }));
	const deltas = out.filter((c) => c.type === 'tool-call-delta');
	const blocks = assembleToolCalls(out);

	assert.deepEqual(deltas.map((d) => d.id), [blocks[0].id, blocks[1].id], 'delta and block ids must agree per index');
	for (const block of blocks) assert.match(block.id, ID_SHAPE);
	assert.notEqual(blocks[0].id, blocks[1].id, 'two calls in one message must not share an id');
	assert.ok(blocks[0].id.length > 0 && blocks[1].id.length > 0);
	// the untouched chunks keep their identity
	assert.equal(out[0], chunks[0]);
	assert.equal(out[out.length - 1], chunks[chunks.length - 1]);
	// both stages of both blocks are reported once each
	assert.equal(fixes.length, 4, JSON.stringify(fixes));
	assert.deepEqual(fixes.map((f) => f.stage).sort(), ['block-end', 'block-end', 'tool-call-delta', 'tool-call-delta']);
});

test('the reproduced failure: an empty id must never reach the persisted block', async () => {
	// exactly the shape seen in a bricked session
	const chunks = [
		{ type: 'block-start', index: 0, blockType: 'reasoning' },
		{ type: 'reasoning-delta', index: 0, text: 'thinking' },
		blockEnd(0, { type: 'reasoning', text: 'thinking' }),
		{ type: 'block-start', index: 1, blockType: 'tool-call' },
		delta(1, '', 'bash', '{"command":"ls 02_正文"}'),
		blockEnd(1, { type: 'tool-call', id: '', name: 'bash', arguments: '{"command":"ls 02_正文"}' }),
		{ type: 'finish', reason: { kind: 'stop' } },
	];
	const out = await collect(normalizeStream(streamOf(chunks), { seed: 'seed-c' }));
	const [block] = assembleToolCalls(out);
	assert.ok(typeof block.id === 'string' && block.id.length > 0, 'persisted block id must be non-empty');
	assert.match(block.id, ID_SHAPE);
	// the upstream request will carry this id for both the assistant call and the tool result
	assert.equal(out.find((c) => c.type === 'tool-call-delta').id, block.id);
});

test('a missing (undefined) block id is filled the same way', async () => {
	const chunks = [
		{ type: 'block-start', index: 3, blockType: 'tool-call' },
		delta(3, undefined),
		blockEnd(3, { type: 'tool-call', name: 'bash', arguments: '{}' }),
	];
	const out = await collect(normalizeStream(streamOf(chunks), { seed: 'seed-d' }));
	const [block] = assembleToolCalls(out);
	assert.match(block.id, ID_SHAPE);
	assert.equal(out[1].id, block.id);
});

test('a real id arriving only in the closing block wins over synthesis', async () => {
	const chunks = [
		{ type: 'block-start', index: 0, blockType: 'tool-call' },
		delta(0, ''),
		blockEnd(0, { type: 'tool-call', id: 'call_aaaaaaaaaaaaaaaaaaaaaaaa', name: 'bash', arguments: '{}' }),
	];
	const out = await collect(normalizeStream(streamOf(chunks), { seed: 'seed-e' }));
	const [block] = assembleToolCalls(out);
	assert.equal(block.id, 'call_aaaaaaaaaaaaaaaaaaaaaaaa');
});

test('a real id arriving only in the delta is carried into the closing block', async () => {
	const chunks = [
		{ type: 'block-start', index: 0, blockType: 'tool-call' },
		delta(0, 'call_bbbbbbbbbbbbbbbbbbbbbbbb'),
		blockEnd(0, { type: 'tool-call', id: '', name: 'bash', arguments: '{}' }),
	];
	const out = await collect(normalizeStream(streamOf(chunks), { seed: 'seed-f' }));
	const [block] = assembleToolCalls(out);
	assert.equal(block.id, 'call_bbbbbbbbbbbbbbbbbbbbbbbb');
});

test('non tool-call chunks pass through untouched', async () => {
	const chunks = [
		{ type: 'block-start', index: 0, blockType: 'text' },
		{ type: 'text-delta', index: 0, text: 'hi' },
		blockEnd(0, { type: 'text', text: 'hi' }),
		{ type: 'usage', usage: { inputTokens: 3, outputTokens: 4, totalTokens: 7 } },
		{ type: 'finish', reason: { kind: 'stop' } },
	];
	const out = await collect(normalizeStream(streamOf(chunks), { seed: 'seed-g' }));
	assert.deepEqual(out.map((c, i) => c === chunks[i]), chunks.map(() => true));
});

test('two streams never synthesize the same id for the same block index', async () => {
	const make = () =>
		streamOf([
			{ type: 'block-start', index: 0, blockType: 'tool-call' },
			delta(0, ''),
			blockEnd(0, { type: 'tool-call', id: '', name: 'bash', arguments: '{}' }),
		]);
	const [a] = assembleToolCalls(await collect(normalizeStream(make(), { seed: 'seed-h' })));
	const [b] = assembleToolCalls(await collect(normalizeStream(make(), { seed: 'seed-i' })));
	assert.notEqual(a.id, b.id);
	assert.match(synthesizeToolCallId('seed-h', 0), ID_SHAPE);
});
