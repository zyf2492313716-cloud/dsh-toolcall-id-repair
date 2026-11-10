/**
 * @dsh-local/dsh-toolcall-id-repair
 *
 * DSH 工具调用空 ID 自动修复守卫：
 * 某些兼容 OpenAI 协议的模型上游/中转网关在流式返回 tool_calls 时未携带 id（或 id 为空字符串 ""）。
 * DSH 会话格式 v4 严格校验 tool/result 的 toolCallId 匹配性，空 ID 会触发
 * `format v4 tool/result at seq N requires toolCallId matching its tool source`
 * 导致整轮会话死锁中断（"本轮运行失败"）。
 *
 * 本插件在 Cordis 的 `llm/stream` 级联钩子上拦截流式数据块，对缺失 ID 的 tool-call-delta 与
 * block-end 自动合成确定性的 `call_<sha256(seed:index)[0:24]>` ID，保持消息块、事件与持久化会话的一致性。
 *
 * 权限边界：纯内存流转换，零文件读写、零网络请求、零命令执行、不读取凭据。
 */
import { createHash, randomUUID } from 'node:crypto';

/** Cordis 插件标识 */
export const name = 'dsh-toolcall-id-repair';

/**
 * 合成标准 OpenAI 形状的 tool-call id：`call_` + 24 位十六进制字符。
 *
 * @param {string} seed - 每个 stream 的独立随机种子
 * @param {number} index - 消息块索引
 * @returns {string} 确定且不碰撞的合法 tool call id
 */
export function synthesizeToolCallId(seed, index) {
	return `call_${createHash('sha256').update(`${seed}:${index}`).digest('hex').slice(0, 24)}`;
}

/** @param {unknown} value @returns {boolean} 是否是非空合法 id */
const isId = (value) => typeof value === 'string' && value.length > 0;

/**
 * 包装流式数据块生成器，自动填补缺失的 tool-call id。
 *
 * @param {AsyncIterable<any>} source - 上游数据块流
 * @param {object} [options]
 * @param {string} [options.seed] - 用于合成 id 的种子
 * @param {(info: { stage: string, index: number, name?: string, id: string }) => void} [options.onFix] - 修复回调
 */
export async function* normalizeStream(source, options = {}) {
	const seed = options.seed ?? randomUUID();
	const onFix = typeof options.onFix === 'function' ? options.onFix : undefined;
	/** 每个 block index 对应固定的合成或已知 id */
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
			// 如果模型本身给出了有效 ID，优先保留并记住
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
			if (synthesized) {
				onFix?.({ stage: 'tool-call-delta', index: chunk.index, name: chunk.name, id });
			}
			yield { ...chunk, id };
			continue;
		}

		if (chunk.type === 'block-end' && chunk.block?.type === 'tool-call') {
			const block = chunk.block;
			// 闭合块是 DSH 最终落盘持久化的内容：若没有有效 ID 则填补
			const id = isId(block.id) ? block.id : (ids.get(chunk.index) ?? synthesize(chunk.index));
			ids.set(chunk.index, id);
			if (!isId(block.id)) {
				onFix?.({ stage: 'block-end', index: chunk.index, name: block.name, id });
			}
			yield block.id === id ? chunk : { ...chunk, block: { ...block, id } };
			continue;
		}

		yield chunk;
	}
}

/**
 * 注册到 Cordis 宿主，在所有 llm/stream 上安装自动修复守卫。
 *
 * @param {import('cordis').Context} ctx
 */
export function apply(ctx) {
	ctx.on(
		'llm/stream',
		(_options, next) => {
			const seed = randomUUID();
			return normalizeStream(next(), {
				seed,
				onFix: (info) => {
					try {
						ctx?.logger?.info?.(
							`[${name}] filled missing tool-call id ${info.id} (block ${info.index}${info.name ? `, ${info.name}` : ''})`
						);
					} catch {}
				},
			});
		},
		{ global: true },
	);

	try {
		ctx?.logger?.info?.(`[${name}] mounted: filling empty tool-call ids on llm/stream`);
	} catch {}
}
