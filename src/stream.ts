import type {
	Api,
	ApiKey,
	ApiKeyResolver,
	AssistantMessage,
	AssistantMessageEventStream,
	AuthStorage,
	Context,
	Model,
	SimpleStreamOptions,
	TextContent,
	ToolCall,
	Usage,
} from "@oh-my-pi/pi-ai";
import { AUTH_RETRY_STEPS, isApiKeyResolver, resolveApiKeyOnce } from "@oh-my-pi/pi-ai";
import { createAssistantMessageEventStream } from "@oh-my-pi/pi-ai/utils/event-stream";

import {
	API_ID,
	buildHeaders,
	classifyFailure,
	DEFAULT_MAX_TOKENS,
	PROVIDER_ID,
	resetAtMs,
	resolveBaseUrl,
} from "./api";

const MAX_KEY_ATTEMPTS = 4;
const MAX_RATE_LIMIT_ATTEMPTS = 3;
const NO_KEY_MESSAGE = "No Command Code API key. Run /login and pick Command Code.";
const MISSING_FINISH_MESSAGE = "Command Code stream ended without a finish event";

type WireContentPart =
	| { type: "text"; text: string }
	| { type: "tool-call"; toolCallId: string; toolName: string; input: unknown }
	| {
			type: "tool-result";
			toolCallId: string;
			toolName: string;
			output: { type: "text"; value: string };
			isError: boolean;
	  };

type WireMessage = { role: string; content: WireContentPart[] };

type WireParams = {
	model: string;
	messages: WireMessage[];
	tools?: { name: string; description: string; input_schema: unknown }[];
	system?: string;
	max_tokens: number;
	stream: true;
	temperature?: number;
	reasoning_effort?: "low" | "medium" | "high";
};

/** Canonical boundary guard for gateway JSON; every field read re-checks with typeof. */
function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null;
}

function readErrorMessage(body: unknown): string | undefined {
	if (!isRecord(body)) return undefined;
	const nested = body.error;
	if (isRecord(nested) && typeof nested.message === "string") return nested.message;
	if (typeof body.message === "string") return body.message;
	return undefined;
}

function readErrorStatusCode(body: unknown): number | undefined {
	if (!isRecord(body)) return undefined;
	const nested = body.error;
	if (isRecord(nested) && typeof nested.statusCode === "number") return nested.statusCode;
	return undefined;
}

function textOf(content: string | (TextContent | { type: string })[]): string {
	if (typeof content === "string") return content;
	return content
		.filter((part): part is TextContent => part.type === "text")
		.map((part) => part.text)
		.join("\n");
}

function buildMessages(context: Context): WireMessage[] {
	const messages: WireMessage[] = [];
	for (const message of context.messages) {
		if (message.role === "user" || message.role === "developer") {
			// The gateway has no developer role; fold it into a user turn.
			messages.push({
				role: "user",
				content: [{ type: "text", text: textOf(message.content) }],
			});
		} else if (message.role === "assistant") {
			const parts: WireContentPart[] = [];
			for (const part of message.content) {
				if (part.type === "text") {
					parts.push({ type: "text", text: part.text });
				} else if (part.type === "toolCall") {
					parts.push({
						type: "tool-call",
						toolCallId: part.id,
						toolName: part.name,
						input: part.arguments,
					});
				}
				// thinking / redactedThinking have no replay slot on the gateway; drop them.
			}
			if (parts.length > 0) messages.push({ role: "assistant", content: parts });
		} else if (message.role === "toolResult") {
			messages.push({
				role: "tool",
				content: [
					{
						type: "tool-result",
						toolCallId: message.toolCallId,
						toolName: message.toolName,
						output: {
							type: "text",
							value: message.content
								.filter((part): part is TextContent => part.type === "text")
								.map((part) => part.text)
								.join("\n"),
						},
						isError: message.isError,
					},
				],
			});
		}
	}
	return messages;
}

function buildBody(
	model: Model<Api>,
	context: Context,
	options: SimpleStreamOptions | undefined,
	threadId: string,
) {
	const params: WireParams = {
		model: model.id,
		messages: buildMessages(context),
		max_tokens: Math.min(
			options?.maxTokens ?? DEFAULT_MAX_TOKENS,
			model.maxTokens ?? DEFAULT_MAX_TOKENS,
		),
		stream: true,
	};
	const system = context.systemPrompt?.join("\n\n");
	if (system) params.system = system;
	if (context.tools && context.tools.length > 0) {
		params.tools = context.tools.map((tool) => ({
			name: tool.name,
			description: tool.description,
			input_schema: tool.parameters,
		}));
	}
	if (options?.temperature !== undefined) params.temperature = options.temperature;
	const effort = options?.reasoning;
	if (model.reasoning && (effort === "low" || effort === "medium" || effort === "high")) {
		params.reasoning_effort = effort;
	}
	// Wrapper shape verified live against POST /alpha/generate: the gateway rejects
	// null wrapper fields — memory/taste/skills are strings and config is an object.
	return {
		config: {
			workingDir: process.cwd(),
			date: new Date().toISOString().slice(0, 10),
			environment:
				(process.env.COMMANDCODE_API_ENV ?? "prod") === "prod"
					? "production"
					: (process.env.COMMANDCODE_API_ENV as string),
			structure: [],
			isGitRepo: false,
			currentBranch: "",
			mainBranch: "",
			gitStatus: "",
			recentCommits: [],
		},
		memory: "",
		taste: "",
		skills: "",
		permissionMode: "auto-accept",
		threadId,
		mode: "agent",
		params,
	};
}

function seedPartial(model: Model<Api>): AssistantMessage {
	return {
		role: "assistant",
		content: [],
		api: API_ID,
		provider: PROVIDER_ID,
		model: model.id,
		usage: {
			input: 0,
			output: 0,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 0,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
		stopReason: "stop",
		timestamp: Date.now(),
	};
}

/** Vendor's callModelWithRetry backoff curve, clamped to [1s, 10s]. */
function backoffMs(attempt: number): number {
	return Math.min(10_000, Math.max(1_000, 100 * 2 ** attempt));
}

/**
 * Collapse every bearer source into one resolver. omp's agent loop passes an
 * ApiKeyResolver in `options.apiKey`; a pinned literal has nothing to rotate
 * to, so it resolves to itself; a direct caller (test, embedder) gets
 * AuthStorage's own a/b/c resolver. Undefined means no credential at all.
 */
function keyResolver(
	apiKey: ApiKey | undefined,
	auth: AuthStorage | undefined,
	modelId: string,
	sessionId: string | undefined,
): ApiKeyResolver | undefined {
	if (isApiKeyResolver(apiKey)) return apiKey;
	if (typeof apiKey === "string" && apiKey !== "") return () => apiKey;
	return auth?.resolver(PROVIDER_ID, { sessionId, modelId });
}

/** Sleep that resolves `false` early when the abort signal fires. */
function abortableSleep(ms: number, signal: AbortSignal | undefined): Promise<boolean> {
	const { promise, resolve } = Promise.withResolvers<boolean>();
	if (signal?.aborted) {
		resolve(false);
		return promise;
	}
	const timer = setTimeout(() => {
		signal?.removeEventListener("abort", onAbort);
		resolve(true);
	}, ms);
	const onAbort = () => {
		clearTimeout(timer);
		resolve(false);
	};
	signal?.addEventListener("abort", onAbort, { once: true });
	return promise;
}

function isAbortError(err: unknown): boolean {
	return err instanceof Error && err.name === "AbortError";
}

function readWireUsage(finishEvent: Record<string, unknown>): Usage | undefined {
	const totalUsage = finishEvent.totalUsage;
	if (!isRecord(totalUsage)) return undefined;
	const output = typeof totalUsage.outputTokens === "number" ? totalUsage.outputTokens : 0;
	let cacheRead = 0;
	let cacheWrite = 0;
	const details = totalUsage.inputTokenDetails;
	if (isRecord(details)) {
		if (typeof details.cacheReadTokens === "number") cacheRead = details.cacheReadTokens;
		if (typeof details.cacheWriteTokens === "number") cacheWrite = details.cacheWriteTokens;
	}
	// The gateway's inputTokens is inclusive: on consecutive turns
	// cacheRead(N) ≈ inputTokens(N-1), i.e. the cache-served prefix is counted
	// inside inputTokens (verified against live sessions). omp's Usage contract
	// is disjoint — input = uncached, cacheRead = served from cache — so back
	// the cached portion out or the TUI cache-hit denominator and /stats
	// "Uncached Input" both overcount.
	const input = Math.max(
		0,
		(typeof totalUsage.inputTokens === "number" ? totalUsage.inputTokens : 0) -
			cacheRead -
			cacheWrite,
	);
	// Command Code bills its own credits, not per-token USD — cost is honestly zero.
	return {
		input,
		output,
		cacheRead,
		cacheWrite,
		totalTokens: input + output + cacheRead + cacheWrite,
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
	};
}

type StreamOutcome =
	| "done"
	| "aborted"
	| "content-failed"
	| { status: number | undefined; body: unknown };

export function createCommandCodeStream(deps: {
	getAuthStorage(): AuthStorage | undefined;
	getSessionId(): string | undefined;
	getProjectSlug(): string;
	fetchImpl?: typeof fetch;
}): (
	model: Model<Api>,
	context: Context,
	options?: SimpleStreamOptions,
) => AssistantMessageEventStream {
	return (model, context, options) => {
		const stream = createAssistantMessageEventStream();
		void run(model, context, options, stream).catch((err: unknown) => {
			// Last-resort guard: run() converts every known failure into a
			// terminal event, so reaching here means an unexpected throw.
			if (stream.done) return;
			const partial = seedPartial(model);
			const aborted = isAbortError(err);
			partial.stopReason = aborted ? "aborted" : "error";
			partial.errorMessage = err instanceof Error ? err.message : String(err);
			stream.push({ type: "error", reason: aborted ? "aborted" : "error", error: partial });
		});
		return stream;
	};

	async function run(
		model: Model<Api>,
		context: Context,
		options: SimpleStreamOptions | undefined,
		stream: AssistantMessageEventStream,
	): Promise<void> {
		const fetchImpl = deps.fetchImpl ?? globalThis.fetch;
		const signal = options?.signal;
		const sessionId = deps.getSessionId();
		const threadId = sessionId ?? crypto.randomUUID();
		const partial = seedPartial(model);
		stream.push({ type: "start", partial });

		const fail = (message: string, status?: number): void => {
			partial.stopReason = "error";
			partial.errorMessage = message;
			if (status !== undefined) partial.errorStatus = status;
			stream.push({ type: "error", reason: "error", error: partial });
		};

		const failAborted = (): void => {
			partial.stopReason = "aborted";
			partial.errorMessage = "Command Code request aborted";
			stream.push({ type: "error", reason: "aborted", error: partial });
		};

		const authStorage = deps.getAuthStorage();
		const resolver = keyResolver(options?.apiKey, authStorage, model.id, sessionId);
		let key = resolver === undefined ? undefined : await resolveApiKeyOnce(resolver, signal);
		if (key === undefined || resolver === undefined) {
			fail(NO_KEY_MESSAGE);
			return;
		}

		let rateLimitAttempt = 0;
		let quotaSwitches = 0;
		let authStep = 0;
		let otherRetried = false;

		/** Walk the remaining native a/b/c steps until the resolver yields a different bearer. */
		const nextAuthKey = async (
			error: unknown,
			previousKey: string,
		): Promise<string | undefined> => {
			while (authStep < AUTH_RETRY_STEPS.length) {
				const lastChance = AUTH_RETRY_STEPS[authStep] ?? true;
				authStep += 1;
				const next = await resolver({ lastChance, error, previousKey, signal });
				if (next !== undefined && next !== previousKey) return next;
			}
			return undefined;
		};

		for (;;) {
			if (signal?.aborted) {
				failAborted();
				return;
			}

			let response: Response;
			try {
				response = await fetchImpl(`${resolveBaseUrl()}/alpha/generate`, {
					method: "POST",
					headers: buildHeaders(key, {
						sessionId: sessionId ?? threadId,
						projectSlug: deps.getProjectSlug(),
					}),
					body: JSON.stringify(buildBody(model, context, options, threadId)),
					signal,
				});
			} catch (err) {
				if (isAbortError(err) || signal?.aborted) {
					failAborted();
					return;
				}
				fail(err instanceof Error ? err.message : String(err));
				return;
			}

			let status: number | undefined;
			let body: unknown;
			if (!response.ok) {
				status = response.status;
				const text = await response.text().catch(() => "");
				try {
					body = JSON.parse(text);
				} catch {
					body = {};
				}
			} else {
				const outcome = await consumeStream(response, partial, stream);
				if (outcome === "done") return;
				if (outcome === "aborted") {
					failAborted();
					return;
				}
				if (outcome === "content-failed") {
					// A content event is already on the wire; replaying would
					// duplicate it. The terminal error was pushed inside consumeStream.
					return;
				}
				status = outcome.status;
				body = outcome.body;
			}

			// Failure handling (plan step 9). Nothing beyond `start` has been
			// emitted on every path that reaches here, so retrying is safe.
			const verdict = classifyFailure(status, body);

			if (verdict === "quota" && authStorage) {
				const resetMs = resetAtMs(body);
				const mark = await authStorage.markUsageLimitReached(PROVIDER_ID, sessionId, {
					retryAfterMs: resetMs !== undefined ? resetMs - Date.now() : undefined,
					modelId: model.id,
					apiKey: key,
					signal,
				});
				quotaSwitches += 1;
				if (mark.switched && quotaSwitches < MAX_KEY_ATTEMPTS) {
					// markUsageLimitReached already advanced the session-sticky pointer,
					// so an initial resolve returns the sibling. Asking the resolver to
					// rotate (lastChance) here would rotate twice and skip a key.
					const next = await resolveApiKeyOnce(resolver, signal);
					if (next !== undefined && next !== key) {
						key = next;
						continue;
					}
				}
				let poolSize = 1;
				try {
					poolSize = Math.max(1, authStorage.listStoredCredentials(PROVIDER_ID).length);
				} catch {
					poolSize = 1;
				}
				const resetIso =
					mark.retryAtMs !== undefined ? new Date(mark.retryAtMs).toISOString() : "unknown";
				fail(
					`Command Code quota exhausted on all ${poolSize} key(s). Earliest reset: ${resetIso}. Run /login to add another key.`,
				);
				return;
			}

			if (verdict === "auth") {
				const next = await nextAuthKey(body, key);
				if (next !== undefined) {
					key = next;
					continue;
				}
				fail(
					`Command Code rejected the API key ending in …${key.slice(-4)}. Run /login to replace it.`,
					status,
				);
				return;
			}

			if (verdict === "rate-limit") {
				// Transient server throttling: back off, never rotate the key.
				if (rateLimitAttempt < MAX_RATE_LIMIT_ATTEMPTS) {
					rateLimitAttempt += 1;
					if (!(await abortableSleep(backoffMs(rateLimitAttempt), signal))) {
						failAborted();
						return;
					}
					continue;
				}
				fail(readErrorMessage(body) ?? "Command Code rate limit persisted across retries", status);
				return;
			}

			// verdict === "other"
			const retryable = status !== undefined && (status === 429 || status >= 500);
			if (retryable && !otherRetried) {
				otherRetried = true;
				if (!(await abortableSleep(backoffMs(1), signal))) {
					failAborted();
					return;
				}
				continue;
			}
			fail(
				readErrorMessage(body) ??
					`Command Code request failed${status !== undefined ? ` (HTTP ${status})` : ""}`,
				status,
			);
			return;
		}
	}

	async function consumeStream(
		response: Response,
		partial: AssistantMessage,
		stream: AssistantMessageEventStream,
	): Promise<StreamOutcome> {
		if (!response.body) return { status: response.status, body: {} };
		const reader = response.body.getReader();
		const decoder = new TextDecoder();
		let buffer = "";
		let contentIndex = 0;
		let openBlock: "text" | "thinking" | undefined;
		let sawToolCall = false;
		const startedAt = performance.now();
		let firstTokenAt: number | undefined;

		/** Stamp the timing fields omp reads for the TTFT/TPS usage row. */
		const settleTiming = (): void => {
			partial.duration = performance.now() - startedAt;
			if (firstTokenAt !== undefined) partial.ttft = firstTokenAt - startedAt;
		};

		/** First observable output (text, reasoning, or a tool call) marks TTFT. */
		const markFirstToken = (): void => {
			if (firstTokenAt === undefined) firstTokenAt = performance.now();
		};

		const fail = (message: string, status?: number): void => {
			partial.stopReason = "error";
			partial.errorMessage = message;
			if (status !== undefined) partial.errorStatus = status;
			stream.push({ type: "error", reason: "error", error: partial });
		};

		const closeOpenBlock = (): void => {
			if (openBlock === "text") {
				const part = partial.content[contentIndex];
				stream.push({
					type: "text_end",
					contentIndex,
					content: part?.type === "text" ? part.text : "",
					partial,
				});
				contentIndex += 1;
			} else if (openBlock === "thinking") {
				const part = partial.content[contentIndex];
				stream.push({
					type: "thinking_end",
					contentIndex,
					content: part?.type === "thinking" ? part.thinking : "",
					partial,
				});
				contentIndex += 1;
			}
			// toolcall blocks close on their own toolcall_end event.
			openBlock = undefined;
		};

		for (;;) {
			const result = await reader.read().catch((err: unknown) => {
				if (isAbortError(err)) return null;
				throw err;
			});
			if (result === null) return "aborted";
			if (result.done) break;
			buffer += decoder.decode(result.value, { stream: true });
			let newline = buffer.indexOf("\n");
			while (newline !== -1) {
				const line = buffer.slice(0, newline).trim();
				buffer = buffer.slice(newline + 1);
				newline = buffer.indexOf("\n");
				if (line.length === 0) continue;

				let event: unknown;
				try {
					event = JSON.parse(line);
				} catch {
					// Guards against a trailing partial chunk; not fatal.
					continue;
				}
				if (!isRecord(event) || typeof event.type !== "string") continue;

				switch (event.type) {
					case "text-delta": {
						if (typeof event.text !== "string") break;
						markFirstToken();
						if (openBlock !== "text") {
							closeOpenBlock();
							partial.content.push({ type: "text", text: "" });
							stream.push({ type: "text_start", contentIndex, partial });
							openBlock = "text";
						}
						const part = partial.content[contentIndex];
						if (part?.type === "text") part.text += event.text;
						stream.push({ type: "text_delta", contentIndex, delta: event.text, partial });
						break;
					}
					case "reasoning-start": {
						markFirstToken();
						if (openBlock !== "thinking") {
							closeOpenBlock();
							partial.content.push({ type: "thinking", thinking: "" });
							stream.push({ type: "thinking_start", contentIndex, partial });
							openBlock = "thinking";
						}
						break;
					}
					case "reasoning-delta": {
						if (typeof event.text !== "string") break;
						markFirstToken();
						if (openBlock !== "thinking") {
							closeOpenBlock();
							partial.content.push({ type: "thinking", thinking: "" });
							stream.push({ type: "thinking_start", contentIndex, partial });
							openBlock = "thinking";
						}
						const part = partial.content[contentIndex];
						if (part?.type === "thinking") part.thinking += event.text;
						stream.push({ type: "thinking_delta", contentIndex, delta: event.text, partial });
						break;
					}
					case "reasoning-end": {
						if (openBlock === "thinking") closeOpenBlock();
						break;
					}
					case "tool-call": {
						if (
							typeof event.toolCallId !== "string" ||
							typeof event.toolName !== "string" ||
							event.input === undefined
						) {
							break;
						}
						closeOpenBlock();
						markFirstToken();
						sawToolCall = true;
						const input: Record<string, unknown> = isRecord(event.input) ? event.input : {};
						const toolCall: ToolCall = {
							type: "toolCall",
							id: event.toolCallId,
							name: event.toolName,
							arguments: input,
						};
						stream.push({ type: "toolcall_start", contentIndex, partial });
						stream.push({
							type: "toolcall_delta",
							contentIndex,
							delta: JSON.stringify(input),
							partial,
						});
						partial.content.push(toolCall);
						stream.push({ type: "toolcall_end", contentIndex, toolCall, partial });
						contentIndex += 1;
						break;
					}
					case "tool-result": {
						// Provider-executed tool results are ignored; omp executes tools itself.
						break;
					}
					case "finish": {
						closeOpenBlock();
						settleTiming();
						const usage = readWireUsage(event);
						if (usage) partial.usage = usage;
						const finishReason = typeof event.finishReason === "string" ? event.finishReason : "";
						if (sawToolCall || finishReason === "tool_calls") {
							partial.stopReason = "toolUse";
						} else if (finishReason === "max_tokens" || finishReason === "length") {
							partial.stopReason = "length";
						} else {
							partial.stopReason = "stop";
						}
						stream.push({ type: "done", reason: partial.stopReason, message: partial });
						return "done";
					}
					case "error": {
						// Mid-stream failure: error.statusCode is the status, the whole
						// line is the classification body.
						const status = readErrorStatusCode(event);
						if (openBlock !== undefined || contentIndex > 0) {
							closeOpenBlock();
							settleTiming();
							fail(readErrorMessage(event) ?? "Command Code stream failed", status);
							return "content-failed";
						}
						return { status, body: event };
					}
					default:
						break;
				}
			}
		}

		// Stream ended without a finish event.
		if (openBlock !== undefined || contentIndex > 0) closeOpenBlock();
		settleTiming();
		fail(MISSING_FINISH_MESSAGE);
		return "content-failed";
	}
}
