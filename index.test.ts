import { afterEach, describe, expect, test } from "bun:test";
import type {
	Api,
	ApiKeyResolveContext,
	ApiKeyResolver,
	AssistantMessage,
	AssistantMessageEvent,
	AuthStorage,
	Context,
	Model,
	StoredAuthCredential,
	UsageLimitMarkResult,
} from "@oh-my-pi/pi-ai";
import type { ExtensionAPI, ProviderConfig } from "@oh-my-pi/pi-coding-agent";

import commandCodeProvider from "./index";
import {
	buildHeaders,
	classifyFailure,
	PROVIDER_ID,
	resetAtMs,
	resolveBaseUrl,
	sanitizeApiKey,
} from "./src/api";
import {
	DEFAULT_MODEL_ID,
	DISCOVERY_TIMEOUT_MS,
	fetchCommandCodeModels,
	parseModelsList,
} from "./src/models";
import { createCommandCodeStream } from "./src/stream";

/* ------------------------------------------------------------------ *
 * Test helpers
 * ------------------------------------------------------------------ */

/** Minimal `Model<Api>` the stream touches: id, maxTokens, reasoning. */
function makeModel(
	overrides: Partial<Pick<Model<Api>, "id" | "maxTokens" | "reasoning">> = {},
): Model<Api> {
	return {
		id: overrides.id ?? "deepseek/deepseek-v4-flash",
		name: "test model",
		api: "commandcode-generate" as Api,
		provider: "commandcode",
		baseUrl: "https://api.commandcode.ai",
		reasoning: overrides.reasoning ?? false,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 1_000_000,
		maxTokens: overrides.maxTokens ?? 64_000,
		compat: undefined,
	};
}

/** A `Context` with one user text message and no tools. */
function makeContext(text = "Reply with the single word: ok"): Context {
	return {
		messages: [{ role: "user", content: [{ type: "text", text }], timestamp: Date.now() }],
	};
}

/** Build a `Response` whose body is a ReadableStream emitting the given chunks.
 *  `Response.ok` is auto-derived from `status` (200–299 → true). */
function makeResponse(chunks: Uint8Array[], status = 200): Response {
	const stream = new ReadableStream<Uint8Array>({
		start(controller) {
			for (const chunk of chunks) controller.enqueue(chunk);
			controller.close();
		},
	});
	return new Response(stream, {
		status,
		headers: { "Content-Type": "application/json" },
	});
}

/** Encode a string into a UTF-8 Uint8Array chunk. */
function enc(text: string): Uint8Array {
	return new TextEncoder().encode(text);
}

/**
 * The valid ndjson body used across stream tests. The gateway reports
 * `inputTokens` inclusive of the cache-served prefix: 5 input + 3 read from
 * cache + 2 written to cache → inputTokens 10, matching live sessions where
 * `cacheRead(N) ≈ inputTokens(N-1)`.
 */
const VALID_NDJSON =
	'{"type":"text-delta","text":"He"}\n' +
	'{"type":"text-delta","text":"llo"}\n' +
	'{"type":"finish","finishReason":"end_turn","totalUsage":{"inputTokens":10,"outputTokens":2,"inputTokenDetails":{"cacheReadTokens":3,"cacheWriteTokens":2}}}\n';

/** Split VALID_NDJSON mid-line into two chunks (the break is inside `llo`). */
function splitMidLine(): [Uint8Array, Uint8Array] {
	const full = VALID_NDJSON;
	// Break inside the second text-delta line, after `"l` — mid-line.
	const cut = full.indexOf('"llo"') + 2;
	return [enc(full.slice(0, cut)), enc(full.slice(cut))];
}

/** Collect every event from an AssistantMessageEventStream into an array. */
async function collectEvents(stream: {
	[Symbol.asyncIterator](): AsyncIterator<AssistantMessageEvent>;
}): Promise<AssistantMessageEvent[]> {
	const events: AssistantMessageEvent[] = [];
	for await (const event of stream) {
		events.push(event);
		if (event.type === "done" || event.type === "error") break;
	}
	return events;
}

/** Extract the final AssistantMessage from a settled stream via result(). */
async function finalMessage(stream: {
	result(): Promise<AssistantMessage>;
}): Promise<AssistantMessage> {
	return stream.result();
}

/** A recording fetch that routes by Authorization bearer. */
function fetchByBearer(routes: Record<string, () => Response>): {
	fetch: typeof fetch;
	calls: { auth: string; body: string }[];
} {
	const calls: { auth: string; body: string }[] = [];
	const fn: typeof fetch = Object.assign(
		async (input: URL | RequestInfo, init?: RequestInit | BunFetchRequestInit) => {
			const headers = init?.headers as Record<string, string> | undefined;
			const auth = headers?.Authorization ?? "";
			const bodyText = typeof init?.body === "string" ? init.body : "";
			calls.push({ auth, body: bodyText });
			const factory = routes[auth];
			if (!factory) throw new Error(`unexpected bearer ${auth} for ${String(input)}`);
			return factory();
		},
		{ preconnect: () => undefined },
	);
	return { fetch: fn, calls };
}

/** The subset of AuthStorage the stream exercises. Test double for a large
 *  third-party interface (~30 methods) — only these five are touched. */
interface StreamAuthStorage {
	getApiKey(provider: string, sessionId?: string): Promise<string | undefined>;
	markUsageLimitReached(
		provider: string,
		sessionId: string | undefined,
		options?: unknown,
	): Promise<UsageLimitMarkResult>;
	rotateSessionCredential(
		provider: string,
		sessionId: string | undefined,
		options?: unknown,
	): Promise<boolean>;
	listStoredCredentials(provider?: string): StoredAuthCredential[];
	resolver(provider: string, options?: unknown): ApiKeyResolver;
}

/** A stub AuthStorage whose getApiKey yields keys in order, with live call counters. */
function stubAuthStorage(opts: {
	keys: string[];
	markResult?: UsageLimitMarkResult;
	rotateResult?: boolean;
}): AuthStorage & { markCalls: number; rotateCalls: number } {
	let idx = 0;
	const counts = { mark: 0, rotate: 0 };
	const base: StreamAuthStorage = {
		getApiKey: async () => {
			// Yield keys in order; once exhausted, repeat the last. This mirrors
			// the real resolver: after a non-rotating retry it returns the same key.
			const k = opts.keys[Math.min(idx, opts.keys.length - 1)];
			idx += 1;
			return k;
		},
		markUsageLimitReached: async () => {
			counts.mark += 1;
			return opts.markResult ?? { switched: false };
		},
		rotateSessionCredential: async () => {
			counts.rotate += 1;
			return opts.rotateResult ?? false;
		},
		listStoredCredentials: () => [],
		// Mirrors createApiKeyResolver: initial/refresh resolve reads the store,
		// lastChance rotates first. Ordered getApiKey supplies the sibling.
		resolver:
			(): ApiKeyResolver =>
			async ({ lastChance, error, previousKey, signal }: ApiKeyResolveContext) => {
				if (error !== undefined && lastChance) {
					await base.rotateSessionCredential(PROVIDER_ID, undefined, {
						error,
						apiKey: previousKey,
						signal,
					});
				}
				return base.getApiKey(PROVIDER_ID, undefined);
			},
	};
	const stub = base as AuthStorage;
	Object.defineProperty(stub, "markCalls", { get: () => counts.mark });
	Object.defineProperty(stub, "rotateCalls", { get: () => counts.rotate });
	return stub as AuthStorage & { markCalls: number; rotateCalls: number };
}

/* ------------------------------------------------------------------ *
 * 1. classifyFailure / resetAtMs / sanitizeApiKey / resolveBaseUrl
 * ------------------------------------------------------------------ */

describe("api — classifyFailure", () => {
	test("400 insufficient credits → quota", () => {
		expect(
			classifyFailure(400, {
				error: { message: "You have insufficient credits to make this request." },
			}),
		).toBe("quota");
	});

	test("429 RATE_LIMITED with rateLimit → quota, resetAtMs ×1000", () => {
		const body = {
			error: { code: "RATE_LIMITED", rateLimit: { window: "weekly", reset: 1_800_000_000 } },
		};
		expect(classifyFailure(429, body)).toBe("quota");
		expect(resetAtMs(body)).toBe(1_800_000_000_000);
	});

	test("429 rate_limit_error → rate-limit (NOT quota)", () => {
		expect(
			classifyFailure(429, { error: { type: "rate_limit_error", message: "Too many requests" } }),
		).toBe("rate-limit");
	});

	test("401 empty body → auth", () => {
		expect(classifyFailure(401, {})).toBe("auth");
	});

	test("bare RATE_LIMITED without a window label → rate-limit (vendor: window must resolve)", () => {
		expect(classifyFailure(undefined, { error: { code: "RATE_LIMITED" } })).toBe("rate-limit");
	});

	test("RATE_LIMITED with a window label → quota", () => {
		expect(
			classifyFailure(undefined, {
				error: { code: "RATE_LIMITED", rateLimit: { window: "fiveHour" } },
			}),
		).toBe("quota");
		expect(
			classifyFailure(429, {
				error: { code: "RATE_LIMITED", message: "usage limit for your plan" },
			}),
		).toBe("quota");
	});

	test("500 empty → other; 403 with message → other", () => {
		expect(classifyFailure(500, {})).toBe("other");
		expect(classifyFailure(403, { error: { message: "nope" } })).toBe("other");
	});
});

describe("api — resetAtMs fallback regex", () => {
	test("falls back to /resets at (ISO)/ when rateLimit absent", () => {
		const iso = "2027-01-02T03:04:05Z";
		const body = { error: { message: `Your usage limit for your plan resets at ${iso}.` } };
		expect(resetAtMs(body)).toBe(Date.parse(iso));
	});

	test("returns undefined when neither rateLimit nor a timestamp is present", () => {
		expect(resetAtMs({ error: { message: "something broke" } })).toBeUndefined();
	});
});

describe("api — sanitizeApiKey", () => {
	test("strips bracketed-paste markers and control chars", () => {
		expect(sanitizeApiKey("\u001b[200~ user_abc \u001b[201~")).toBe("user_abc");
	});

	test("drops ASCII control characters (code ≤ 31 and 127)", () => {
		expect(sanitizeApiKey("user_\u0000\u007fxyz")).toBe("user_xyz");
	});
});

describe("api — resolveBaseUrl", () => {
	test("defaults to prod", () => {
		expect(resolveBaseUrl({})).toBe("https://api.commandcode.ai");
	});

	test("staging env", () => {
		expect(resolveBaseUrl({ COMMANDCODE_API_ENV: "staging" })).toBe(
			"https://staging-api.commandcode.ai",
		);
	});

	test("local env", () => {
		expect(resolveBaseUrl({ COMMANDCODE_API_ENV: "local" })).toBe("http://localhost:9090");
	});

	test("sandbox + COMMANDCODE_API_URL wins", () => {
		expect(resolveBaseUrl({ COMMANDCODE_SANDBOX: "true", COMMANDCODE_API_URL: "http://x" })).toBe(
			"http://x",
		);
	});

	test("COMMANDCODE_API_URL without sandbox is ignored (falls back to prod)", () => {
		expect(resolveBaseUrl({ COMMANDCODE_API_URL: "http://x" })).toBe("https://api.commandcode.ai");
	});

	test("unknown env value defaults to prod", () => {
		expect(resolveBaseUrl({ COMMANDCODE_API_ENV: "garbage" })).toBe("https://api.commandcode.ai");
	});
});

/* ------------------------------------------------------------------ *
 * 2. buildHeaders
 * ------------------------------------------------------------------ */

describe("api — buildHeaders", () => {
	test("contains the full CLI header set", () => {
		const h = buildHeaders("user_test", { sessionId: "sess-1", projectSlug: "0123456789" });
		expect(h.Authorization).toBe("Bearer user_test");
		expect(h["User-Agent"]).toBe("cli");
		expect(h["x-command-code-version"]).toBe("1.14.0");
		expect(h["x-cli-environment"]).toBe("production");
		expect(h["x-session-id"]).toBe("sess-1");
		expect(h["x-project-slug"]).toBe("0123456789");
		expect(h["x-taste-learning"]).toBe("false");
		expect(h["x-co-flag"]).toBe("false");
		expect(h["Content-Type"]).toBe("application/json");
	});
});

/* ------------------------------------------------------------------ *
 * 3. models discovery
 * ------------------------------------------------------------------ */

const SAMPLE_ROW = {
	id: "claude-sonnet-5",
	object: "model",
	created: 1,
	owned_by: "command-code",
	name: "Claude Sonnet 5",
	context_length: 1_000_000,
};

describe("models discovery", () => {
	const realFetch = globalThis.fetch;
	afterEach(() => {
		globalThis.fetch = realFetch;
	});

	test("maps id, name, and context_length; zeros cost; leaves reasoning and input unset", () => {
		const [model] = parseModelsList({ object: "list", data: [SAMPLE_ROW] });
		expect(model).toEqual({
			id: "claude-sonnet-5",
			name: "Claude Sonnet 5",
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
			contextWindow: 1_000_000,
			maxTokens: 64_000,
		});
		expect(model).not.toHaveProperty("reasoning");
		expect(model).not.toHaveProperty("input");
	});

	test("skips malformed rows and later duplicates", () => {
		const models = parseModelsList({
			object: "list",
			data: [
				SAMPLE_ROW,
				{ ...SAMPLE_ROW, name: "duplicate" },
				{ id: "  ", name: "blank", context_length: 1000 },
				{ id: "bad-window", name: "bad", context_length: 0 },
				{ id: "deepseek/deepseek-v4-flash", name: "Flash", context_length: 1_000_000 },
				null,
				"skip",
			],
		});
		expect(models.map((m) => m.id)).toEqual(["claude-sonnet-5", "deepseek/deepseek-v4-flash"]);
	});

	test("falls back to id when name is missing", () => {
		const [model] = parseModelsList({
			object: "list",
			data: [{ id: "xai/grok-4.6", context_length: 500_000 }],
		});
		expect(model?.name).toBe("xai/grok-4.6");
	});

	test("throws when the envelope is not a data array", () => {
		expect(() => parseModelsList({ models: [SAMPLE_ROW] })).toThrow("expected { data: Model[] }");
		expect(() => parseModelsList(null)).toThrow("expected { data: Model[] }");
	});

	test("throws when every row is invalid", () => {
		expect(() => parseModelsList({ object: "list", data: [{ id: "x" }] })).toThrow("empty catalog");
	});

	test("GET /provider/v1/models without Authorization", async () => {
		const seen: { url: string; authorization: string | null }[] = [];
		globalThis.fetch = Object.assign(
			async (input: URL | RequestInfo, init?: RequestInit | BunFetchRequestInit) => {
				const headers = new Headers(init?.headers);
				seen.push({
					url: String(input),
					authorization: headers.get("Authorization"),
				});
				return Response.json({ object: "list", data: [SAMPLE_ROW] });
			},
			{ preconnect: () => undefined },
		);
		const models = await fetchCommandCodeModels(
			"user_should_not_be_sent",
			"https://api.commandcode.ai/",
		);
		expect(seen).toEqual([
			{ url: "https://api.commandcode.ai/provider/v1/models", authorization: null },
		]);
		expect(models).toHaveLength(1);
		expect(models[0]?.id).toBe("claude-sonnet-5");
	});

	test("throws on a non-OK response", async () => {
		globalThis.fetch = Object.assign(async () => new Response("nope", { status: 503 }), {
			preconnect: () => undefined,
		});
		await expect(fetchCommandCodeModels(undefined, "https://api.commandcode.ai")).rejects.toThrow(
			"HTTP 503",
		);
	});

	test("a hung fetch is aborted; timeout maps to a clear error", async () => {
		let signal: AbortSignal | undefined;
		globalThis.fetch = Object.assign(
			async (_input: URL | RequestInfo, init?: RequestInit | BunFetchRequestInit) => {
				signal = init?.signal ?? undefined;
				// Never resolves on its own: abort only comes from our signal.
				return new Promise<Response>((_resolve, reject) => {
					init?.signal?.addEventListener("abort", () =>
						reject(init?.signal?.reason ?? new Error("aborted")),
					);
				});
			},
			{ preconnect: () => undefined },
		);
		await expect(
			fetchCommandCodeModels(undefined, "https://api.commandcode.ai", 20),
		).rejects.toThrow("Command Code models: timed out after 20ms");
		expect(signal).toBeInstanceOf(AbortSignal);
		expect(signal?.aborted).toBe(true);
	});

	test("DISCOVERY_TIMEOUT_MS is 10s — under omp's 15s wrapper so we fail first", () => {
		expect(DISCOVERY_TIMEOUT_MS).toBe(10_000);
	});

	test("DEFAULT_MODEL_ID is the vendor default", () => {
		expect(DEFAULT_MODEL_ID).toBe("deepseek/deepseek-v4-flash");
	});
});

/* ------------------------------------------------------------------ *
 * 4. stream ndjson decoding
 * ------------------------------------------------------------------ */

describe("stream — ndjson decoding across a mid-line split", () => {
	test("emits start, text_start, text_delta×2, text_end, done with Hello + usage", async () => {
		const [chunkA, chunkB] = splitMidLine();
		const { fetch: fetchImpl } = fetchByBearer({
			"Bearer user_test": () => makeResponse([chunkA, chunkB]),
		});
		const auth = stubAuthStorage({ keys: ["user_test"] });

		const streamFn = createCommandCodeStream({
			getAuthStorage: () => auth,
			getSessionId: () => "sess-1",
			getProjectSlug: () => "0123456789",
			fetchImpl,
		});

		const stream = streamFn(makeModel(), makeContext());
		const events = await collectEvents(stream);
		const types = events.map((e) => e.type);

		expect(types).toEqual(["start", "text_start", "text_delta", "text_delta", "text_end", "done"]);

		const msg = await finalMessage(stream);
		const textPart = msg.content.find((c) => c.type === "text");
		expect(textPart?.type === "text" ? textPart.text : "").toBe("Hello");
		// inputTokens is inclusive of the cache-served prefix; omp's Usage
		// contract is disjoint, so input backs out the cached buckets.
		expect(msg.usage.input).toBe(5);
		expect(msg.usage.output).toBe(2);
		expect(msg.usage.cacheRead).toBe(3);
		expect(msg.usage.cacheWrite).toBe(2);
		expect(msg.usage.totalTokens).toBe(12);
		// Timing fields omp reads for the TTFT/TPS usage row must be populated.
		expect(typeof msg.duration).toBe("number");
		expect(msg.duration).toBeGreaterThan(0);
		expect(typeof msg.ttft).toBe("number");
		expect(msg.ttft).toBeGreaterThan(0);
		if (msg.duration !== undefined) expect(msg.ttft).toBeLessThanOrEqual(msg.duration);
	});

	test("mid-stream error after content still stamps duration/ttft on the terminal message", async () => {
		const body =
			'{"type":"text-delta","text":"partial"}\n' +
			'{"type":"error","error":{"message":"stream blew up","statusCode":500}}\n';
		const { fetch: fetchImpl } = fetchByBearer({
			"Bearer user_test": () => makeResponse([enc(body)]),
		});
		const auth = stubAuthStorage({ keys: ["user_test"] });

		const streamFn = createCommandCodeStream({
			getAuthStorage: () => auth,
			getSessionId: () => "sess-1",
			getProjectSlug: () => "0123456789",
			fetchImpl,
		});

		const stream = streamFn(makeModel(), makeContext());
		const events = await collectEvents(stream);
		const errEvent = events.find((e) => e.type === "error");

		expect(errEvent?.type).toBe("error");
		if (errEvent?.type === "error") {
			expect(errEvent.error.errorMessage).toContain("stream blew up");
			expect(typeof errEvent.error.duration).toBe("number");
			expect(errEvent.error.duration).toBeGreaterThan(0);
			expect(typeof errEvent.error.ttft).toBe("number");
			expect(errEvent.error.ttft).toBeGreaterThan(0);
		}
	});
});

/* ------------------------------------------------------------------ *
 * 5. Rotation
 * ------------------------------------------------------------------ */

describe("stream — quota rotation switches keys and retries", () => {
	test("user_a 400 insufficient credits → markUsageLimitReached once, user_b succeeds", async () => {
		const [chunkA, chunkB] = splitMidLine();
		const { fetch: fetchImpl, calls } = fetchByBearer({
			"Bearer user_a": () =>
				makeResponse([enc('{"error":{"message":"insufficient credits"}}\n')], 400),
			"Bearer user_b": () => makeResponse([chunkA, chunkB]),
		});
		const auth = stubAuthStorage({
			keys: ["user_a", "user_b"],
			markResult: { switched: true },
		});

		const streamFn = createCommandCodeStream({
			getAuthStorage: () => auth,
			getSessionId: () => "sess-1",
			getProjectSlug: () => "0123456789",
			fetchImpl,
		});

		const stream = streamFn(makeModel(), makeContext());
		const events = await collectEvents(stream);
		const last = events[events.length - 1];

		expect(last?.type).toBe("done");
		expect(auth.markCalls).toBe(1);
		expect(calls).toHaveLength(2);
		expect(calls[0]?.auth).toBe("Bearer user_a");
		expect(calls[1]?.auth).toBe("Bearer user_b");
	});
});

/* ------------------------------------------------------------------ *
 * 6. Exhaustion
 * ------------------------------------------------------------------ */

describe("stream — all keys exhausted fails fast with reset time", () => {
	test("switched:false → one terminal error naming quota exhausted + ISO reset, fetch once", async () => {
		const { fetch: fetchImpl, calls } = fetchByBearer({
			"Bearer user_a": () =>
				makeResponse([enc('{"error":{"message":"insufficient credits"}}\n')], 400),
		});
		const resetAt = 1_800_000_000_000;
		const auth = stubAuthStorage({
			keys: ["user_a"],
			markResult: { switched: false, retryAtMs: resetAt },
		});

		const streamFn = createCommandCodeStream({
			getAuthStorage: () => auth,
			getSessionId: () => "sess-1",
			getProjectSlug: () => "0123456789",
			fetchImpl,
		});

		const stream = streamFn(makeModel(), makeContext());
		const events = await collectEvents(stream);
		const errEvent = events.find((e) => e.type === "error");

		expect(errEvent?.type).toBe("error");
		if (errEvent?.type === "error") {
			expect(errEvent.error.errorMessage).toContain("quota exhausted");
			expect(errEvent.error.errorMessage).toContain(new Date(resetAt).toISOString());
		}
		expect(auth.markCalls).toBe(1);
		expect(calls).toHaveLength(1);
	});
});

/* ------------------------------------------------------------------ *
 * 7. rate-limit does not rotate
 * ------------------------------------------------------------------ */

describe("stream — rate-limit backs off without rotating", () => {
	test("two 429 rate_limit_error then valid stream; never marks/rotates", async () => {
		const [chunkA, chunkB] = splitMidLine();
		let count = 0;
		const calls: { auth: string }[] = [];
		const fetchImpl: typeof fetch = Object.assign(
			async (_input: URL | RequestInfo, init?: RequestInit | BunFetchRequestInit) => {
				const headers = init?.headers as Record<string, string> | undefined;
				calls.push({ auth: headers?.Authorization ?? "" });
				count += 1;
				if (count <= 2) {
					return makeResponse(
						[enc('{"error":{"type":"rate_limit_error","message":"Too many requests"}}\n')],
						429,
					);
				}
				return makeResponse([chunkA, chunkB]);
			},
			{ preconnect: () => undefined },
		);

		const auth = stubAuthStorage({ keys: ["user_test"] });

		const streamFn = createCommandCodeStream({
			getAuthStorage: () => auth,
			getSessionId: () => "sess-1",
			getProjectSlug: () => "0123456789",
			fetchImpl,
		});

		const stream = streamFn(makeModel(), makeContext());
		const events = await collectEvents(stream);
		const last = events[events.length - 1];

		expect(last?.type).toBe("done");
		expect(auth.markCalls).toBe(0);
		expect(auth.rotateCalls).toBe(0);
		expect(calls).toHaveLength(3);
	});
});

/* ------------------------------------------------------------------ *
 * 8. native ApiKeyResolver
 * ------------------------------------------------------------------ */

describe("stream — native ApiKeyResolver", () => {
	test("uses the resolver from options.apiKey and never touches AuthStorage", async () => {
		const [chunkA, chunkB] = splitMidLine();
		const { fetch: fetchImpl, calls } = fetchByBearer({
			"Bearer user_resolved": () => makeResponse([chunkA, chunkB]),
		});

		const streamFn = createCommandCodeStream({
			getAuthStorage: () => undefined,
			getSessionId: () => "sess-1",
			getProjectSlug: () => "0123456789",
			fetchImpl,
		});

		const stream = streamFn(makeModel(), makeContext(), {
			apiKey: (): string => "user_resolved",
		});
		const events = await collectEvents(stream);

		expect(events[events.length - 1]?.type).toBe("done");
		expect(calls).toHaveLength(1);
		expect(calls[0]?.auth).toBe("Bearer user_resolved");
	});

	test("401 walks the a/b/c steps and retries with the rotated key", async () => {
		const [chunkA, chunkB] = splitMidLine();
		const { fetch: fetchImpl, calls } = fetchByBearer({
			"Bearer user_1": () => makeResponse([enc('{"error":{"message":"unauthorized"}}\n')], 401),
			"Bearer user_2": () => makeResponse([chunkA, chunkB]),
		});

		const seen: { lastChance: boolean; previousKey: string | undefined }[] = [];
		const resolver: ApiKeyResolver = ({ lastChance, previousKey }: ApiKeyResolveContext) => {
			seen.push({ lastChance, previousKey });
			return lastChance ? "user_2" : "user_1";
		};

		const streamFn = createCommandCodeStream({
			getAuthStorage: () => undefined,
			getSessionId: () => "sess-1",
			getProjectSlug: () => "0123456789",
			fetchImpl,
		});

		const stream = streamFn(makeModel(), makeContext(), { apiKey: resolver });
		const events = await collectEvents(stream);

		expect(events[events.length - 1]?.type).toBe("done");
		expect(seen).toEqual([
			{ lastChance: false, previousKey: undefined },
			{ lastChance: false, previousKey: "user_1" },
			{ lastChance: true, previousKey: "user_1" },
		]);
		expect(calls).toHaveLength(2);
		expect(calls[0]?.auth).toBe("Bearer user_1");
		expect(calls[1]?.auth).toBe("Bearer user_2");
	});
});

/* ------------------------------------------------------------------ *
 * 9. extension registration
 * ------------------------------------------------------------------ */

/** The subset of ExtensionAPI the plugin's factory touches. */
interface FakeExtensionAPI {
	on(event: string, handler: (e: unknown, ctx: unknown) => void): void;
	registerProvider(name: string, config: ProviderConfig): void;
	registerCommand(name: string, options: unknown): void;
}

function fakeExtensionApi(): {
	pi: ExtensionAPI;
	provider: { name?: string; config?: ProviderConfig };
	commands: string[];
	sessionStart(): ((e: unknown, ctx: unknown) => void) | undefined;
} {
	const provider: { name?: string; config?: ProviderConfig } = {};
	const commands: string[] = [];
	let onSessionStart: ((e: unknown, ctx: unknown) => void) | undefined;
	const base: FakeExtensionAPI = {
		on: (event, handler) => {
			if (event === "session_start") onSessionStart = handler;
		},
		registerProvider: (name, config) => {
			provider.name = name;
			provider.config = config;
		},
		registerCommand: (name) => {
			commands.push(name);
		},
	};
	return { pi: base as ExtensionAPI, provider, commands, sessionStart: () => onSessionStart };
}

describe("extension registration", () => {
	const realFetch = globalThis.fetch;
	afterEach(() => {
		globalThis.fetch = realFetch;
	});

	test("registers the provider with native oauth login and no bespoke commands", () => {
		const { pi, provider, commands } = fakeExtensionApi();
		commandCodeProvider(pi);

		expect(provider.name).toBe(PROVIDER_ID);
		expect(provider.config?.api).toBe("commandcode-generate");
		expect(provider.config?.oauth?.name).toBe("Command Code");
		expect(typeof provider.config?.oauth?.login).toBe("function");
		expect(provider.config?.models).toBeUndefined();
		expect(provider.config?.fetchDynamicModels).toBe(fetchCommandCodeModels);
		expect(commands).toEqual([]);
	});

	test("session_start wires omp's session id onto the wire header", async () => {
		const { pi, provider, sessionStart } = fakeExtensionApi();
		commandCodeProvider(pi);

		const handler = sessionStart();
		expect(handler).toBeDefined();
		handler?.(undefined, {
			modelRegistry: { authStorage: stubAuthStorage({ keys: ["user_test"] }) },
			sessionManager: { getSessionId: () => "sess-native" },
			cwd: "/tmp/cc-test",
		});

		const [chunkA, chunkB] = splitMidLine();
		const seen: Record<string, string>[] = [];
		globalThis.fetch = Object.assign(
			async (_input: URL | RequestInfo, init?: RequestInit | BunFetchRequestInit) => {
				seen.push((init?.headers as Record<string, string>) ?? {});
				return makeResponse([chunkA, chunkB]);
			},
			{ preconnect: () => undefined },
		);

		const streamSimple = provider.config?.streamSimple;
		expect(streamSimple).toBeDefined();
		const stream = streamSimple?.(makeModel(), makeContext(), {
			apiKey: (): string => "user_resolved",
		});
		expect(stream).toBeDefined();
		if (!stream) return;
		const events = await collectEvents(stream);

		expect(events[events.length - 1]?.type).toBe("done");
		expect(seen[0]?.["x-session-id"]).toBe("sess-native");
	});
});
