/**
 * Live Command Code catalog from `GET /provider/v1/models`.
 *
 * The public list is an OpenAI-shaped envelope
 * `{ object: "list", data: [{ id, name, context_length, ... }] }`.
 * It does not advertise reasoning, vision, or USD cost. Cost is always
 * zero because Command Code bills credits. `maxTokens` stays the CLI
 * default. `reasoning` and `input` are left unset so omp's
 * `finalizeCustomModel` inherits them from the bundled catalog by bare
 * id; unknown ids fall back to text-only / no-reasoning.
 */

import type { ProviderModelConfig } from "@oh-my-pi/pi-coding-agent";

import { DEFAULT_MAX_TOKENS, resolveBaseUrl } from "./api";

const ZERO_COST = {
	input: 0,
	output: 0,
	cacheRead: 0,
	cacheWrite: 0,
} as const;

export const MODELS_PATH = "/provider/v1/models";

/**
 * Bound the discovery request so a host that accepts the connection but
 * never completes the response cannot hang catalog fetch indefinitely.
 * Kept under omp's 15 s dynamic-model wrapper timeout so we fail first.
 */
export const DISCOVERY_TIMEOUT_MS = 10_000;

/** The vendor's `qn` default. */
export const DEFAULT_MODEL_ID = "deepseek/deepseek-v4-flash";

/** Fields the public list actually supplies, plus the two constants we fill. */
export type DiscoveredModelConfig = Pick<
	ProviderModelConfig,
	"id" | "name" | "cost" | "contextWindow" | "maxTokens"
>;

function asObject(value: unknown): Record<string, unknown> | undefined {
	return typeof value === "object" && value !== null && !Array.isArray(value)
		? (value as Record<string, unknown>)
		: undefined;
}

/**
 * Parse a `/provider/v1/models` JSON body.
 * Skips malformed rows and later duplicates. Throws when the envelope is
 * wrong or nothing valid remains — a successful empty list would wipe
 * omp's cached catalog.
 */
export function parseModelsList(body: unknown): DiscoveredModelConfig[] {
	const envelope = asObject(body);
	if (envelope === undefined || !Array.isArray(envelope.data)) {
		throw new Error("Command Code models: expected { data: Model[] }");
	}
	const seen = new Set<string>();
	const out: DiscoveredModelConfig[] = [];
	for (const item of envelope.data) {
		const row = asObject(item);
		if (row === undefined) continue;
		const id = typeof row.id === "string" ? row.id.trim() : "";
		if (id === "" || seen.has(id)) continue;
		const rawName = typeof row.name === "string" ? row.name.trim() : "";
		const contextWindow = row.context_length;
		if (
			typeof contextWindow !== "number" ||
			!Number.isFinite(contextWindow) ||
			contextWindow <= 0
		) {
			continue;
		}
		seen.add(id);
		out.push({
			id,
			name: rawName === "" ? id : rawName,
			cost: ZERO_COST,
			contextWindow,
			maxTokens: DEFAULT_MAX_TOKENS,
		});
	}
	if (out.length === 0) {
		throw new Error("Command Code models: empty catalog");
	}
	return out;
}

/**
 * Fetch the live catalog. The endpoint is public; a stored key is ignored
 * so a blocked credential cannot take discovery down with it.
 */
export async function fetchCommandCodeModels(
	_apiKey?: string,
	baseUrl: string = resolveBaseUrl(),
	timeoutMs: number = DISCOVERY_TIMEOUT_MS,
): Promise<readonly ProviderModelConfig[]> {
	const url = `${baseUrl.replace(/\/$/, "")}${MODELS_PATH}`;
	let response: Response;
	try {
		response = await fetch(url, {
			headers: { Accept: "application/json" },
			signal: AbortSignal.timeout(timeoutMs),
		});
	} catch (error) {
		if (isAbortFailure(error)) {
			throw new Error(`Command Code models: timed out after ${timeoutMs}ms`);
		}
		throw error;
	}
	if (!response.ok) {
		throw new Error(`Command Code models: HTTP ${response.status}`);
	}
	return parseModelsList(await response.json()) as ProviderModelConfig[];
}

/** `AbortSignal.timeout` rejects with a `TimeoutError` DOMException. */
function isAbortFailure(error: unknown): boolean {
	return error instanceof Error && (error.name === "TimeoutError" || error.name === "AbortError");
}
