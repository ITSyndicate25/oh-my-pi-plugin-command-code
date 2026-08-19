import { createHash } from "node:crypto";
import type { AuthStorage } from "@oh-my-pi/pi-ai";
import type { ExtensionAPI } from "@oh-my-pi/pi-coding-agent";

import { API_ID, PROVIDER_ID, resolveBaseUrl } from "./src/api";
import { loginWithCommandCode } from "./src/login";
import { fetchCommandCodeModels } from "./src/models";
import { createCommandCodeStream } from "./src/stream";

// Captured on session_start; read by the stream.
let authStorage: AuthStorage | undefined;
let getSessionId: () => string | undefined = () => undefined;
let projectSlug = "0000000000";

export default function commandCodeProvider(pi: ExtensionAPI): void {
	pi.on("session_start", (_e, ctx) => {
		authStorage = ctx.modelRegistry.authStorage;
		// Read live per request rather than snapshotting, so `/session new`
		// re-keys the sticky credential and the wire threadId together.
		// `|| undefined` keeps AuthStorage's sticky key absent rather than "".
		getSessionId = () => ctx.sessionManager.getSessionId() || undefined;
		projectSlug = createHash("sha256").update(ctx.cwd).digest("hex").slice(0, 10);
	});

	pi.registerProvider(PROVIDER_ID, {
		baseUrl: resolveBaseUrl(),
		api: API_ID,
		fetchDynamicModels: fetchCommandCodeModels,
		streamSimple: createCommandCodeStream({
			getAuthStorage: () => authStorage,
			getSessionId: () => getSessionId(),
			getProjectSlug: () => projectSlug,
		}),
		// streamSimple owns the Authorization header, so no authHeader/apiKey.
		// Credentials are entirely native: /login appends a key, /logout removes
		// one, and omp's ApiKeyResolver picks and rotates the bearer.
		oauth: { name: "Command Code", login: loginWithCommandCode },
	});
}
