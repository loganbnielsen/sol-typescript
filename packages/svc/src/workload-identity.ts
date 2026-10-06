// DEC-063 callee side: a Sol service authenticates Sol-to-Sol callers by their
// projected workload identity, then authorizes the caller unit against the
// `called_by` set Sol derived from the caller-owned `calls` graph.
//
// The trust root is target/infrastructure truth: Sol discovers the Kubernetes
// OIDC issuer and projects it as SOL_TRUSTED_WORKLOAD_ISSUER. This module never
// trusts the incoming token's `iss` on its own -- the token issuer must equal
// the projected issuer, and the JWKS is fetched from that projected issuer's
// OIDC discovery document, whose `issuer` must also match.
//
// Verification uses Node's built-in WebCrypto, so @sol-fab/svc stays free of
// runtime dependencies. This mirrors the OCaml sol-svc-core auth contract
// (audience, claims mapping, error codes) rather than its module structure.

import { webcrypto } from "node:crypto";

/** JWKS cache TTL, matching OCaml Auth_cache.ttl_s. */
const JWKS_TTL_MS = 300_000;
/** Unknown-kid refetch floor, matching OCaml Auth_cache.unknown_kid_refetch_interval_s. */
const UNKNOWN_KID_REFETCH_MS = 30_000;
/** Failure backoff, matching OCaml Auth_cache.failure_backoff_s. */
const FAILURE_BACKOFF_MS = 5_000;
const DISCOVERY_TIMEOUT_MS = 10_000;
const SERVICE_ACCOUNT_PREFIX = "system:serviceaccount:";

/** The authenticated Sol caller. */
export interface WorkloadIdentity {
  /** The caller's Sol unit, e.g. "payments/charge-svc". */
  readonly unitId: string;
  /** The proved Kubernetes service account, e.g. "myapp-payments:charge-svc". */
  readonly serviceAccount: string;
}

export interface WorkloadIdentityConfig {
  /** The callee's own unit (`SOL_UNIT`); the token audience must include it. */
  readonly audience: string;
  /** "<namespace>:<serviceaccount>" -> caller unit, derived by Sol (`SOL_CALLED_BY`). */
  readonly callers: ReadonlyMap<string, string>;
  /** The target-established Kubernetes OIDC issuer (`SOL_TRUSTED_WORKLOAD_ISSUER`). */
  readonly trustedIssuer: string;
}

export type WorkloadIdentityDenialStatus = 401 | 403 | 500;

export type WorkloadIdentityResult =
  | { readonly ok: true; readonly identity: WorkloadIdentity }
  | { readonly ok: false; readonly status: WorkloadIdentityDenialStatus; readonly message: string };

/** Parse Sol's projected `SOL_CALLED_BY`: "<unit>=<namespace>:<serviceaccount>", comma separated. */
export function parseCalledBy(raw: string | undefined): Map<string, string> {
  const callers = new Map<string, string>();
  if (raw === undefined) return callers;
  for (const entry of raw.split(",")) {
    const separator = entry.indexOf("=");
    if (separator < 0) continue;
    const unitId = entry.slice(0, separator).trim();
    const serviceAccount = entry.slice(separator + 1).trim();
    if (unitId && serviceAccount) callers.set(serviceAccount, unitId);
  }
  return callers;
}

/**
 * Build the verification config from the environment Sol projects. Throws when
 * the callee's unit or the target trust root is missing, so a service that
 * requires workload identity fails closed instead of starting unprotected.
 */
export function workloadIdentityConfigFromEnv(
  env: NodeJS.ProcessEnv = process.env,
): WorkloadIdentityConfig {
  const audience = env.SOL_UNIT?.trim();
  if (!audience) {
    throw new Error(
      "SOL_UNIT is not set: the callee's own unit is required to check the token audience",
    );
  }
  const trustedIssuer = env.SOL_TRUSTED_WORKLOAD_ISSUER?.trim();
  if (!trustedIssuer) {
    throw new Error(
      "SOL_TRUSTED_WORKLOAD_ISSUER is not set: Sol projects the target's trusted workload " +
        "issuer, and workload identity must fail closed without it",
    );
  }
  return { audience, callers: parseCalledBy(env.SOL_CALLED_BY), trustedIssuer };
}

interface JwsParts {
  readonly header: Record<string, unknown>;
  readonly payload: Record<string, unknown>;
  readonly signingInput: Uint8Array;
  readonly signature: Uint8Array;
}

function decodeBase64Url(value: string): Uint8Array {
  return new Uint8Array(Buffer.from(value, "base64url"));
}

function decodeJsonObject(value: string): Record<string, unknown> | undefined {
  try {
    const parsed: unknown = JSON.parse(Buffer.from(value, "base64url").toString("utf8"));
    if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
    return undefined;
  } catch {
    return undefined;
  }
}

function parseJws(token: string): JwsParts | undefined {
  const segments = token.split(".");
  if (segments.length !== 3) return undefined;
  const [headerB64, payloadB64, signatureB64] = segments as [string, string, string];
  const header = decodeJsonObject(headerB64);
  const payload = decodeJsonObject(payloadB64);
  if (!header || !payload) return undefined;
  return {
    header,
    payload,
    signingInput: new TextEncoder().encode(`${headerB64}.${payloadB64}`),
    signature: decodeBase64Url(signatureB64),
  };
}

interface SignatureAlgorithm {
  readonly importAlgorithm: RsaHashedImportParams | EcKeyImportParams;
  readonly verifyAlgorithm: AlgorithmIdentifier | EcdsaParams;
}

function signatureAlgorithm(alg: unknown): SignatureAlgorithm | undefined {
  switch (alg) {
    case "RS256":
      return {
        importAlgorithm: { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
        verifyAlgorithm: { name: "RSASSA-PKCS1-v1_5" },
      };
    case "ES256":
      return {
        importAlgorithm: { name: "ECDSA", namedCurve: "P-256" },
        verifyAlgorithm: { name: "ECDSA", hash: "SHA-256" },
      };
    case "ES384":
      return {
        importAlgorithm: { name: "ECDSA", namedCurve: "P-384" },
        verifyAlgorithm: { name: "ECDSA", hash: "SHA-384" },
      };
    case "ES512":
      return {
        importAlgorithm: { name: "ECDSA", namedCurve: "P-521" },
        verifyAlgorithm: { name: "ECDSA", hash: "SHA-512" },
      };
    default:
      return undefined;
  }
}

/** WebCrypto's JsonWebKey omits `kid`, which JOSE JWKS documents carry. */
type Jwk = JsonWebKey & { readonly kid?: string };

interface Jwks {
  readonly keys: readonly Jwk[];
}

async function getJson(fetchImpl: typeof fetch, url: string): Promise<Record<string, unknown>> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), DISCOVERY_TIMEOUT_MS);
  try {
    const response = await fetchImpl(url, {
      headers: { accept: "application/json" },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`${url} returned HTTP ${response.status}`);
    const body: unknown = await response.json();
    if (body === null || typeof body !== "object" || Array.isArray(body)) {
      throw new Error(`${url} did not return a JSON object`);
    }
    return body as Record<string, unknown>;
  } finally {
    clearTimeout(timer);
  }
}

function requireHttpsUrl(value: string, what: string): URL {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${what} is not an absolute https:// URL`);
  }
  if (url.protocol !== "https:" || !url.host) {
    throw new Error(`${what} is not an absolute https:// URL`);
  }
  return url;
}

/**
 * Fetch the JWKS for the projected trusted issuer through its OIDC discovery
 * document. The discovery document's `issuer` must equal the projected issuer:
 * a redirect or spoofed document cannot move the trust root.
 */
async function fetchWorkloadJwks(issuer: string, fetchImpl: typeof fetch): Promise<Jwks> {
  requireHttpsUrl(issuer, "trusted workload issuer");
  const base = issuer.endsWith("/") ? issuer.slice(0, -1) : issuer;
  const discovery = await getJson(fetchImpl, `${base}/.well-known/openid-configuration`);
  if (discovery.issuer !== issuer) {
    throw new Error("OIDC discovery issuer does not match the trusted target issuer");
  }
  const jwksUri = discovery.jwks_uri;
  if (typeof jwksUri !== "string" || jwksUri === "") {
    throw new Error("OIDC discovery document has no jwks_uri");
  }
  requireHttpsUrl(jwksUri, "OIDC discovery jwks_uri");
  const document = await getJson(fetchImpl, jwksUri);
  if (!Array.isArray(document.keys)) throw new Error("JWKS document has no keys array");
  return { keys: document.keys as Jwk[] };
}

/** Per-issuer JWKS cache. Exported so tests (and long-lived processes) can isolate state. */
export class WorkloadJwksCache {
  private readonly entries = new Map<string, { jwks: Jwks; fetchedAt: number }>();
  private readonly failures = new Map<string, { at: number; message: string }>();

  clear(): void {
    this.entries.clear();
    this.failures.clear();
  }

  /** Return JWKS no older than `maxAgeMs`, else refetch. */
  private async resolve(
    issuer: string,
    maxAgeMs: number,
    fetchImpl: typeof fetch,
    now: number,
  ): Promise<Jwks> {
    const entry = this.entries.get(issuer);
    if (entry && now - entry.fetchedAt < maxAgeMs) return entry.jwks;
    const failure = this.failures.get(issuer);
    if (failure && now - failure.at < FAILURE_BACKOFF_MS) throw new Error(failure.message);
    try {
      const jwks = await fetchWorkloadJwks(issuer, fetchImpl);
      this.entries.set(issuer, { jwks, fetchedAt: now });
      this.failures.delete(issuer);
      return jwks;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.failures.set(issuer, { at: now, message });
      throw error;
    }
  }

  get(issuer: string, fetchImpl: typeof fetch, now: number): Promise<Jwks> {
    return this.resolve(issuer, JWKS_TTL_MS, fetchImpl, now);
  }

  /** A token named a key we do not hold; refetch, but no more often than the floor. */
  refresh(issuer: string, fetchImpl: typeof fetch, now: number): Promise<Jwks> {
    return this.resolve(issuer, UNKNOWN_KID_REFETCH_MS, fetchImpl, now);
  }
}

const sharedJwksCache = new WorkloadJwksCache();

/** Drop cached JWKS (tests; long-lived processes that must re-read trust). */
export function resetWorkloadIdentityCache(): void {
  sharedJwksCache.clear();
}

export interface VerifyWorkloadIdentityOptions {
  readonly fetchImpl?: typeof fetch;
  readonly now?: () => number;
  readonly cache?: WorkloadJwksCache;
}

function deny(
  status: WorkloadIdentityDenialStatus,
  message: string,
): WorkloadIdentityResult {
  return { ok: false, status, message };
}

function bearerToken(authorization: string | undefined): string | undefined {
  if (authorization === undefined) return undefined;
  const prefix = "Bearer ";
  if (!authorization.startsWith(prefix)) return undefined;
  const token = authorization.slice(prefix.length);
  return token === "" ? undefined : token;
}

function claimStrings(claims: Record<string, unknown>, name: string): string[] {
  const value = claims[name];
  if (typeof value === "string") return [value];
  if (Array.isArray(value)) return value.filter((item): item is string => typeof item === "string");
  return [];
}

function numericDate(claims: Record<string, unknown>, name: string): number | undefined | null {
  const value = claims[name];
  if (value === undefined || value === null) return undefined;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  return null;
}

async function verifySignature(
  algorithm: SignatureAlgorithm,
  jwk: Jwk,
  jws: JwsParts,
): Promise<boolean> {
  const key = await webcrypto.subtle.importKey(
    "jwk",
    jwk,
    algorithm.importAlgorithm,
    false,
    ["verify"],
  );
  return webcrypto.subtle.verify(
    algorithm.verifyAlgorithm as AlgorithmIdentifier,
    key,
    jws.signature,
    jws.signingInput,
  );
}

/**
 * Verify a projected ServiceAccount token and authorize its caller unit.
 *
 * 401: the caller is not authenticated (missing/malformed token, untrusted
 * issuer, bad signature, wrong audience, outside the validity window).
 * 403: the caller is authenticated but is not a Kubernetes workload identity or
 * its unit is not in the derived caller set.
 * 500: the trust root itself could not be resolved (discovery/JWKS); this fails
 * closed rather than treating the caller as unauthenticated.
 */
export async function verifyWorkloadIdentity(
  authorization: string | undefined,
  config: WorkloadIdentityConfig,
  options: VerifyWorkloadIdentityOptions = {},
): Promise<WorkloadIdentityResult> {
  const token = bearerToken(authorization);
  if (token === undefined) return deny(401, "Missing or malformed Authorization header");

  const jws = parseJws(token);
  if (jws === undefined) return deny(401, "Malformed JWT");

  const algorithm = signatureAlgorithm(jws.header.alg);
  if (algorithm === undefined) return deny(401, "JWT alg not permitted");

  const issuer = claimStrings(jws.payload, "iss")[0];
  if (issuer === undefined) return deny(401, "JWT issuer missing");
  if (issuer !== config.trustedIssuer) return deny(401, `JWT issuer is not trusted: ${issuer}`);

  const kid = jws.header.kid;
  if (typeof kid !== "string" || kid === "") return deny(401, "JWT missing kid");

  const fetchImpl = options.fetchImpl ?? fetch;
  const now = (options.now ?? Date.now)();
  const cache = options.cache ?? sharedJwksCache;

  let jwks: Jwks;
  try {
    jwks = await cache.get(config.trustedIssuer, fetchImpl, now);
  } catch (error) {
    return deny(500, `JWKS fetch failed: ${error instanceof Error ? error.message : String(error)}`);
  }

  let jwk = jwks.keys.find((candidate) => candidate.kid === kid);
  if (jwk === undefined) {
    try {
      jwks = await cache.refresh(config.trustedIssuer, fetchImpl, now);
      jwk = jwks.keys.find((candidate) => candidate.kid === kid);
    } catch {
      // A failed refetch leaves the cached key set authoritative (OCaml parity).
    }
  }
  if (jwk === undefined) return deny(401, "JWT key id not found in JWKS");

  let signatureValid: boolean;
  try {
    signatureValid = await verifySignature(algorithm, jwk, jws);
  } catch (error) {
    return deny(401, `JWT invalid: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (!signatureValid) return deny(401, "JWT signature invalid");

  const notBefore = numericDate(jws.payload, "nbf");
  const expires = numericDate(jws.payload, "exp");
  if (notBefore === null || expires === null) return deny(401, "JWT time claim is invalid");
  const nowSeconds = now / 1000;
  if (notBefore !== undefined && nowSeconds < notBefore) return deny(401, "JWT not yet valid");
  if (expires !== undefined && nowSeconds >= expires) return deny(401, "JWT expired");

  if (!claimStrings(jws.payload, "aud").includes(config.audience)) {
    return deny(401, "JWT audience mismatch");
  }

  const subject = claimStrings(jws.payload, "sub")[0];
  if (subject === undefined || !subject.startsWith(SERVICE_ACCOUNT_PREFIX)) {
    return deny(403, `authenticated subject ${subject ?? "(missing)"} is not a workload identity`);
  }
  const serviceAccount = subject.slice(SERVICE_ACCOUNT_PREFIX.length);
  const unitId = config.callers.get(serviceAccount);
  if (unitId === undefined) {
    return deny(403, `caller ${serviceAccount} is authenticated but is not in this unit's callers set`);
  }
  return { ok: true, identity: { unitId, serviceAccount } };
}

export interface WorkloadIdentityRequest {
  /** The `Authorization` header value, if any. */
  readonly authorization?: string | undefined;
  /** Optional request path, for an explicit public-path exception. */
  readonly path?: string | undefined;
}

export type WorkloadIdentityDecision =
  | { readonly ok: true; readonly identity: WorkloadIdentity | null }
  | { readonly ok: false; readonly status: WorkloadIdentityDenialStatus; readonly message: string };

export interface WorkloadIdentityGuardOptions {
  /** Defaults to the Sol-projected environment; throws (fails closed) when incomplete. */
  readonly config?: WorkloadIdentityConfig;
  /** Explicit exception. Everything not named public is authenticated. */
  readonly isPublic?: (request: WorkloadIdentityRequest) => boolean;
  readonly fetchImpl?: typeof fetch;
  readonly now?: () => number;
  readonly cache?: WorkloadJwksCache;
}

export interface WorkloadIdentityGuard {
  /** Authenticate and authorize one request. */
  check(request: WorkloadIdentityRequest): Promise<WorkloadIdentityDecision>;
}

/**
 * Internal-by-default guard: a request is authenticated unless `isPublic`
 * explicitly exempts it, so a new route cannot be accidentally external. Wire
 * `guard.check` into the app's request pipeline (a Fastify preHandler, an
 * Express middleware, ...) and map a denial to its status.
 */
export function createWorkloadIdentityGuard(
  options: WorkloadIdentityGuardOptions = {},
): WorkloadIdentityGuard {
  const config = options.config ?? workloadIdentityConfigFromEnv();
  return {
    async check(request) {
      if (options.isPublic?.(request) === true) return { ok: true, identity: null };
      const result = await verifyWorkloadIdentity(request.authorization, config, {
        fetchImpl: options.fetchImpl,
        now: options.now,
        cache: options.cache,
      });
      if (result.ok) return { ok: true, identity: result.identity };
      return { ok: false, status: result.status, message: result.message };
    },
  };
}
