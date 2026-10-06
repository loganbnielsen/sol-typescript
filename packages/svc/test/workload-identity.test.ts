import { test } from "node:test";
import assert from "node:assert/strict";
import { webcrypto } from "node:crypto";
import {
  WorkloadJwksCache,
  createWorkloadIdentityGuard,
  parseCalledBy,
  verifyWorkloadIdentity,
  workloadIdentityConfigFromEnv,
  type WorkloadIdentityConfig,
} from "../src/index.js";

const ISSUER = "https://oidc.eks.us-east-1.amazonaws.com/id/cluster";
const AUDIENCE = "checkout/checkout-svc";
const CALLER_SERVICE_ACCOUNT = "myapp-payments:charge-svc";
const CALLER_SUBJECT = `system:serviceaccount:${CALLER_SERVICE_ACCOUNT}`;
const CALLER_UNIT = "payments/charge-svc";
const JWKS_URI = `${ISSUER}/openid/v1/jwks`;

function config(overrides: Partial<WorkloadIdentityConfig> = {}): WorkloadIdentityConfig {
  return {
    audience: AUDIENCE,
    callers: new Map([[CALLER_SERVICE_ACCOUNT, CALLER_UNIT]]),
    trustedIssuer: ISSUER,
    ...overrides,
  };
}

interface RsaFixture {
  readonly privateKey: CryptoKey;
  readonly jwk: JsonWebKey;
}

async function rsaFixture(kid: string): Promise<RsaFixture> {
  const pair = (await webcrypto.subtle.generateKey(
    {
      name: "RSASSA-PKCS1-v1_5",
      modulusLength: 2048,
      publicExponent: new Uint8Array([1, 0, 1]),
      hash: "SHA-256",
    },
    true,
    ["sign", "verify"],
  )) as CryptoKeyPair;
  const jwk = (await webcrypto.subtle.exportKey("jwk", pair.publicKey)) as JsonWebKey;
  return { privateKey: pair.privateKey, jwk: { ...jwk, kid, alg: "RS256", use: "sig" } };
}

async function ecFixture(kid: string): Promise<RsaFixture> {
  const pair = (await webcrypto.subtle.generateKey(
    { name: "ECDSA", namedCurve: "P-256" },
    true,
    ["sign", "verify"],
  )) as CryptoKeyPair;
  const jwk = (await webcrypto.subtle.exportKey("jwk", pair.publicKey)) as JsonWebKey;
  return { privateKey: pair.privateKey, jwk: { ...jwk, kid, alg: "ES256", use: "sig" } };
}

type Claims = Record<string, unknown>;

function baseClaims(overrides: Claims = {}): Claims {
  const now = Math.floor(Date.now() / 1000);
  return {
    iss: ISSUER,
    aud: AUDIENCE,
    sub: CALLER_SUBJECT,
    iat: now,
    nbf: now - 1,
    exp: now + 3600,
    ...overrides,
  };
}

async function sign(claims: Claims, privateKey: CryptoKey, kid: string, alg = "RS256") {
  const header = Buffer.from(JSON.stringify({ alg, kid, typ: "JWT" })).toString("base64url");
  const payload = Buffer.from(JSON.stringify(claims)).toString("base64url");
  const data = new TextEncoder().encode(`${header}.${payload}`);
  const algorithm = alg === "ES256"
    ? ({ name: "ECDSA", hash: "SHA-256" } as EcdsaParams)
    : "RSASSA-PKCS1-v1_5";
  const signature = new Uint8Array(
    await webcrypto.subtle.sign(algorithm, privateKey, data),
  );
  return `${header}.${payload}.${Buffer.from(signature).toString("base64url")}`;
}

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

interface FetchCounters {
  discovery: number;
  jwks: number;
}

function fetchFor(keys: readonly JsonWebKey[] | (() => readonly JsonWebKey[]), counters: FetchCounters) {
  const current = typeof keys === "function" ? keys : () => keys;
  return (async (input: string | URL | Request) => {
    const url = typeof input === "string" ? input : input.toString();
    if (url === `${ISSUER}/.well-known/openid-configuration`) {
      counters.discovery += 1;
      return jsonResponse({ issuer: ISSUER, jwks_uri: JWKS_URI });
    }
    if (url === JWKS_URI) {
      counters.jwks += 1;
      return jsonResponse({ keys: current() });
    }
    return new Response("not found", { status: 404 });
  }) as typeof fetch;
}

function options(keys: readonly JsonWebKey[] | (() => readonly JsonWebKey[]), counters: FetchCounters) {
  return { cache: new WorkloadJwksCache(), fetchImpl: fetchFor(keys, counters) };
}

test("a declared caller authenticates and is authorized to its unit", async () => {
  const { privateKey, jwk } = await rsaFixture("key-1");
  const counters = { discovery: 0, jwks: 0 };
  const token = await sign(baseClaims(), privateKey, "key-1");
  const result = await verifyWorkloadIdentity(`Bearer ${token}`, config(), options([jwk], counters));
  assert.deepEqual(result, {
    ok: true,
    identity: { unitId: CALLER_UNIT, serviceAccount: CALLER_SERVICE_ACCOUNT },
  });
  assert.equal(counters.discovery, 1);
  assert.equal(counters.jwks, 1);
});

test("an ES256 projection verifies as well", async () => {
  const { privateKey, jwk } = await ecFixture("ec-1");
  const counters = { discovery: 0, jwks: 0 };
  const token = await sign(baseClaims(), privateKey, "ec-1", "ES256");
  const result = await verifyWorkloadIdentity(`Bearer ${token}`, config(), options([jwk], counters));
  assert.equal(result.ok, true);
});

test("an authenticated but undeclared caller is a 403", async () => {
  const { privateKey, jwk } = await rsaFixture("key-1");
  const counters = { discovery: 0, jwks: 0 };
  const token = await sign(baseClaims(), privateKey, "key-1");
  const result = await verifyWorkloadIdentity(
    `Bearer ${token}`,
    config({ callers: new Map() }),
    options([jwk], counters),
  );
  assert.equal(result.ok, false);
  assert.equal(result.ok === false && result.status, 403);
});

test("an untrusted token issuer is a 401 and never touches the network", async () => {
  const { privateKey, jwk } = await rsaFixture("key-1");
  let fetched = false;
  const fetchImpl = (async () => {
    fetched = true;
    return new Response("{}", { status: 200 });
  }) as typeof fetch;
  const token = await sign(baseClaims({ iss: "https://attacker.test" }), privateKey, "key-1");
  const result = await verifyWorkloadIdentity(`Bearer ${token}`, config(), {
    cache: new WorkloadJwksCache(),
    fetchImpl,
  });
  assert.equal(result.ok === false && result.status, 401);
  assert.equal(fetched, false);
  assert.equal(jwk.kid, "key-1");
});

test("a token for another audience is a 401", async () => {
  const { privateKey, jwk } = await rsaFixture("key-1");
  const counters = { discovery: 0, jwks: 0 };
  const token = await sign(baseClaims({ aud: "payments/other" }), privateKey, "key-1");
  const result = await verifyWorkloadIdentity(`Bearer ${token}`, config(), options([jwk], counters));
  assert.equal(result.ok === false && result.status, 401);
});

test("a tampered signature is a 401", async () => {
  const { privateKey, jwk } = await rsaFixture("key-1");
  const counters = { discovery: 0, jwks: 0 };
  const token = await sign(baseClaims(), privateKey, "key-1");
  const tampered = `${token.slice(0, -6)}AAAAAA`;
  const result = await verifyWorkloadIdentity(`Bearer ${tampered}`, config(), options([jwk], counters));
  assert.equal(result.ok === false && result.status, 401);
});

test("an expired token is a 401", async () => {
  const { privateKey, jwk } = await rsaFixture("key-1");
  const counters = { discovery: 0, jwks: 0 };
  const now = Math.floor(Date.now() / 1000);
  const token = await sign(baseClaims({ exp: now - 10 }), privateKey, "key-1");
  const result = await verifyWorkloadIdentity(`Bearer ${token}`, config(), options([jwk], counters));
  assert.equal(result.ok === false && result.status, 401);
});

test("a verified non-workload subject is a 403", async () => {
  const { privateKey, jwk } = await rsaFixture("key-1");
  const counters = { discovery: 0, jwks: 0 };
  const token = await sign(baseClaims({ sub: "user-123" }), privateKey, "key-1");
  const result = await verifyWorkloadIdentity(`Bearer ${token}`, config(), options([jwk], counters));
  assert.equal(result.ok === false && result.status, 403);
});

test("a missing or non-bearer Authorization header is a 401", async () => {
  const { jwk } = await rsaFixture("key-1");
  const counters = { discovery: 0, jwks: 0 };
  const fetchImpl = fetchFor([jwk], counters);
  assert.equal(
    (await verifyWorkloadIdentity(undefined, config(), { fetchImpl })).ok === false,
    true,
  );
  assert.equal(
    (await verifyWorkloadIdentity("Basic abc", config(), { fetchImpl })).ok === false,
    true,
  );
  assert.equal(counters.jwks, 0);
});

test("an HS256 token is refused: the signature must come from the trusted issuer's keys", async () => {
  const { privateKey, jwk } = await rsaFixture("key-1");
  const counters = { discovery: 0, jwks: 0 };
  const token = await sign(baseClaims(), privateKey, "key-1", "HS256");
  const result = await verifyWorkloadIdentity(`Bearer ${token}`, config(), options([jwk], counters));
  assert.equal(result.ok === false && result.status, 401);
});

test("a discovery document naming another issuer is a fail-closed 500", async () => {
  const { privateKey, jwk } = await rsaFixture("key-1");
  const token = await sign(baseClaims(), privateKey, "key-1");
  const fetchImpl = (async (input: string | URL | Request) => {
    const url = typeof input === "string" ? input : input.toString();
    if (url === `${ISSUER}/.well-known/openid-configuration`) {
      return jsonResponse({ issuer: "https://attacker.test", jwks_uri: JWKS_URI });
    }
    return jsonResponse({ keys: [jwk] });
  }) as typeof fetch;
  const result = await verifyWorkloadIdentity(`Bearer ${token}`, config(), {
    cache: new WorkloadJwksCache(),
    fetchImpl,
  });
  assert.equal(result.ok === false && result.status, 500);
});

test("JWKS is cached across calls", async () => {
  const { privateKey, jwk } = await rsaFixture("key-1");
  const counters = { discovery: 0, jwks: 0 };
  const verifyOptions = options([jwk], counters);
  const token = await sign(baseClaims(), privateKey, "key-1");
  await verifyWorkloadIdentity(`Bearer ${token}`, config(), verifyOptions);
  await verifyWorkloadIdentity(`Bearer ${token}`, config(), verifyOptions);
  assert.equal(counters.discovery, 1);
  assert.equal(counters.jwks, 1);
});

test("an unknown kid refetches once the cached key set is old enough", async () => {
  const { privateKey, jwk } = await rsaFixture("key-2");
  const counters = { discovery: 0, jwks: 0 };
  let clock = Date.now();
  const verifyOptions = {
    cache: new WorkloadJwksCache(),
    fetchImpl: fetchFor(() => (counters.jwks === 1 ? [] : [jwk]), counters),
    now: () => clock,
  };
  const token = await sign(baseClaims(), privateKey, "key-2");

  // The first key set predates the key and is still fresh, so the refetch is
  // rate-limited and the token is refused.
  const first = await verifyWorkloadIdentity(`Bearer ${token}`, config(), verifyOptions);
  assert.equal(first.ok, false);
  assert.equal(counters.jwks, 1);

  // Past the refetch floor the same token verifies against the refreshed set.
  clock += 31_000;
  const second = await verifyWorkloadIdentity(`Bearer ${token}`, config(), verifyOptions);
  assert.equal(second.ok, true);
  assert.equal(counters.jwks, 2);
});

test("workloadIdentityConfigFromEnv fails closed without the projection", () => {
  assert.throws(() => workloadIdentityConfigFromEnv({}), /SOL_UNIT/);
  assert.throws(
    () => workloadIdentityConfigFromEnv({ SOL_UNIT: AUDIENCE }),
    /SOL_TRUSTED_WORKLOAD_ISSUER/,
  );
  const parsed = workloadIdentityConfigFromEnv({
    SOL_UNIT: AUDIENCE,
    SOL_TRUSTED_WORKLOAD_ISSUER: ISSUER,
    SOL_CALLED_BY: `${CALLER_UNIT}=${CALLER_SERVICE_ACCOUNT},broken,=x`,
  });
  assert.equal(parsed.audience, AUDIENCE);
  assert.equal(parsed.trustedIssuer, ISSUER);
  assert.equal(parsed.callers.get(CALLER_SERVICE_ACCOUNT), CALLER_UNIT);
  assert.equal(parsed.callers.size, 1);
});

test("parseCalledBy keeps well-formed entries and drops malformed ones", () => {
  const callers = parseCalledBy("a/b=ns:sa,c/d=ns2:sa2,");
  assert.equal(callers.get("ns:sa"), "a/b");
  assert.equal(callers.get("ns2:sa2"), "c/d");
  assert.equal(callers.size, 2);
});

test("the guard authenticates by default and exempts only explicit public requests", async () => {
  const { privateKey, jwk } = await rsaFixture("key-1");
  const counters = { discovery: 0, jwks: 0 };
  const token = await sign(baseClaims(), privateKey, "key-1");
  const guard = createWorkloadIdentityGuard({
    config: config(),
    isPublic: (request) => request.path === "/healthz",
    cache: new WorkloadJwksCache(),
    fetchImpl: fetchFor([jwk], counters),
  });

  const publicDecision = await guard.check({ path: "/healthz" });
  assert.deepEqual(publicDecision, { ok: true, identity: null });

  const protectedDecision = await guard.check({
    authorization: `Bearer ${token}`,
    path: "/quote",
  });
  assert.deepEqual(protectedDecision, {
    ok: true,
    identity: { unitId: CALLER_UNIT, serviceAccount: CALLER_SERVICE_ACCOUNT },
  });

  const denied = await guard.check({ path: "/quote" });
  assert.equal(denied.ok === false && denied.status, 401);
});
