import { test } from "node:test";
import assert from "node:assert/strict";
import { generateKeyPair, SignJWT, type JWTVerifyGetKey } from "jose";
import { authenticateWorkload, callersOfProjection } from "../src/index.js";

const issuer = "https://cluster.example/oidc";
const audience = "payments/charge_svc";
const now = new Date("2026-01-01T00:00:00Z");
const { privateKey, publicKey } = await generateKeyPair("RS256");
const resolveKey: JWTVerifyGetKey = async () => publicKey;

async function token(subject: string, overrides: { issuer?: string; audience?: string; expired?: boolean } = {}) {
  const builder = new SignJWT({})
    .setProtectedHeader({ alg: "RS256", kid: "test-key" })
    .setIssuer(overrides.issuer ?? issuer)
    .setAudience(overrides.audience ?? audience)
    .setSubject(subject)
    .setIssuedAt(now)
    .setExpirationTime(overrides.expired ? Math.floor(now.getTime() / 1000) - 1 : "5m");
  return builder.sign(privateKey);
}

const options = {
  trustedIssuer: issuer,
  audience,
  callers: "checkout/checkout_svc=sol:checkout",
  resolveKey,
  currentDate: now,
};

test("caller projection maps service accounts to Sol units", () => {
  assert.deepEqual(
    [...callersOfProjection("checkout/checkout_svc=sol:checkout,broken, =ignored")],
    [["sol:checkout", "checkout/checkout_svc"]],
  );
});

test("valid workload identity returns its authorized Sol principal", async () => {
  assert.deepEqual(
    await authenticateWorkload(`Bearer ${await token("system:serviceaccount:sol:checkout")}`, options),
    { unit: "checkout/checkout_svc", serviceAccount: "sol:checkout" },
  );
});

test("missing or invalid token, issuer, audience, and expiry fail authentication", async () => {
  assert.equal((await authenticateWorkload(undefined, options)).status, 401);
  assert.equal((await authenticateWorkload("Basic abc", options)).status, 401);
  assert.equal(
    (await authenticateWorkload(`Bearer ${await token("system:serviceaccount:sol:checkout", { issuer: "https://wrong" })}`, options)).status,
    401,
  );
  assert.equal(
    (await authenticateWorkload(`Bearer ${await token("system:serviceaccount:sol:checkout", { audience: "other" })}`, options)).status,
    401,
  );
  assert.equal(
    (await authenticateWorkload(`Bearer ${await token("system:serviceaccount:sol:checkout", { expired: true })}`, options)).status,
    401,
  );
});

test("authenticated non-workload and undeclared callers are forbidden", async () => {
  assert.equal((await authenticateWorkload(`Bearer ${await token("user:alice")}`, options)).status, 403);
  assert.equal(
    (await authenticateWorkload(`Bearer ${await token("system:serviceaccount:sol:unknown")}`, options)).status,
    403,
  );
});

test("key resolver outages are server errors, not authentication denials", async () => {
  const unavailable: JWTVerifyGetKey = async () => {
    throw new Error("issuer discovery unavailable");
  };
  const result = await authenticateWorkload(
    `Bearer ${await token("system:serviceaccount:sol:checkout")}`,
    { ...options, resolveKey: unavailable },
  );
  assert.equal("status" in result ? result.status : undefined, 500);
});
