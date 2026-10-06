import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { declaredPeer, peerHeaders, peerUrl } from "../src/index.js";

const peer = declaredPeer("checkout/checkout_svc", "checkout_svc");

async function withEnv(values: Record<string, string | undefined>, run: () => Promise<void>) {
  const old = Object.fromEntries(Object.keys(values).map((key) => [key, process.env[key]]));
  for (const [key, value] of Object.entries(values)) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  try {
    await run();
  } finally {
    for (const [key, value] of Object.entries(old)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

test("peerUrl reads the declared service URL and rejects invalid values", async () => {
  await withEnv({ CHECKOUT_SVC_URL: "https://checkout.example" }, async () => {
    assert.equal(peerUrl(peer).href, "https://checkout.example/");
    process.env.CHECKOUT_SVC_URL = "file:///tmp/checkout";
    assert.throws(() => peerUrl(peer), /absolute http\(s\) URL/);
  });
});

test("peerHeaders reads the projected token each time and preserves other headers", async () => {
  const dir = await mkdtemp(join(tmpdir(), "sol-peer-"));
  const tokenFile = join(dir, "token");
  try {
    await withEnv({
      CHECKOUT_SVC_TOKEN_FILE: tokenFile,
      SOL_ALLOW_PLAINTEXT_PEER_AUTH: "1",
      SOL_API_KEY: "shared",
    }, async () => {
      await writeFile(tokenFile, "first-token\n");
      const first = await peerHeaders(peer, { headers: { accept: "application/json" }, traceparent: "00-trace" });
      assert.equal(first.get("authorization"), "Bearer first-token");
      assert.equal(first.get("x-api-key"), null);
      assert.equal(first.get("accept"), "application/json");
      assert.equal(first.get("traceparent"), "00-trace");

      await writeFile(tokenFile, "rotated-token\n");
      assert.equal((await peerHeaders(peer)).get("authorization"), "Bearer rotated-token");
    });
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("peerHeaders fails closed on an unreadable projection instead of using the local key", async () => {
  await withEnv({
    CHECKOUT_SVC_TOKEN_FILE: "/missing/sol/token",
    SOL_ALLOW_PLAINTEXT_PEER_AUTH: "1",
    SOL_API_KEY: "shared",
  }, async () => {
    await assert.rejects(() => peerHeaders(peer), /could not read CHECKOUT_SVC_TOKEN_FILE/);
  });
});

test("peerHeaders uses the API key only for explicit local opt-in without a projected identity", async () => {
  await withEnv({
    CHECKOUT_SVC_TOKEN_FILE: undefined,
    SOL_ALLOW_PLAINTEXT_PEER_AUTH: "1",
    SOL_API_KEY: "shared",
  }, async () => {
    const headers = await peerHeaders(peer);
    assert.equal(headers.get("x-api-key"), "shared");
    assert.equal(headers.get("authorization"), null);
  });
});
