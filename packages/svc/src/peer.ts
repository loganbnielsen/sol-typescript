import { readFile } from "node:fs/promises";

const peerBrand: unique symbol = Symbol("Sol peer");

export interface Peer {
  readonly [peerBrand]: true;
  readonly unitId: string;
  readonly serviceName: string;
}

/** Used by generated application bindings. */
export function declaredPeer(unitId: string, serviceName: string): Peer {
  if (!unitId.includes("/") || !serviceName) {
    throw new Error("Sol peer requires a domain/service unit and service name");
  }
  return Object.freeze({ unitId, serviceName }) as Peer;
}

function envName(serviceName: string): string {
  return serviceName.replace(/[^a-zA-Z0-9]/g, "_").toUpperCase();
}

export function peerUrl(peer: Peer): URL {
  const key = `${envName(peer.serviceName)}_URL`;
  const value = process.env[key];
  if (!value) throw new Error(`${key} is not set`);

  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${key} must be an absolute http(s) URL`);
  }
  if ((url.protocol !== "http:" && url.protocol !== "https:") || !url.host) {
    throw new Error(`${key} must be an absolute http(s) URL`);
  }
  return url;
}

export interface PeerHeadersOptions {
  headers?: HeadersInit;
  traceparent?: string;
}

async function readSecretFile(key: string, path: string): Promise<string> {
  let content: string;
  try {
    content = await readFile(path, "utf8");
  } catch (error) {
    throw new Error(`could not read ${key} (${path}): ${String(error)}`);
  }
  const value = content.trim();
  if (!value) throw new Error(`${key} file is empty: ${path}`);
  return value;
}

/** Read the projected token on every call so Kubernetes token rotation is observed. */
export async function peerHeaders(
  peer: Peer,
  options: PeerHeadersOptions = {},
): Promise<Headers> {
  const name = envName(peer.serviceName);
  const tokenPath = process.env[`${name}_TOKEN_FILE`];
  const headers = new Headers(options.headers);

  if (tokenPath !== undefined) {
    headers.set("authorization", `Bearer ${await readSecretFile(`${name}_TOKEN_FILE`, tokenPath)}`);
  } else if (process.env.SOL_ALLOW_PLAINTEXT_PEER_AUTH === "1") {
    const keyPath = process.env.SOL_API_KEY_FILE;
    const apiKey = keyPath !== undefined
      ? await readSecretFile("SOL_API_KEY_FILE", keyPath)
      : process.env.SOL_API_KEY;
    if (!apiKey) throw new Error("SOL_API_KEY/SOL_API_KEY_FILE is not set");
    headers.set("x-api-key", apiKey);
  } else {
    throw new Error(
      `${name}_TOKEN_FILE is not set, so this unit has no projected identity for ${peer.unitId}. ` +
      "A deployed Sol-to-Sol call uses the projected ServiceAccount token; set " +
      "SOL_ALLOW_PLAINTEXT_PEER_AUTH=1 only for local development.",
    );
  }

  if (options.traceparent) headers.set("traceparent", options.traceparent);
  return headers;
}
