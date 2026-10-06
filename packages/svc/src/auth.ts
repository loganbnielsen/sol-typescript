import { jwtVerify, type JWTVerifyGetKey } from "jose";

export interface WorkloadPrincipal {
  readonly unit: string;
  readonly serviceAccount: string;
}

export type WorkloadAuthError =
  | { readonly status: 401; readonly message: string }
  | { readonly status: 403; readonly message: string }
  | { readonly status: 500; readonly message: string };

export interface WorkloadAuthOptions {
  /** Sol-projected target capability; never derive trust from the token. */
  readonly trustedIssuer: string;
  /** The receiving Sol unit, used as the token audience. */
  readonly audience: string;
  /** `SOL_CALLED_BY` projection: `unit=namespace:serviceaccount,...`. */
  readonly callers: string;
  /** Adapter-owned discovery/JWKS lookup and runtime-specific cache. */
  readonly resolveKey: JWTVerifyGetKey;
  /** Test seam; defaults to the system clock. */
  readonly currentDate?: Date;
}

export type WorkloadAuthenticator = (
  authorization: string | undefined,
) => Promise<WorkloadPrincipal | WorkloadAuthError>;

class KeyResolutionError extends Error {
  constructor(cause: unknown) {
    super("Unable to resolve workload signing key", { cause });
  }
}

export function callersOfProjection(raw: string): Map<string, string> {
  const callers = new Map<string, string>();
  for (const entry of raw.split(",")) {
    const separator = entry.indexOf("=");
    if (separator < 1) continue;
    const unit = entry.slice(0, separator).trim();
    const serviceAccount = entry.slice(separator + 1).trim();
    if (unit && serviceAccount && !callers.has(serviceAccount)) callers.set(serviceAccount, unit);
  }
  return callers;
}

/** Build an authenticator from Sol's projected workload contract and runtime key resolver. */
export function createWorkloadAuthenticator(resolveKey: JWTVerifyGetKey): WorkloadAuthenticator {
  const trustedIssuer = process.env.SOL_TRUSTED_WORKLOAD_ISSUER;
  const audience = process.env.SOL_UNIT;
  if (!trustedIssuer || !audience) {
    throw new Error("Sol workload authentication requires projected SOL_TRUSTED_WORKLOAD_ISSUER and SOL_UNIT");
  }
  const options: WorkloadAuthOptions = {
    trustedIssuer,
    audience,
    callers: process.env.SOL_CALLED_BY ?? "",
    resolveKey,
  };
  return (authorization) => authenticateWorkload(authorization, options);
}

/** Verify Sol workload identity and authorize its ServiceAccount against calls-derived policy. */
export async function authenticateWorkload(
  authorization: string | undefined,
  options: WorkloadAuthOptions,
): Promise<WorkloadPrincipal | WorkloadAuthError> {
  const match = authorization?.match(/^Bearer (.+)$/);
  if (!match) return { status: 401, message: "Missing or invalid Bearer authorization" };

  let payload;
  try {
    const resolveKey: JWTVerifyGetKey = async (header, token) => {
      try {
        return await options.resolveKey(header, token);
      } catch (error) {
        throw new KeyResolutionError(error);
      }
    };
    ({ payload } = await jwtVerify(match[1], resolveKey, {
      issuer: options.trustedIssuer,
      audience: options.audience,
      algorithms: ["RS256", "ES256", "ES384", "ES512"],
      currentDate: options.currentDate,
    }));
  } catch (error) {
    return error instanceof KeyResolutionError
      ? { status: 500, message: error.message }
      : { status: 401, message: "Invalid workload identity token" };
  }

  const subject = payload.sub;
  const prefix = "system:serviceaccount:";
  if (typeof subject !== "string" || !subject.startsWith(prefix)) {
    return { status: 403, message: "Authenticated subject is not a workload identity" };
  }
  const serviceAccount = subject.slice(prefix.length);
  const unit = callersOfProjection(options.callers).get(serviceAccount);
  return unit
    ? { unit, serviceAccount }
    : { status: 403, message: "Authenticated caller is not declared by the calls graph" };
}
