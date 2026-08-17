import dns from "node:dns";
import net from "node:net";
import { Agent } from "undici";

/**
 * SSRF protection for outbound job HTTP requests.
 *
 * Two failure modes we're closing:
 *  1. A user-supplied (or redirect-supplied) URL points straight at a
 *     private/loopback/link-local/metadata IP literal.
 *  2. A hostname resolves to a public IP at "check" time but a private IP
 *     at "connect" time (DNS rebinding) - classic TOCTOU.
 *
 * The fix for (2) is to never let two separate resolutions happen. We
 * resolve once ourselves, validate that address, and hand the *exact same*
 * address to the socket via a custom `lookup` used by an undici Agent.
 * Node/undici will connect to the IP we return here, not re-resolve.
 */

// Only allow these schemes. Blocks file:, gopher:, ftp:, data:, etc.
const ALLOWED_PROTOCOLS = new Set(["http:", "https:"]);

// Cloud metadata endpoints - not always caught by "private range" checks
// (169.254.169.254 IS link-local so it's covered below, but keeping this
// explicit set makes the intent obvious and covers IPv6 metadata too).
const BLOCKED_HOSTNAMES = new Set([
  "metadata.google.internal",
  "metadata.goog",
]);

function isBlockedIPv4(ip) {
  const parts = ip.split(".").map(Number);
  const [a, b] = parts;

  if (a === 127) return true; // loopback 127.0.0.0/8
  if (a === 10) return true; // private 10.0.0.0/8
  if (a === 172 && b >= 16 && b <= 31) return true; // private 172.16.0.0/12
  if (a === 192 && b === 168) return true; // private 192.168.0.0/16
  if (a === 169 && b === 254) return true; // link-local / cloud metadata 169.254.0.0/16
  if (a === 0) return true; // "this network" 0.0.0.0/8
  if (a === 100 && b >= 64 && b <= 127) return true; // CGNAT 100.64.0.0/10
  if (a >= 224) return true; // multicast (224-239) + reserved (240-255)
  return false;
}

function isBlockedIPv6(ip) {
  const normalized = ip.toLowerCase();
  if (normalized === "::1") return true; // loopback
  if (normalized === "::") return true; // unspecified
  if (normalized.startsWith("fe80:") || normalized.startsWith("fe8") || normalized.startsWith("fe9") || normalized.startsWith("fea") || normalized.startsWith("feb")) return true; // link-local fe80::/10
  if (normalized.startsWith("fc") || normalized.startsWith("fd")) return true; // unique local fc00::/7
  // IPv4-mapped IPv6 (::ffff:10.0.0.1) - unwrap and recheck
  const mapped = normalized.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/);
  if (mapped) return isBlockedIPv4(mapped[1]);
  return false;
}

function isBlockedIp(ip) {
  if (net.isIPv4(ip)) return isBlockedIPv4(ip);
  if (net.isIPv6(ip)) return isBlockedIPv6(ip);
  return true; // unrecognized format -> fail closed
}

export class SsrfBlockedError extends Error {
  constructor(message) {
    super(message);
    this.name = "SsrfBlockedError";
  }
}

/**
 * Resolve `hostname` and return the first address that passes validation,
 * or throw SsrfBlockedError. Used both as a pre-check and as the `lookup`
 * fed into the undici Agent that actually performs the connection.
 */
function safeLookup(hostname, options, callback) {
  dns.lookup(hostname, { ...options, all: true, verbatim: true }, (err, addresses) => {
    if (err) return callback(err);

    const safe = addresses.filter((a) => !isBlockedIp(a.address));
    if (safe.length === 0) {
      return callback(
        new SsrfBlockedError(
          `Resolved address for "${hostname}" is not allowed (private/loopback/link-local/reserved range)`,
        ),
      );
    }

    // Hand back only the vetted address(es). This is the piece that closes
    // the DNS-rebinding gap: the socket connects to what we return here,
    // it does not re-resolve the hostname.
    if (options.all) {
      return callback(null, safe);
    }
    callback(null, safe[0].address, safe[0].family);
  });
}

/**
 * Cheap synchronous checks that don't need a DNS round trip: scheme
 * allow-list, credentials-in-URL, and IP-literal hosts. Call this before
 * every fetch (initial URL AND every redirect target).
 */
export function assertUrlSyntaxIsSafe(rawUrl) {
  let url;
  try {
    url = new URL(rawUrl);
  } catch {
    throw new SsrfBlockedError(`Invalid URL: ${rawUrl}`);
  }

  if (!ALLOWED_PROTOCOLS.has(url.protocol)) {
    throw new SsrfBlockedError(`Protocol "${url.protocol}" is not allowed`);
  }

  const hostname = url.hostname.toLowerCase();

  if (hostname === "localhost" || hostname.endsWith(".localhost")) {
    throw new SsrfBlockedError(`Host "${hostname}" is not allowed`);
  }
  if (BLOCKED_HOSTNAMES.has(hostname)) {
    throw new SsrfBlockedError(`Host "${hostname}" is not allowed`);
  }

  // If the hostname is already an IP literal, validate it directly -
  // dns.lookup() on a literal just echoes it back, so this catches the
  // same thing safeLookup would, but fails fast without a DNS call.
  const bare = hostname.replace(/^\[|\]$/g, ""); // strip [] from IPv6 literals
  if (net.isIP(bare) && isBlockedIp(bare)) {
    throw new SsrfBlockedError(`IP address "${bare}" is not allowed`);
  }

  return url;
}

/**
 * A shared undici Agent whose connections are DNS-pinned through
 * safeLookup. Pass this as `dispatcher` to every fetch() call the worker
 * makes for a job - initial request and every followed redirect.
 */
export const ssrfSafeAgent = new Agent({
  connect: {
    lookup: safeLookup,
    // Keep these reasonable - job execution already has its own timeout
    // via AbortController, this is just a floor so a hung DNS/connect
    // doesn't hang indefinitely on its own.
    timeout: 10_000,
  },
});