// Opaque keyset-pagination cursor. Encodes the (created_at, execution_id)
// tuple of the last row on a page so the next query can resume with
// `(created_at, execution_id) < / > (...)` instead of OFFSET.
//
// Base64-encoded JSON rather than a raw string so the shape can grow
// later without breaking already-issued cursors, and so it's opaque to
// API callers (they shouldn't construct or parse it themselves).

export function encodeCursor({ created_at, execution_id }) {
  const payload = JSON.stringify({
    created_at: new Date(created_at).toISOString(),
    execution_id,
  });
  return Buffer.from(payload, "utf8").toString("base64url");
}

export function decodeCursor(cursor) {
  let payload;
  try {
    payload = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
  } catch {
    throw new Error("Invalid cursor");
  }

  if (!payload?.created_at || !payload?.execution_id) {
    throw new Error("Invalid cursor");
  }

  return payload;
}