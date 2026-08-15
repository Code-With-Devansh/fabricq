import { Resend } from "resend";
import config from "../config/index.js";
import logger from "../config/logger/index.js";

if (!config.mail.resendApiKey) {
  // Not fatal at import time (mirrors how other optional config is
  // handled) - the mail worker process checks this explicitly on boot
  // so a missing key fails fast there instead of on the first send.
  logger.warn("[mail] RESEND_API_KEY is not set");
}

// Lazily constructed: the Resend SDK throws in its own constructor when
// given an undefined key, which would crash this module at import time
// (before mail_worker_process.js's own fatal check ever runs, since ESM
// imports are evaluated before any of that file's code). Deferring
// construction to first use keeps the "warn on import, fatal on boot in
// the worker process" behavior intact.
let _client;
function getClient() {
  if (!_client) {
    _client = new Resend(config.mail.resendApiKey || "missing_resend_api_key");
  }
  return _client;
}

/**
 * Thin wrapper so the worker doesn't touch the Resend SDK response
 * shape directly. Throws on failure so BullMQ's retry/backoff handles
 * transient send errors.
 */
export async function sendEmail({ to, subject, html, text }) {
  console.log("from: ", config.mail.from)
  const { data, error } = await getClient().emails.send({
    from: config.mail.from,
    to,
    subject,
    html,
    text,
  });

  if (error) {
    throw new Error(`[mail] Resend send failed: ${error.message ?? error}`);
  }

  return data;
}
