import { Queue } from "bullmq";
import { createBullConnection } from "../config/bullRedis.js";

export const MAIL_QUEUE_NAME = "email";

// Job names - keep them stable, the worker switches on these.
export const MAIL_JOB = {
  TEAM_INVITATION: "team_invitation",
  EMAIL_VERIFICATION: "email_verification",
  PASSWORD_RESET: "password_reset",
  TWO_FACTOR_CODE: "two_factor_code",
};

const connection = createBullConnection("mail-queue");

export const mailQueue = new Queue(MAIL_QUEUE_NAME, {
  connection,
  defaultJobOptions: {
    attempts: 5,
    backoff: { type: "exponential", delay: 5_000 },
    // Emails are small and short-lived as job records; keep the queue
    // tidy without needing a separate cleanup process.
    removeOnComplete: { age: 60 * 60 * 24, count: 1000 },
    removeOnFail: { age: 60 * 60 * 24 * 7 },
  },
});

/**
 * Enqueues a team-invitation email. jobId is deterministic per
 * invitation so retrying createInvitationService (e.g. a re-invite that
 * replaces the pending row) can't silently double-enqueue a send for
 * the same invitation id.
 */
export async function enqueueInvitationEmail({
  invitationId,
  teamName,
  inviterEmail,
  roleName,
  toEmail,
  acceptUrl,
  expiresAt,
}) {
  await mailQueue.add(
    MAIL_JOB.TEAM_INVITATION,
    {
      teamName,
      inviterEmail,
      roleName,
      toEmail,
      acceptUrl,
      expiresAt,
    },
    { jobId: `invitation-${invitationId}` }
  );
}

/**
 * Enqueues an email-verification link email. jobId keyed on the token
 * id (not user id) so a resend after an old token was invalidated
 * always gets its own job rather than colliding with a prior one.
 */
export async function enqueueVerificationEmail({
  tokenId,
  toEmail,
  verifyUrl,
  expiresAt,
}) {
  await mailQueue.add(
    MAIL_JOB.EMAIL_VERIFICATION,
    { toEmail, verifyUrl, expiresAt },
    { jobId: `email-verify-${tokenId}` }
  );
}

/**
 * Enqueues a password-reset link email.
 */
export async function enqueuePasswordResetEmail({
  tokenId,
  toEmail,
  resetUrl,
  expiresAt,
}) {
  await mailQueue.add(
    MAIL_JOB.PASSWORD_RESET,
    { toEmail, resetUrl, expiresAt },
    { jobId: `password-reset-${tokenId}` }
  );
}

/**
 * Enqueues a 2FA one-time-code email. No retry-triggered duplicate risk
 * here beyond the jobId dedupe, since a fresh challenge always gets a
 * fresh id (old ones are invalidated first - see auth.service.js).
 */
export async function enqueueTwoFactorCodeEmail({
  challengeId,
  toEmail,
  code,
  expiresAt,
}) {
  await mailQueue.add(
    MAIL_JOB.TWO_FACTOR_CODE,
    { toEmail, code, expiresAt },
    { jobId: `two-factor-${challengeId}` }
  );
}
