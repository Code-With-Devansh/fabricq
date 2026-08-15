import { Queue } from "bullmq";
import { createBullConnection } from "../config/bullRedis.js";

export const MAIL_QUEUE_NAME = "email";

// Job names - keep them stable, the worker switches on these.
export const MAIL_JOB = {
  TEAM_INVITATION: "team_invitation",
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
