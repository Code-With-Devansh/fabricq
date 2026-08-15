import { Worker } from "bullmq";
import logger from "../config/logger/index.js";
import { createBullConnection } from "../config/bullRedis.js";
import { MAIL_QUEUE_NAME, MAIL_JOB } from "../queues/mail.queue.js";
import { sendEmail } from "./mailer.js";
import { teamInvitationTemplate } from "./templates.js";

async function handleJob(job) {
  switch (job.name) {
    case MAIL_JOB.TEAM_INVITATION: {
      const { toEmail, teamName, inviterEmail, roleName, acceptUrl, expiresAt } =
        job.data;
      const { subject, html, text } = teamInvitationTemplate({
        teamName,
        inviterEmail,
        roleName,
        acceptUrl,
        expiresAt,
      });
      await sendEmail({ to: toEmail, subject, html, text });
      return;
    }
    default:
      // Unknown job name: log and drop rather than retrying forever on
      // something this worker will never know how to handle.
      logger.warn({ jobName: job.name }, "[mail-worker] unknown job name, skipping");
  }
}

export function startMailWorker() {
  const connection = createBullConnection("mail-worker");

  const worker = new Worker(MAIL_QUEUE_NAME, handleJob, {
    connection,
    concurrency: 5,
  });

  worker.on("completed", (job) => {
    logger.info({ jobId: job.id, jobName: job.name }, "[mail-worker] sent");
  });

  worker.on("failed", (job, err) => {
    logger.error(
      { jobId: job?.id, jobName: job?.name, attemptsMade: job?.attemptsMade, err },
      "[mail-worker] send failed"
    );
  });

  return worker;
}
