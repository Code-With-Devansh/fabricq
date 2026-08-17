import { CronExpressionParser } from "cron-parser";
import {
  createJob,
  findJobByIdForTeam,
  listJobsForTeam,
  updateJobForTeam,
  deleteJobForTeam,
} from "../repositories/httpJob.repository.js";
import {
  getExecutionHistory,
  getExecutionByIdForTeam,
} from "../repositories/execution.repository.js";
import { AppError } from "../Error/appError.js";

export const createJobService = async (teamId, data) => {
  let next_run = null;
  if (data.schedule_type === "ONCE") {
    next_run = Math.floor(new Date(data.run_at).getTime() / 1000);
  } else {
    next_run = Math.floor(
      CronExpressionParser.parse(data.cron_expression).next().getTime() / 1000
    );
  }
  return createJob({
    ...data,
    team_id: teamId,
    next_run,
  });
};

export const getJobsService = async (
  teamId,
  { status, enabled, schedule_type, limit, offset }
) => {
  return listJobsForTeam({
    teamId,
    status,
    enabled,
    scheduleType: schedule_type,
    limit,
    offset,
  });
};

export const getJobByIdService = async (teamId, jobId) => {
  const job = await findJobByIdForTeam(teamId, jobId);
  if (!job) throw new AppError("Job not found", 404);
  return job;
};

export const updateJobService = async (teamId, jobId, updates) => {
  const existing = await findJobByIdForTeam(teamId, jobId);
  if (!existing) throw new AppError("Job not found", 404);

  const merged = { ...existing, ...updates };

  // Recompute next_run if anything schedule-related changed, so the
  // scheduler doesn't keep polling against a stale timestamp.
  const scheduleChanged =
    "run_at" in updates || "cron_expression" in updates || "schedule_type" in updates;

  if (scheduleChanged) {
    if (merged.schedule_type === "ONCE") {
      updates.next_run = Math.floor(new Date(merged.run_at).getTime() / 1000);
    } else {
      updates.next_run = Math.floor(
        CronExpressionParser.parse(merged.cron_expression).next().getTime() / 1000
      );
    }
  }

  const updated = await updateJobForTeam(teamId, jobId, updates);
  if (!updated) throw new AppError("Job not found", 404);
  return updated;
};

export const deleteJobService = async (teamId, jobId) => {
  const deleted = await deleteJobForTeam(teamId, jobId);
  if (!deleted) throw new AppError("Job not found", 404);
  return deleted;
};

export const getJobExecutionHistoryService = async (
  teamId,
  jobId,
  { limit, offset }
) => {
  const job = await findJobByIdForTeam(teamId, jobId);
  if (!job) throw new AppError("Job not found", 404);
  return getExecutionHistory(jobId, { limit, offset });
};

export const getExecutionDetailService = async (teamId, executionId) => {
  const execution = await getExecutionByIdForTeam(teamId, executionId);
  if (!execution) throw new AppError("Execution not found", 404);
  return execution;
};
