import { CronExpressionParser } from "cron-parser";
import { createJob } from "../repositories/httpJob.repository.js";
export const createJobService = async(data)=>{
    let next_run = null;
    if(data.schedule_type === 'ONCE'){
        next_run = Math.floor(new Date(data.run_at).getTime() / 1000);
    }else{
        next_run = Math.floor(
    CronExpressionParser.parse(data.cron_expression)
      .next()
      .getTime() / 1000
  );
    }
    data.next_run = next_run;
    data.attempts = 0;
    data.status = "PENDING"
    return await createJob(data);
}
import { CronExpressionParser as CronParser2 } from "cron-parser";
import {
  findJobById,
  listJobs,
  updateJob,
  deleteJob,
} from "../repositories/httpJob.repository.js";
import {
  getExecutionHistory,
  getExecutionById,
} from "../repositories/execution.repository.js";
import { AppError } from "../Error/appError.js";

export const getJobsService = async ({ status, schedule_type, limit, offset }) => {
  return listJobs({ status, scheduleType: schedule_type, limit, offset });
};

export const getJobByIdService = async (jobId) => {
  const job = await findJobById(jobId);
  if (!job) throw new AppError("Job not found", 404);
  return job;
};

export const updateJobService = async (jobId, updates) => {
  const existing = await findJobById(jobId);
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
        CronParser2.parse(merged.cron_expression).next().getTime() / 1000
      );
    }
  }

  const updated = await updateJob(jobId, updates);
  if (!updated) throw new AppError("Job not found", 404);
  return updated;
};

export const deleteJobService = async (jobId) => {
  const deleted = await deleteJob(jobId);
  if (!deleted) throw new AppError("Job not found", 404);
  return deleted;
};

export const getJobExecutionHistoryService = async (jobId, { limit, offset }) => {
  const job = await findJobById(jobId);
  if (!job) throw new AppError("Job not found", 404);
  return getExecutionHistory(jobId, { limit, offset });
};

export const getExecutionDetailService = async (executionId) => {
  const execution = await getExecutionById(executionId);
  if (!execution) throw new AppError("Execution not found", 404);
  return execution;
};