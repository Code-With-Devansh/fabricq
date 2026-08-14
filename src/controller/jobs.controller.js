import { createJobSchema } from "../validators/http_job.js";
import { asyncHandler } from "../middlewares/asyncHandler.js";
import { AppError } from "../Error/appError.js";
import {
  createJobService,
  getJobsService,
  getJobByIdService,
  updateJobService,
  deleteJobService,
  getJobExecutionHistoryService,
  getExecutionDetailService,
} from "../services/httpJob.js";
import {
  updateJobSchema,
  listJobsQuerySchema,
  paginationQuerySchema,
} from "../validators/http_job.js";

// Jobs are reachable two ways - the dashboard (/teams/:teamId/jobs, JWT +
// role permissions, sets req.team) and the public API (/v1/jobs, API key
// + scopes, sets req.apiKey). Both resolve to the same team_id-scoped
// service calls, so one controller serves both route trees instead of
// duplicating this logic per auth mode.
function resolveTeamId(req) {
  return req.team?.teamId ?? req.apiKey?.teamId;
}

export const uploadJob = asyncHandler(async (req, res) => {
  const validated = createJobSchema.safeParse(req.body);

  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const job = await createJobService(resolveTeamId(req), validated.data);
  return res.status(201).json({ success: true, data: job });
});

export const getJobs = asyncHandler(async (req, res) => {
  const validated = listJobsQuerySchema.safeParse(req.query);
  if (!validated.success) {
    throw new AppError("Invalid query parameters", 400, validated.error.flatten());
  }

  const { status, enabled, schedule_type, limit, offset } = validated.data;
  const { jobs, total } = await getJobsService(resolveTeamId(req), {
    status,
    enabled,
    schedule_type,
    limit,
    offset,
  });

  return res.status(200).json({
    success: true,
    data: jobs,
    pagination: { total, limit, offset },
  });
});

export const getJob = asyncHandler(async (req, res) => {
  const job = await getJobByIdService(resolveTeamId(req), req.params.jobId);
  return res.status(200).json({ success: true, data: job });
});

export const updateJob = asyncHandler(async (req, res) => {
  const validated = updateJobSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const job = await updateJobService(
    resolveTeamId(req),
    req.params.jobId,
    validated.data
  );
  return res.status(200).json({ success: true, data: job });
});

export const deleteJob = asyncHandler(async (req, res) => {
  await deleteJobService(resolveTeamId(req), req.params.jobId);
  return res.status(204).send();
});

export const getJobExecutions = asyncHandler(async (req, res) => {
  const validated = paginationQuerySchema.safeParse(req.query);
  if (!validated.success) {
    throw new AppError("Invalid query parameters", 400, validated.error.flatten());
  }

  const { limit, offset } = validated.data;
  const { executions, total } = await getJobExecutionHistoryService(
    resolveTeamId(req),
    req.params.jobId,
    { limit, offset }
  );

  return res.status(200).json({
    success: true,
    data: executions,
    pagination: { total, limit, offset },
  });
});

export const getExecutionDetail = asyncHandler(async (req, res) => {
  const execution = await getExecutionDetailService(
    resolveTeamId(req),
    req.params.executionId
  );
  return res.status(200).json({ success: true, data: execution });
});
