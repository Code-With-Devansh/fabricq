import { createJobService } from "../services/httpJob.js";
import { createJobSchema } from "../validators/http_job.js";
import { asyncHandler } from "../middlewares/asyncHandler.js";
import {AppError} from "../Error/appError.js"
import {
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
export const uploadJob = asyncHandler(async (req, res) => {
  const payload = req.body;
  const validated = createJobSchema.safeParse(payload);

  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const job = await createJobService(validated.data);
  return res.status(201).json({ success: true, data: job });
});

export const getJobs = asyncHandler(async (req, res) => {
  const validated = listJobsQuerySchema.safeParse(req.query);
  if (!validated.success) {
    throw new AppError("Invalid query parameters", 400, validated.error.flatten());
  }

  const { status, enabled, schedule_type, limit, offset } = validated.data;
  const { jobs, total } = await getJobsService({ status, enabled, schedule_type, limit, offset });

  return res.status(200).json({
    success: true,
    data: jobs,
    pagination: { total, limit, offset },
  });
});

export const getJob = asyncHandler(async (req, res) => {
  const job = await getJobByIdService(req.params.jobId);
  return res.status(200).json({ success: true, data: job });
});

export const updateJob = asyncHandler(async (req, res) => {
  const validated = updateJobSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const job = await updateJobService(req.params.jobId, validated.data);
  return res.status(200).json({ success: true, data: job });
});

export const deleteJob = asyncHandler(async (req, res) => {
  await deleteJobService(req.params.jobId);
  return res.status(204).send();
});

export const getJobExecutions = asyncHandler(async (req, res) => {
  const validated = paginationQuerySchema.safeParse(req.query);
  if (!validated.success) {
    throw new AppError("Invalid query parameters", 400, validated.error.flatten());
  }

  const { limit, offset } = validated.data;
  const { executions, total } = await getJobExecutionHistoryService(req.params.jobId, {
    limit,
    offset,
  });

  return res.status(200).json({
    success: true,
    data: executions,
    pagination: { total, limit, offset },
  });
});

export const getExecutionDetail = asyncHandler(async (req, res) => {
  const execution = await getExecutionDetailService(req.params.executionId);
  return res.status(200).json({ success: true, data: execution });
});