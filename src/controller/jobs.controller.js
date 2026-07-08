import { createJobService } from "../services/httpJob.js";
import { createJobSchema } from "../validators/http_job.js";
import { asyncHandler } from "../middlewares/asyncHandler.js";
import {AppError} from "../Error/appError.js"
export const uploadJob = asyncHandler(async (req, res) => {
  const payload = req.body;
  const validated = createJobSchema.safeParse(payload);

  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const job = await createJobService(validated.data);
  return res.status(201).json({ success: true, data: job });
});