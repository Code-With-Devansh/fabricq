import { z } from "zod";

export const createJobSchema = z
  .object({
    method: z.enum([
      "GET",
      "POST",
      "PUT",
      "PATCH",
      "DELETE",
    ]),

    url: z.string().url(),

    body: z.record(z.string(), z.any()).optional().default({}),

    headers: z.record(z.string(), z.string()).optional().default({}),

    schedule_type: z.enum(["ONCE", "CRON"]),

    run_at: z.number().int().positive().optional(),

    cron_expression: z.string().optional(),

    max_attempts: z.number().int().min(1).max(100).optional().default(3),

    backoff_seconds: z.number().int().min(0).optional().default(60),
  })
  .superRefine((data, ctx) => {
    if (data.schedule_type === "ONCE") {
      if (data.run_at == null) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["run_at"],
          message: "run_at is required for ONCE jobs.",
        });
      }

      if (data.cron_expression != null) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["cron_expression"],
          message: "cron_expression must not be provided for ONCE jobs.",
        });
      }
    }

    if (data.schedule_type === "CRON") {
      if (!data.cron_expression) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["cron_expression"],
          message: "cron_expression is required for CRON jobs.",
        });
      }

      if (data.run_at != null) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["run_at"],
          message: "run_at must not be provided for CRON jobs.",
        });
      }
    }
  });
// Partial update: every field optional, but reuses the same ONCE/CRON
// consistency rules if the caller is actually changing the schedule.
export const updateJobSchema = z
  .object({
    method: z.enum(["GET", "POST", "PUT", "PATCH", "DELETE"]).optional(),
    url: z.string().url().optional(),
    body: z.record(z.string(), z.any()).optional(),
    headers: z.record(z.string(), z.string()).optional(),
    schedule_type: z.enum(["ONCE", "CRON"]).optional(),
    run_at: z.number().int().positive().optional(),
    cron_expression: z.string().optional(),
    max_attempts: z.number().int().min(1).max(100).optional(),
    backoff_seconds: z.number().int().min(0).optional(),
  })
  .strict()
  .refine((data) => Object.keys(data).length > 0, {
    message: "At least one field must be provided to update.",
  })
  .superRefine((data, ctx) => {
    if (data.schedule_type === "ONCE" && data.cron_expression != null) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["cron_expression"],
        message: "cron_expression must not be provided for ONCE jobs.",
      });
    }
    if (data.schedule_type === "CRON" && data.run_at != null) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["run_at"],
        message: "run_at must not be provided for CRON jobs.",
      });
    }
  });

export const listJobsQuerySchema = z.object({
  status: z.enum(["PENDING", "RUNNING", "RETRYING", "COMPLETED", "FAILED"]).optional(),
  schedule_type: z.enum(["ONCE", "CRON"]).optional(),
  limit: z.coerce.number().int().min(1).max(100).optional().default(20),
  offset: z.coerce.number().int().min(0).optional().default(0),
});

export const paginationQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(100).optional().default(20),
  offset: z.coerce.number().int().min(0).optional().default(0),
});