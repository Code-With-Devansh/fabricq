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