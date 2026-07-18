import { z } from "zod";

const authConfigSchema = z.discriminatedUnion("auth_type", [
  z.object({ auth_type: z.literal("NONE") }),
  z.object({
    auth_type: z.literal("BEARER"),
    auth_config: z.object({ token: z.string().min(1) }),
  }),
  z.object({
    auth_type: z.literal("BASIC"),
    auth_config: z.object({
      username: z.string().min(1),
      password: z.string().min(1),
    }),
  }),
  z.object({
    auth_type: z.literal("API_KEY"),
    auth_config: z.object({
      key: z.string().min(1),
      value: z.string().min(1),
      in: z.enum(["header", "query"]).optional().default("header"),
    }),
  }),
]);

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

    body_type: z.enum(["json", "form"]).optional().default("json"),

    headers: z.record(z.string(), z.string()).optional().default({}),

    query_params: z.record(z.string(), z.string()).optional().default({}),

    auth_type: z.enum(["NONE", "BEARER", "BASIC", "API_KEY"]).optional().default("NONE"),

    auth_config: z.record(z.string(), z.any()).optional().default({}),

    redirect_mode: z.enum(["follow", "manual", "error"]).optional().default("follow"),

    timeout_ms: z.number().int().min(1).max(120_000).optional().default(30_000),

    schedule_type: z.enum(["ONCE", "CRON"]),

    run_at: z.number().int().positive().optional(),

    cron_expression: z.string().optional(),

    max_attempts: z.number().int().min(1).max(100).optional().default(3),

    backoff_seconds: z.number().int().min(0).optional().default(60),

    enabled: z.boolean().optional().default(true),
  })
  .superRefine((data, ctx) => {
    const authResult = authConfigSchema.safeParse({
      auth_type: data.auth_type,
      auth_config: data.auth_config,
    });
    if (!authResult.success) {
      for (const issue of authResult.error.issues) {
        ctx.addIssue({
          ...issue,
          path: ["auth_config", ...issue.path.filter((p) => p !== "auth_type")],
        });
      }
    }

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
    body_type: z.enum(["json", "form"]).optional(),
    headers: z.record(z.string(), z.string()).optional(),
    query_params: z.record(z.string(), z.string()).optional(),
    auth_type: z.enum(["NONE", "BEARER", "BASIC", "API_KEY"]).optional(),
    auth_config: z.record(z.string(), z.any()).optional(),
    redirect_mode: z.enum(["follow", "manual", "error"]).optional(),
    timeout_ms: z.number().int().min(1).max(120_000).optional(),
    schedule_type: z.enum(["ONCE", "CRON"]).optional(),
    run_at: z.number().int().positive().optional(),
    cron_expression: z.string().optional(),
    max_attempts: z.number().int().min(1).max(100).optional(),
    backoff_seconds: z.number().int().min(0).optional(),
    enabled: z.boolean().optional(),
  })
  .strict()
  .refine((data) => Object.keys(data).length > 0, {
    message: "At least one field must be provided to update.",
  })
  .superRefine((data, ctx) => {
    if (data.auth_type != null && data.auth_type !== "NONE") {
      const authResult = authConfigSchema.safeParse({
        auth_type: data.auth_type,
        auth_config: data.auth_config ?? {},
      });
      if (!authResult.success) {
        for (const issue of authResult.error.issues) {
          ctx.addIssue({
            ...issue,
            path: ["auth_config", ...issue.path.filter((p) => p !== "auth_type")],
          });
        }
      }
    }

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
  // COMPLETED/FAILED reflect the job's most recent execution outcome, not
  // a scheduling state - there's no PENDING/RUNNING column anymore, use
  // `enabled` for pause/active filtering instead.
  status: z.enum(["COMPLETED", "FAILED"]).optional(),
  enabled: z.coerce.boolean().optional(),
  schedule_type: z.enum(["ONCE", "CRON"]).optional(),
  limit: z.coerce.number().int().min(1).max(100).optional().default(20),
  offset: z.coerce.number().int().min(0).optional().default(0),
});

export const paginationQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(100).optional().default(20),
  offset: z.coerce.number().int().min(0).optional().default(0),
});