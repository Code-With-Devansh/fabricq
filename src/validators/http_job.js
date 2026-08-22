import { z } from "zod";
import { redirectPolicySchema } from "./redirect_policy.js";

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
    method: z.enum(["GET", "POST", "PUT", "PATCH", "DELETE"]),

    url: z.string().url(),

    body: z.record(z.string(), z.any()).optional().default({}),

    body_type: z.enum(["json", "form"]).optional().default("json"),

    headers: z.record(z.string(), z.string()).optional().default({}),

    query_params: z.record(z.string(), z.string()).optional().default({}),

    auth_type: z
      .enum(["NONE", "BEARER", "BASIC", "API_KEY"])
      .optional()
      .default("NONE"),

    auth_config: z.record(z.string(), z.any()).optional().default({}),

    redirect_mode: z
      .enum(["follow", "manual", "error"])
      .optional()
      .default("follow"),

    redirect_policy: redirectPolicySchema.partial().optional(),

    timeout_ms: z.number().int().min(1).max(120_000).optional().default(30_000),

    schedule_type: z.enum(["ONCE", "CRON"]),

    run_at: z.number().int().positive().optional(),

    cron_expression: z.string().optional(),

    max_attempts: z.number().int().min(1).max(100).optional().default(3),

    backoff_seconds: z.number().int().min(0).optional().default(60),

    retry_strategy: z
      .enum([
        "IMMEDIATE",
        "FIXED",
        "LINEAR",
        "EXPONENTIAL",
        "EXPONENTIAL_JITTER",
        "FIBONACCI",
      ])
      .optional()
      .default("FIXED"),

    retry_multiplier: z.number().positive().optional().default(2),

    // Nullable = no ceiling (applies to every retry_strategy, and to
    // Retry-After honoring below). Defaults to a bounded value; pass null
    // explicitly to opt a job out of capping.
    retry_max_seconds: z.number().int().min(0).nullable().optional().default(3600),

    // When true, a retryable failure whose response carries a Retry-After
    // header uses that header's delay for the next attempt instead of the
    // configured retry_strategy - for that one attempt only. Falls back to
    // retry_strategy if the header is absent, unparseable, or resolves to a
    // non-positive delay. Still bounded by retry_max_seconds when set.
    honor_retry_after: z.boolean().optional().default(false),

    // See migration 023. backfill_on_missed_run only makes sense for CRON
    // jobs - a ONCE job has exactly one due tick by definition, there's
    // nothing to "fall behind" on. max_catchup_per_poll is meaningless
    // without backfill_on_missed_run=true (skip-ahead only ever creates
    // one execution regardless), so it's rejected unless backfill is on -
    // better to error loudly than silently ignore a value the caller set.
    backfill_on_missed_run: z.boolean().optional().default(false),

    max_catchup_per_poll: z.number().int().min(1).max(100).optional(),

    enabled: z.boolean().optional().default(true),
  })
  .superRefine((data, ctx) => {
    if (data.backfill_on_missed_run && data.schedule_type === "ONCE") {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["backfill_on_missed_run"],
        message: "backfill_on_missed_run only applies to CRON jobs.",
      });
    }

    if (data.max_catchup_per_poll != null && !data.backfill_on_missed_run) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["max_catchup_per_poll"],
        message: "max_catchup_per_poll requires backfill_on_missed_run to be true.",
      });
    }
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
    retry_strategy: z
      .enum([
        "IMMEDIATE",
        "FIXED",
        "LINEAR",
        "EXPONENTIAL",
        "EXPONENTIAL_JITTER",
        "FIBONACCI",
      ])
      .optional(),
    retry_multiplier: z.number().positive().optional(),
    // .nullable() lets a PATCH explicitly clear the cap (send null);
    // omitting the field entirely still means "don't change it".
    retry_max_seconds: z.number().int().min(0).nullable().optional(),
    honor_retry_after: z.boolean().optional(),
    backfill_on_missed_run: z.boolean().optional(),
    max_catchup_per_poll: z.number().int().min(1).max(100).optional(),
    enabled: z.boolean().optional(),
  })
  .strict()
  .refine((data) => Object.keys(data).length > 0, {
    message: "At least one field must be provided to update.",
  })
  .superRefine((data, ctx) => {
    // Both checks below only fire on values present IN THIS PARTIAL
    // UPDATE, not against the job's existing merged state - the service
    // layer merges with the existing row afterward (see updateJobService)
    // and doesn't re-run this validation, so a request that only sends
    // max_catchup_per_poll against a job that already has
    // backfill_on_missed_run=true is legitimate and shouldn't be rejected
    // here just because backfill_on_missed_run isn't in THIS payload.
    if (data.backfill_on_missed_run === true && data.schedule_type === "ONCE") {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["backfill_on_missed_run"],
        message: "backfill_on_missed_run only applies to CRON jobs.",
      });
    }

    if (data.max_catchup_per_poll != null && data.backfill_on_missed_run === false) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["max_catchup_per_poll"],
        message: "max_catchup_per_poll requires backfill_on_missed_run to be true.",
      });
    }
    if (data.auth_type != null && data.auth_type !== "NONE") {
      const authResult = authConfigSchema.safeParse({
        auth_type: data.auth_type,
        auth_config: data.auth_config ?? {},
      });
      if (!authResult.success) {
        for (const issue of authResult.error.issues) {
          ctx.addIssue({
            ...issue,
            path: [
              "auth_config",
              ...issue.path.filter((p) => p !== "auth_type"),
            ],
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
  // Stays offset-based (unlike execution history): jobs-per-team is
  // bounded, and the dashboard wants total/page-N, which keyset
  // pagination doesn't give for free.
  sort_by: z.enum(["created_at", "updated_at", "next_run"]).optional().default("created_at"),
  sort_dir: z.enum(["asc", "desc"]).optional().default("desc"),
});

// Execution history is keyset (cursor) paginated, not offset - see
// execution.repository.js#getExecutionHistory for why. `cursor` is the
// opaque token from the previous page's `nextCursor`; omit it to start
// from the first page.
export const executionHistoryQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(100).optional().default(20),
  cursor: z.string().optional(),
  status: z
    .enum([
      "queued",
      "running",
      "success",
      "failed",
      "retry_wait",
      "failed_permanent",
      "failed_max_retries",
    ])
    .optional(),
  sort: z.enum(["asc", "desc"]).optional().default("desc"),
});