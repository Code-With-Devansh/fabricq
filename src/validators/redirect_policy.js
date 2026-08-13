import { z } from "zod";

// Hard ceiling regardless of what the caller asks for - matches the
// CHECK constraint in migration 011_redirect_policy.sql. Keeping both in
// sync matters: the DB constraint is the backstop, this is the friendly
// 400 instead of a 500 from a constraint violation.
const MAX_REDIRECTS_CEILING = 20;

export const redirectPolicySchema = z
  .object({
    maxRedirects: z.number().int().min(0).max(MAX_REDIRECTS_CEILING).optional().default(10),
    allowCrossOrigin: z.boolean().optional().default(false),
    allowHttpDowngrade: z.boolean().optional().default(false),
  })
  .strict();