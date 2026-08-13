import { z } from "zod";

export const updateTeamSchema = z.object({
  name: z.string().trim().min(1).max(200),
});

export const updateMemberRoleSchema = z.object({
  role_id: z.string().uuid(),
});

export const createRoleSchema = z.object({
  name: z.string().trim().min(1).max(100),
  permissions: z.array(z.string()).min(1),
});
