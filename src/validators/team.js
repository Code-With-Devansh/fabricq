import { z } from "zod";

export const createTeamSchema = z.object({
  name: z.string().trim().min(1).max(200),
});

export const updateTeamSchema = z.object({
  name: z.string().trim().min(1).max(200),
});

export const createInvitationSchema = z.object({
  email: z.string().trim().toLowerCase().email(),
  // role_id: z.string().uuid(),
  role_id: z.string().regex(
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
    "Invalid UUID"
  ),
});

export const acceptInvitationSchema = z.object({
  token: z.string().trim().min(1),
});

export const updateMemberRoleSchema = z.object({
  role_id: z.string().uuid(),
});

export const createRoleSchema = z.object({
  name: z.string().trim().min(1).max(100),
  permissions: z.array(z.string()).min(1),
});
