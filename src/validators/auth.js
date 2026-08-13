import { z } from "zod";

export const signupSchema = z.object({
  account_name: z.string().trim().min(1).max(200),
  email: z.string().trim().toLowerCase().email(),
  password: z.string().min(8).max(256),
});

export const loginSchema = z.object({
  email: z.string().trim().toLowerCase().email(),
  password: z.string().min(1).max(256),
});

export const refreshSchema = z.object({
  refresh_token: z.string().min(1),
});

export const logoutSchema = z.object({
  refresh_token: z.string().min(1),
});
