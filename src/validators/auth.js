import { z } from "zod";

export const signupSchema = z.object({
  team_name: z.string().trim().min(1).max(200),
  email: z.string().trim().toLowerCase().email(),
  password: z.string().min(8).max(256),
});

export const loginSchema = z.object({
  email: z.string().trim().toLowerCase().email(),
  password: z.string().min(1).max(256),
});

export const verifyEmailSchema = z.object({
  token: z.string().trim().min(1),
});

export const forgotPasswordSchema = z.object({
  email: z.string().trim().toLowerCase().email(),
});

export const resetPasswordSchema = z.object({
  token: z.string().trim().min(1),
  password: z.string().min(8).max(256),
});

const otpCodeSchema = z.string().trim().regex(/^\d{6}$/, "Code must be 6 digits");

export const twoFactorLoginVerifySchema = z.object({
  challenge_id: z.string().uuid(),
  code: otpCodeSchema,
});

export const twoFactorEnableInitiateSchema = z.object({
  password: z.string().min(1).max(256),
});

export const twoFactorEnableConfirmSchema = z.object({
  challenge_id: z.string().uuid(),
  code: otpCodeSchema,
});

export const twoFactorDisableSchema = z.object({
  password: z.string().min(1).max(256),
});
