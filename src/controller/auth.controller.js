import { asyncHandler } from "../middlewares/asyncHandler.js";
import { AppError } from "../Error/appError.js";
import {
  signupSchema,
  loginSchema,
  refreshSchema,
  logoutSchema,
} from "../validators/auth.js";
import {
  signupService,
  loginService,
  refreshService,
  logoutService,
} from "../services/auth.service.js";

export const signup = asyncHandler(async (req, res) => {
  const validated = signupSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const { account_name, email, password } = validated.data;
  const result = await signupService({
    accountName: account_name,
    email,
    password,
  });

  return res.status(201).json({ success: true, data: result });
});

export const login = asyncHandler(async (req, res) => {
  const validated = loginSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const { email, password } = validated.data;
  const result = await loginService({
    email,
    password,
    userAgent: req.headers["user-agent"] ?? null,
    ip: req.ip ?? null,
  });

  return res.status(200).json({ success: true, data: result });
});

export const refresh = asyncHandler(async (req, res) => {
  const validated = refreshSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const result = await refreshService({
    refreshToken: validated.data.refresh_token,
    userAgent: req.headers["user-agent"] ?? null,
    ip: req.ip ?? null,
  });
  return res.status(200).json({ success: true, data: result });
});

export const logout = asyncHandler(async (req, res) => {
  const validated = logoutSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  await logoutService({ refreshToken: validated.data.refresh_token });

  return res.status(204).send();
});

export const me = asyncHandler(async (req, res) => {
  // req.auth is set by authenticateJWT
  return res.status(200).json({ success: true, data: req.auth });
});
