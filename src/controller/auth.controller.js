import { asyncHandler } from "../middlewares/asyncHandler.js";
import { AppError } from "../Error/appError.js";
import {
  signupSchema,
  loginSchema,
} from "../validators/auth.js";
import {
  signupService,
  loginService,
  refreshService,
  logoutService,
} from "../services/auth.service.js";
import {
  setRefreshCookie,
  clearRefreshCookie,
  getRefreshCookie,
} from "../utils/refreshCookie.js";

export const signup = asyncHandler(async (req, res) => {
  const validated = signupSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const { team_name, email, password } = validated.data;
  const { refreshToken, ...body } = await signupService({
    teamName: team_name,
    email,
    password,
  });

  setRefreshCookie(res, refreshToken);
  return res.status(201).json({ success: true, data: body });
});

export const login = asyncHandler(async (req, res) => {
  const validated = loginSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const { email, password } = validated.data;
  const { refreshToken, ...body } = await loginService({
    email,
    password,
    userAgent: req.headers["user-agent"] ?? null,
    ip: req.ip ?? null,
  });

  setRefreshCookie(res, refreshToken);
  return res.status(200).json({ success: true, data: body });
});

export const refresh = asyncHandler(async (req, res) => {
  const incomingRefreshToken = getRefreshCookie(req);
  if (!incomingRefreshToken) {
    throw new AppError("Missing refresh token", 401);
  }

  const { refreshToken, ...body } = await refreshService({
    refreshToken: incomingRefreshToken,
    userAgent: req.headers["user-agent"] ?? null,
    ip: req.ip ?? null,
  });

  setRefreshCookie(res, refreshToken);
  return res.status(200).json({ success: true, data: body });
});

export const logout = asyncHandler(async (req, res) => {
  const incomingRefreshToken = getRefreshCookie(req);

  if (incomingRefreshToken) {
    await logoutService({ refreshToken: incomingRefreshToken });
  }

  clearRefreshCookie(res);
  return res.status(204).send();
});

export const me = asyncHandler(async (req, res) => {
  // req.auth is set by authenticateJWT
  return res.status(200).json({ success: true, data: req.auth });
});
