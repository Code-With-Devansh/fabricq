import { asyncHandler } from "../middlewares/asyncHandler.js";
import { AppError } from "../Error/appError.js";
import {
  signupSchema,
  loginSchema,
  verifyEmailSchema,
  forgotPasswordSchema,
  resetPasswordSchema,
  twoFactorLoginVerifySchema,
  twoFactorEnableInitiateSchema,
  twoFactorEnableConfirmSchema,
  twoFactorDisableSchema,
} from "../validators/auth.js";
import {
  signupService,
  loginService,
  refreshService,
  logoutService,
  verifyEmailService,
  resendVerificationService,
  forgotPasswordService,
  resetPasswordService,
  twoFactorLoginVerifyService,
  initiateTwoFactorEnableService,
  confirmTwoFactorEnableService,
  disableTwoFactorService,
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
  const result = await loginService({
    email,
    password,
    userAgent: req.headers["user-agent"] ?? null,
    ip: req.ip ?? null,
  });

  // 2FA-enabled accounts don't get tokens yet - the client has to call
  // /auth/2fa/login-verify with the challenge_id + emailed code first.
  if (result.requires_2fa) {
    return res.status(200).json({ success: true, data: result });
  }

  const { refreshToken, ...body } = result;
  setRefreshCookie(res, refreshToken);
  return res.status(200).json({ success: true, data: body });
});

export const twoFactorLoginVerify = asyncHandler(async (req, res) => {
  const validated = twoFactorLoginVerifySchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const { challenge_id, code } = validated.data;
  const { refreshToken, ...body } = await twoFactorLoginVerifyService({
    challengeId: challenge_id,
    code,
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

// --- email verification -------------------------------------------------

export const verifyEmail = asyncHandler(async (req, res) => {
  const validated = verifyEmailSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  await verifyEmailService(validated.data.token);
  return res.status(200).json({ success: true, data: { verified: true } });
});

export const resendVerification = asyncHandler(async (req, res) => {
  await resendVerificationService(req.auth.userId);
  return res.status(202).json({ success: true, data: { sent: true } });
});

// --- forgot / reset password ---------------------------------------------

export const forgotPassword = asyncHandler(async (req, res) => {
  const validated = forgotPasswordSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  await forgotPasswordService({ email: validated.data.email, ip: req.ip ?? null });

  // Always the same response, whether or not the email exists - see
  // forgotPasswordService for the enumeration-safety reasoning.
  return res.status(200).json({
    success: true,
    data: { message: "If that email has an account, a reset link has been sent." },
  });
});

export const resetPassword = asyncHandler(async (req, res) => {
  const validated = resetPasswordSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  await resetPasswordService({
    token: validated.data.token,
    newPassword: validated.data.password,
  });

  // Resetting the password revokes every existing session (see
  // resetPasswordService), so clear this client's own refresh cookie
  // too rather than leaving a now-invalid one sitting in the browser.
  clearRefreshCookie(res);
  return res.status(200).json({ success: true, data: { reset: true } });
});

// --- two-factor authentication (email OTP) --------------------------------

export const initiateTwoFactorEnable = asyncHandler(async (req, res) => {
  const validated = twoFactorEnableInitiateSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const result = await initiateTwoFactorEnableService({
    userId: req.auth.userId,
    password: validated.data.password,
  });
  return res.status(200).json({ success: true, data: result });
});

export const confirmTwoFactorEnable = asyncHandler(async (req, res) => {
  const validated = twoFactorEnableConfirmSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  await confirmTwoFactorEnableService({
    userId: req.auth.userId,
    challengeId: validated.data.challenge_id,
    code: validated.data.code,
  });
  return res.status(200).json({ success: true, data: { two_factor_enabled: true } });
});

export const disableTwoFactor = asyncHandler(async (req, res) => {
  const validated = twoFactorDisableSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  await disableTwoFactorService({
    userId: req.auth.userId,
    password: validated.data.password,
  });
  return res.status(200).json({ success: true, data: { two_factor_enabled: false } });
});
