import express from "express";
import {
  signup,
  login,
  refresh,
  logout,
  me,
  verifyEmail,
  resendVerification,
  forgotPassword,
  resetPassword,
  twoFactorLoginVerify,
  initiateTwoFactorEnable,
  confirmTwoFactorEnable,
  disableTwoFactor,
} from "../controller/auth.controller.js";
import { authenticateJWT } from "../middlewares/authenticateJWT.js";

const router = express.Router();

router.post("/signup", signup);
router.post("/login", login);
router.post("/refresh", refresh);
router.post("/logout", logout);
router.get("/me", authenticateJWT, me);

// Email verification - the link in the email carries the token, no
// auth needed to redeem it. Resending requires auth since it targets
// "my account" rather than an arbitrary email in the body.
router.post("/verify-email", verifyEmail);
router.post("/resend-verification", authenticateJWT, resendVerification);

// Forgot/reset password - deliberately unauthenticated on both ends,
// that's the point of the flow (you're locked out).
router.post("/forgot-password", forgotPassword);
router.post("/reset-password", resetPassword);

// 2FA: completing a login challenge is unauthenticated (no token exists
// yet - that's what this endpoint produces). Enabling/disabling acts on
// "my account" so both require an existing session.
router.post("/2fa/login-verify", twoFactorLoginVerify);
router.post("/2fa/enable/initiate", authenticateJWT, initiateTwoFactorEnable);
router.post("/2fa/enable/confirm", authenticateJWT, confirmTwoFactorEnable);
router.post("/2fa/disable", authenticateJWT, disableTwoFactor);

export default router;
