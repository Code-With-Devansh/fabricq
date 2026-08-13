import express from "express";
import { signup, login, refresh, logout, me } from "../controller/auth.controller.js";
import { authenticateJWT } from "../middlewares/authenticateJWT.js";

const router = express.Router();

router.post("/signup", signup);
router.post("/login", login);
router.post("/refresh", refresh);
router.post("/logout", logout);
router.get("/me", authenticateJWT, me);

export default router;
