import express from "express";
import { authenticateJWT } from "../middlewares/authenticateJWT.js";
import { loadTeamContext, requirePermission } from "../middlewares/teamContext.js";
import {
  listApiKeys,
  createApiKey,
  revokeApiKey,
} from "../controller/apiKey.controller.js";

const router = express.Router({ mergeParams: true });

router.use(authenticateJWT, loadTeamContext("teamId"));

router.get("/", requirePermission("api_keys:read"), listApiKeys);
router.post("/", requirePermission("api_keys:create"), createApiKey);
router.delete("/:keyId", requirePermission("api_keys:revoke"), revokeApiKey);

export default router;
