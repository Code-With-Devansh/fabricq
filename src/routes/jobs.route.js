import express from "express";
import { authenticateJWT } from "../middlewares/authenticateJWT.js";
import { loadTeamContext, requirePermission } from "../middlewares/teamContext.js";
import {
  uploadJob,
  getJobs,
  getJob,
  updateJob,
  deleteJob,
  getJobExecutions,
  getExecutionDetail,
} from "../controller/jobs.controller.js";

const router = express.Router({ mergeParams: true });

router.use(authenticateJWT, loadTeamContext("teamId"));

router.post("/", requirePermission("jobs:write"), uploadJob);
router.get("/", requirePermission("jobs:read"), getJobs);
// Static path before the :jobId param, same ordering as the original
// flat route, so "executions" never gets swallowed as a job id.
router.get(
  "/executions/:executionId",
  requirePermission("executions:read"),
  getExecutionDetail
);
router.get("/:jobId", requirePermission("jobs:read"), getJob);
router.patch("/:jobId", requirePermission("jobs:write"), updateJob);
router.delete("/:jobId", requirePermission("jobs:delete"), deleteJob);
router.get(
  "/:jobId/executions",
  requirePermission("executions:read"),
  getJobExecutions
);

export default router;
