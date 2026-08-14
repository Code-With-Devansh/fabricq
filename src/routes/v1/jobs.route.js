import express from "express";
import { authenticateApiKey, requireScope } from "../../middlewares/authenticateApiKey.js";
import {
  uploadJob,
  getJobs,
  getJob,
  updateJob,
  deleteJob,
  getJobExecutions,
  getExecutionDetail,
} from "../../controller/jobs.controller.js";

// Public API - authenticated via API key, not JWT. Team is implicit in
// the key itself (req.apiKey.teamId), never taken from the URL, so a key
// can only ever see/act on its own team's jobs no matter what.
const router = express.Router();

router.use(authenticateApiKey);

router.post("/", requireScope("jobs:write"), uploadJob);
router.get("/", requireScope("jobs:read"), getJobs);
router.get(
  "/executions/:executionId",
  requireScope("executions:read"),
  getExecutionDetail
);
router.get("/:jobId", requireScope("jobs:read"), getJob);
router.patch("/:jobId", requireScope("jobs:write"), updateJob);
router.delete("/:jobId", requireScope("jobs:delete"), deleteJob);
router.get(
  "/:jobId/executions",
  requireScope("executions:read"),
  getJobExecutions
);

export default router;
