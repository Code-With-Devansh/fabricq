import express from "express";
import cors from "cors";
import config from "./config/index.js";
import jobsRoute from './routes/jobs.route.js'
import {isShuttingDown} from './state/shutdown.js'
import { errorHandler, notFoundHandler } from "./Error/errorHandler.js";
const app = express();
app.use(cors());
app.use(express.json());
app.use((req, res, next) => {
  if (isShuttingDown()) {
    return res.status(503).json({
      success: false,
      message: "Server shutting down",
    });
  }
  next();
});

app.use("/jobs", jobsRoute)

app.get("/health", (req, res) => {
  if (isShuttingDown()) {
    return res.status(503).json({
      success: false,
      status: "shutting_down",
    });
  }
  return res.json({
    success: true,
  });
});

app.use(notFoundHandler);
app.use(errorHandler);

export default app;