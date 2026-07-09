import express from 'express'
import {
  uploadJob,
  getJobs,
  getJob,
  updateJob,
  deleteJob,
  getJobExecutions,
  getExecutionDetail,
} from '../controller/jobs.controller.js'
const router = express.Router()

router.post('/upload', uploadJob)
router.get('/', getJobs)
router.get('/executions/:executionId', getExecutionDetail)
router.get('/:jobId', getJob)
router.patch('/:jobId', updateJob)
router.delete('/:jobId', deleteJob)
router.get('/:jobId/executions', getJobExecutions)

export default router