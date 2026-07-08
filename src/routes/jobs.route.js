import express from 'express'
import { uploadJob } from '../controller/jobs.controller.js'
const router = express.Router()

router.post('/upload', uploadJob)

export default router