import { Router } from 'express';
import { ETLController } from '../controllers/etl.controller';

const router = Router();
const etlController = new ETLController();

// POST /api/etl/jobs - Submit new ETL job
router.post('/jobs', etlController.submitJob);

// GET /api/etl/jobs/:id/status - Get ETL job status (must come before /:id route)
router.get('/jobs/:id/status', etlController.getJobStatus);

// GET /api/etl/jobs/:id - Get ETL job details
router.get('/jobs/:id', etlController.getJob); 

export default router;
