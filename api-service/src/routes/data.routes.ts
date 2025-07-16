import { Router } from 'express';
import { DataController } from '../controllers/data.controller';

const router = Router();
const dataController = new DataController();

// GET /api/data - Query processed clinical data
router.get('/', dataController.getData);

// GET /api/data/studies/:id - Get study data
router.get('/studies/:id', dataController.getStudyData);

export default router;
