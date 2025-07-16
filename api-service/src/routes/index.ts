import { Router } from 'express';
import etlRoutes from './etl.routes';
import dataRoutes from './data.routes';

const router = Router();

router.use('/etl', etlRoutes);
router.use('/data', dataRoutes);

export default router;
