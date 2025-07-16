import express, { Express, NextFunction, Request, Response } from 'express';
import cors from 'cors';
import bodyParser from 'body-parser';
import compression from 'compression';
import morgan from 'morgan';
import router from './routes';
import { errorHandler } from './middleware/errorHandler';

export default async (): Promise<Express> => {
  const app = express();

  // Standard middleware stack
  app.use(bodyParser.urlencoded({ extended: false }));
  app.use(bodyParser.json({ limit: '50mb' }));
  app.use(morgan(':remote-addr - :remote-user [:date[clf]] ":method :url HTTP/:http-version" :status :res[content-length] :response-time ms'));
  app.use(cors({ credentials: true }));
  app.use(express.json());
  app.use(compression());

  // Routes
  app.use('/api', router);

  // Health check endpoint
  app.get('/health', (req: Request, res: Response) => {
    res.json({ status: 'healthy', timestamp: new Date().toISOString() });
  });

  // Error handling middleware
  app.use(errorHandler);

  // 404 handler
  app.use('*', (req: Request, res: Response) => {
    res.status(404).json({ error: 'Route not found' });
  });

  return app;
};
