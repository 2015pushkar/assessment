import { Request, Response, NextFunction } from 'express';
// import { File } from 'multer'; 
import { ETLService } from '../services/etl.service';
import { v4 as uuidv4 } from 'uuid';
import { promises as fs } from 'fs';
import path from 'path';
import { successResponse, errorResponse } from '../utils/response';

// extend only for this module
type MulterRequest = Request & {
    file?: Express.Multer.File;
}

export class ETLController {
  private etlService: ETLService;

  constructor() {
    this.etlService = new ETLService();
  }

  /**
   * Submit new ETL job
   * POST /api/etl/jobs
   */
  submitJob = async (req: MulterRequest, res: Response, next: NextFunction): Promise<void> => {
    try {
      const file = req.file; // Multer will handle file upload
      if (!file) {
        errorResponse(res, 'File is required', 400);
        return;
      }
      const { studyId } = req.body;

      if (!file) {
        errorResponse(res, 'filename is required', 400);
        return;
      }

      // 1. Generate jobId and final filename
      const jobId = uuidv4();
      const orig  = path.basename(file.originalname);
      const finalFilename = `${jobId}_${orig}`;
      const finalPath = path.join('/data', finalFilename);

      // 2. Move from Multer’s tmp dir → shared /data
      await fs.rename(file.path, finalPath);

      // 3. Kick off DB + ETL-service call
      const job = await this.etlService.submitJob(jobId, finalFilename, studyId);
      successResponse(res, job, 'ETL job submitted successfully');
    } catch (error) {
      next(error);
    }
  };

  /**
   * Get ETL job details
   * GET /api/etl/jobs/:id
   */
  getJob = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const { id } = req.params;
      const job = await this.etlService.getJob(id);
      
      if (!job) {
        errorResponse(res, 'Job not found', 404);
        return;
      }

      successResponse(res, job, 'Job retrieved successfully');
    } catch (error) {
      next(error);
    }
  };

  /**
   * Get ETL job status
   * GET /api/etl/jobs/:id/status
   */
  getJobStatus = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const { id } = req.params; // incoming http request

      if (!id) {
        errorResponse(res, 'Job ID is required', 400); // outgoing http response
        return;
      }

      const status = await this.etlService.getJobStatus(id); 
      successResponse(res, status, 'Job status retrieved successfully');
    } catch (error) {
      // Handle specific error cases
      if (error instanceof Error && error.message === 'Job not found') {
        errorResponse(res, 'Job not found', 404);
        return;
      }

      next(error);
    }
  };
}
