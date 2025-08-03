import axios from 'axios';
import { v4 as uuidv4 } from 'uuid';
import { DatabaseService } from './database.service';

export interface ETLJob {
  id: string;
  filename: string;
  studyId?: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  createdAt: Date;
  updatedAt: Date;
  completedAt?: Date;
  errorMessage?: string;
}

export class ETLService {
  private dbService: DatabaseService;
  private etlServiceUrl: string;

  constructor() {
    this.dbService = new DatabaseService();
    this.etlServiceUrl = process.env.ETL_SERVICE_URL || 'http://etl:8000';
  }

  /**
   * Submit new ETL job
   */
  async submitJob(jobId: string, filename: string, studyId?: string): Promise<ETLJob> {
    // const jobId = uuidv4();
    
    // Create job record in database
    const job: ETLJob = {
      id: jobId,
      filename,
      studyId,
      status: 'pending',
      createdAt: new Date(),
      updatedAt: new Date()
    };

    await this.dbService.createETLJob(job);

    // Submit job to ETL service
    try {
      await axios.post(`${this.etlServiceUrl}/jobs`, {
        jobId,
        filename,
        studyId
      });

      // Update job status to running
      await this.dbService.updateETLJobStatus(jobId, 'running');
      job.status = 'running';
    } catch (error) {
      // Update job status to failed
      await this.dbService.updateETLJobStatus(jobId, 'failed', 'Failed to submit to ETL service');
      job.status = 'failed';
      job.errorMessage = 'Failed to submit to ETL service';
    }

    return job;
  }

  /**
   * Get ETL job by ID
   */
  async getJob(jobId: string): Promise<ETLJob | null> {
    return await this.dbService.getETLJob(jobId);
  }

  /**
   * Get ETL job status from ETL service
   */
  async getJobStatus(jobId: string): Promise<{ status: string; progress?: number; message?: string }> {
    // 1. Validate jobId exists in database
    const job = await this.dbService.getETLJob(jobId);
    if (!job) {
      throw new Error('Job not found');
    }

    try {
      // 2. Call ETL service to get real-time status
      const response = await axios.get(`${this.etlServiceUrl}/jobs/${jobId}/status`, {
        timeout: 5000 // 5 second timeout
      });

      const { status, progress, message } = response.data;

      // Update database if status has changed
      if (status !== job.status) {
        await this.dbService.updateETLJobStatus(jobId, status, message);
      }

      // 4. Return formatted status response
      return {
        status,
        progress,
        message
      };
    } catch (error: unknown) {
      // 3. Handle connection errors gracefully
      if (axios.isAxiosError(error)) {
        const axiosError = error as any;
        if (axiosError.code === 'ECONNREFUSED' || axiosError.code === 'ETIMEDOUT') {
          // ETL service is unreachable, return database status
          return {
            status: job.status,
            message: 'ETL service unavailable - showing last known status'
          };
        }
        
        if (axiosError.response?.status === 404) {
          // Job not found in ETL service, might be completed or failed
          return {
            status: job.status,
            message: 'Job not found in ETL service - showing database status'
          };
        }
      }

      // For other errors, rethrow
      throw new Error(`Failed to get job status: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }
}
