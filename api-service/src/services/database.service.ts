import { Pool } from 'pg';
import { ETLJob } from './etl.service';
import { DataFilters, ClinicalMeasurement } from './data.service';

export class DatabaseService {
  private pool: Pool;

  constructor() {
    this.pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    });
  }

  /**
   * Create ETL job record
   */
  async createETLJob(job: ETLJob): Promise<void> {
    const query = `
      INSERT INTO etl_jobs (id, filename, study_id, status, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6)
    `;
    
    await this.pool.query(query, [
      job.id,
      job.filename,
      job.studyId,
      job.status,
      job.createdAt,
      job.updatedAt
    ]);
  }

  /**
   * Update ETL job status
   */
  async updateETLJobStatus(jobId: string, status: string, errorMessage?: string): Promise<void> {
    let query = `
      UPDATE etl_jobs 
      SET status = $1, updated_at = $2
    `;
    const params = [status, new Date()];

    if (status === 'completed') {
      query += ', completed_at = $3';
      params.push(new Date());
    }

    if (errorMessage) {
      query += ', error_message = $' + (params.length + 1);
      params.push(errorMessage);
    }

    query += ' WHERE id = $' + (params.length + 1);
    params.push(jobId);

    await this.pool.query(query, params);
  }

  /**
   * Get ETL job by ID
   */
  async getETLJob(jobId: string): Promise<ETLJob | null> {
    const query = `
      SELECT id, filename, study_id, status, created_at, updated_at, completed_at, error_message
      FROM etl_jobs
      WHERE id = $1
    `;
    
    const result = await this.pool.query(query, [jobId]);
    
    if (result.rows.length === 0) {
      return null;
    }

    const row = result.rows[0];
    return {
      id: row.id,
      filename: row.filename,
      studyId: row.study_id,
      status: row.status,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      completedAt: row.completed_at,
      errorMessage: row.error_message
    };
  }

  /**
   * Query clinical measurements with filters
   */
  async queryMeasurements(filters: DataFilters): Promise<ClinicalMeasurement[]> {
    let query = `
      SELECT id, study_id, participant_id, measurement_type, value, unit, 
             timestamp, site_id, quality_score, processed_at
      FROM clinical_measurements
      WHERE 1=1
    `;
    const params: any[] = [];
    let paramIndex = 1;

    if (filters.studyId) {
      query += ` AND study_id = $${paramIndex}`;
      params.push(filters.studyId);
      paramIndex++;
    }

    if (filters.participantId) {
      query += ` AND participant_id = $${paramIndex}`;
      params.push(filters.participantId);
      paramIndex++;
    }

    if (filters.measurementType) {
      query += ` AND measurement_type = $${paramIndex}`;
      params.push(filters.measurementType);
      paramIndex++;
    }

    if (filters.startDate) {
      query += ` AND timestamp >= $${paramIndex}`;
      params.push(filters.startDate);
      paramIndex++;
    }

    if (filters.endDate) {
      query += ` AND timestamp <= $${paramIndex}`;
      params.push(filters.endDate);
      paramIndex++;
    }

    query += ' ORDER BY timestamp DESC LIMIT 1000';

    const result = await this.pool.query(query, params);
    
    return result.rows.map(row => ({
      id: row.id,
      studyId: row.study_id,
      participantId: row.participant_id,
      measurementType: row.measurement_type,
      value: row.value,
      unit: row.unit,
      timestamp: row.timestamp,
      siteId: row.site_id,
      qualityScore: row.quality_score,
      processedAt: row.processed_at
    }));
  }

  /**
   * Close database connection
   */
  async close(): Promise<void> {
    await this.pool.end();
  }
}
