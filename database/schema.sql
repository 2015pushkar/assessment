-- Clinical Data ETL Pipeline Database Schema
-- TODO: Candidate to design and implement optimal schema

-- Basic schema provided for bootstrapping
-- Candidate should enhance with proper indexes, constraints, and optimization

-- ETL Jobs tracking table
CREATE TABLE IF NOT EXISTS etl_jobs (
    id UUID PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    study_id VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT
);

-- TODO: Candidate to implement
-- Expected tables to be designed by candidate:
-- - clinical_measurements (raw data)
-- - processed_measurements (transformed data)
-- - participants
-- - studies
-- - data_quality_reports
-- - measurement_aggregations

-- Sample basic table structure (candidate should enhance)
CREATE TABLE IF NOT EXISTS clinical_measurements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    study_id VARCHAR(50) NOT NULL,
    participant_id VARCHAR(50) NOT NULL,
    measurement_type VARCHAR(50) NOT NULL,
    value TEXT NOT NULL,
    unit VARCHAR(20),
    timestamp TIMESTAMP NOT NULL,
    site_id VARCHAR(50) NOT NULL,
    quality_score DECIMAL(3,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Basic indexes (candidate should optimize)
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_study_id ON clinical_measurements(study_id);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_participant_id ON clinical_measurements(participant_id);
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_timestamp ON clinical_measurements(timestamp);

-- ETL Jobs indexes
CREATE INDEX IF NOT EXISTS idx_etl_jobs_status ON etl_jobs(status);
CREATE INDEX IF NOT EXISTS idx_etl_jobs_created_at ON etl_jobs(created_at);
