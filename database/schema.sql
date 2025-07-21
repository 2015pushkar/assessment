-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 1. Studies
CREATE TABLE IF NOT EXISTS studies (
    study_id VARCHAR(50) PRIMARY KEY
);

-- 2. Sites
CREATE TABLE IF NOT EXISTS sites (
    site_id VARCHAR(50) PRIMARY KEY
);

-- 3. Participants
CREATE TABLE IF NOT EXISTS participants (
    participant_id VARCHAR(50) PRIMARY KEY,
    study_id VARCHAR(50) NOT NULL,
    CONSTRAINT fk_participant_study FOREIGN KEY (study_id)
        REFERENCES studies(study_id) ON DELETE CASCADE
);

-- 4. Clinical Measurements
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_cm_study FOREIGN KEY (study_id)
        REFERENCES studies(study_id) ON DELETE CASCADE,
    CONSTRAINT fk_cm_participant FOREIGN KEY (participant_id)
        REFERENCES participants(participant_id) ON DELETE CASCADE,
    CONSTRAINT fk_cm_site FOREIGN KEY (site_id)
        REFERENCES sites(site_id) ON DELETE CASCADE
);

-- 5. ETL Jobs
CREATE TABLE IF NOT EXISTS etl_jobs (
    id UUID PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    study_id VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    CONSTRAINT fk_etl_study FOREIGN KEY (study_id)
        REFERENCES studies(study_id) ON DELETE SET NULL
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_clinical_measurements_study_id
    ON clinical_measurements(study_id);

CREATE INDEX IF NOT EXISTS idx_clinical_measurements_participant_id
    ON clinical_measurements(participant_id);

CREATE INDEX IF NOT EXISTS idx_clinical_measurements_timestamp
    ON clinical_measurements(timestamp);

CREATE INDEX IF NOT EXISTS idx_etl_jobs_status
    ON etl_jobs(status);

CREATE INDEX IF NOT EXISTS idx_etl_jobs_created_at
    ON etl_jobs(created_at);
