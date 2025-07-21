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
    participant_id VARCHAR(50) PRIMARY KEY
);

-- 3b. Participant Enrollments
CREATE TABLE IF NOT EXISTS participant_enrollments (
    participant_id VARCHAR(50) NOT NULL,
    study_id       VARCHAR(50) NOT NULL,
    enrolled_at    TIMESTAMP   NOT NULL,
    PRIMARY KEY (participant_id, study_id),
    FOREIGN KEY (participant_id) REFERENCES participants(participant_id) ON DELETE CASCADE,
    FOREIGN KEY (study_id)       REFERENCES studies(study_id)            ON DELETE CASCADE
);

-- 4. Clinical Measurements
CREATE TABLE IF NOT EXISTS clinical_measurements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    study_id        VARCHAR(50) NOT NULL,
    participant_id  VARCHAR(50) NOT NULL,
    site_id         VARCHAR(50) NOT NULL,
    measurement_type VARCHAR(50) NOT NULL,
    value            TEXT        NOT NULL,
    unit             VARCHAR(20),
    "timestamp"      TIMESTAMP   NOT NULL,
    quality_score DECIMAL(3,2),
    processed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    value_num DOUBLE PRECISION GENERATED ALWAYS AS (
        CASE
            WHEN trim(value) ~ '^[0-9]+(\.[0-9]+)?$'
            THEN trim(value)::DOUBLE PRECISION
        END
    ) STORED,
    bp_systolic INT GENERATED ALWAYS AS (
        CASE
            WHEN trim(value) ~ '^[0-9]+/[0-9]+$'
            THEN (string_to_array(trim(value),'/'))[1]::INT
        END
    ) STORED,
    bp_diastolic INT GENERATED ALWAYS AS (
        CASE
            WHEN trim(value) ~ '^[0-9]+/[0-9]+$'
            THEN (string_to_array(trim(value),'/'))[2]::INT
        END
    ) STORED,
    FOREIGN KEY (study_id)                 REFERENCES studies(study_id) ON DELETE CASCADE,
    FOREIGN KEY (participant_id, study_id) REFERENCES participant_enrollments(participant_id, study_id) ON DELETE CASCADE,
    FOREIGN KEY (site_id)                  REFERENCES sites(site_id)    ON DELETE CASCADE
);

-- 5. ETL Jobs
CREATE TABLE IF NOT EXISTS etl_jobs (
    id UUID PRIMARY KEY,
    filename     VARCHAR(255) NOT NULL,
    study_id     VARCHAR(50),
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending',
    progress     INTEGER      DEFAULT 0,
    message      TEXT,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    FOREIGN KEY (study_id) REFERENCES studies(study_id) ON DELETE SET NULL
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_cm_study_id       ON clinical_measurements (study_id);
CREATE INDEX IF NOT EXISTS idx_cm_participant_id ON clinical_measurements (participant_id);
CREATE INDEX IF NOT EXISTS idx_cm_timestamp      ON clinical_measurements ("timestamp");
CREATE INDEX IF NOT EXISTS idx_cm_type_value     ON clinical_measurements (measurement_type, value_num);
CREATE INDEX IF NOT EXISTS idx_cm_bp             ON clinical_measurements (bp_systolic, bp_diastolic);
CREATE INDEX IF NOT EXISTS idx_etl_jobs_status   ON etl_jobs(status);
CREATE INDEX IF NOT EXISTS idx_etl_jobs_created  ON etl_jobs(created_at);
