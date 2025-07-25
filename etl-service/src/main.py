from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict, Any
import uvicorn
import os
import pandas as pd
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from dateutil import parser
import pytz
import psycopg2
import io
import uuid         
from psycopg2.extras import execute_values 
import hashlib, uuid
import asyncio

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(asctime)s | %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Clinical Data ETL Service", version="1.0.0")

# In-memory job storage (for demo purposes)
# In production, this would use a proper database or job queue
jobs: Dict[str, Dict[str, Any]] = {}

class ETLJobRequest(BaseModel):
    jobId: str
    filename: str
    studyId: Optional[str] = None

class ETLJobResponse(BaseModel):
    jobId: str
    status: str
    message: str

class ETLJobStatus(BaseModel):
    jobId: str
    status: str
    progress: Optional[int] = None
    message: Optional[str] = None

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "etl"}

async def extract_file(job_id: str, filename: str) -> Optional[pd.DataFrame]:
    """Read a CSV file from the mounted **/data** volume and update job metadata."""

    file_path = f"/data/{filename}"

    if not os.path.exists(file_path):
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["message"] = f"File not found: {filename}"
        return None

    try:
        df = pd.read_csv(file_path)
        jobs[job_id]["progress"] = 10  # 10 % after extraction
        jobs[job_id]["message"] = f"Extracted {len(df)} rows"
        return df
    except Exception as exc:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["message"] = f"Extraction error: {exc}"
        return None

async def transform_data(job_id: str, df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Clean & normalize the extracted DataFrame.

    Steps:
    1. Lower‑case and underscore column names
    2. Parse `timestamp` to pandas datetime
    3. Coerce numeric columns (`value`, `quality_score`)
    4. Strip whitespace from string fields
    5. Add `processed_at` & `job_id` metadata columns
    """
    try:
        # normalize column names
        df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]

        # Parse timestamp to UTC
        df['timestamp'] = df['timestamp'].apply(lambda t: parser.isoparse(t).astimezone(pytz.UTC))
        # Coerce types
        # df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df['quality_score'] = pd.to_numeric(df['quality_score'], errors='coerce')
        # Clamp quality_score
        df['quality_score'] = df['quality_score'].clip(lower=0, upper=1)
       # Strip strings
        for col in ['study_id','participant_id','measurement_type','unit','site_id']:
            if col in df:
                df[col] = df[col].astype(str).str.strip()

        # Metadata
        df['processed_at'] = datetime.utcnow()
        df['created_at'] = datetime.utcnow()
        jobs[job_id]['progress'] = 30
        jobs[job_id]['message'] = 'Data transformed'
        logger.info(f"Job {job_id}: transformed {len(df)} rows")
        return df
    except Exception as e:
        jobs[job_id]['status'] = 'failed'
        jobs[job_id]['message'] = f'Transform error: {e}'
        logger.exception(f"Job {job_id}: transform failed")
        return None

# python level validation logic
VALID_MEASUREMENTS = {"glucose", "cholesterol", "weight", "height", "blood_pressure", "heart_rate"}

RANGE_LIMITS = {
    "glucose": (70, 200),
    "cholesterol": (100, 300),
    "weight": (30, 200),
    "height": (100, 250),
    "blood_pressure": (90, 120),
    "heart_rate": (60, 100)
}

async def validate_data(job_id: str, df: pd.DataFrame) -> bool:
    errors = []
    # Required columns
    for col in ['study_id','participant_id','measurement_type','value','timestamp']:
        if col not in df.columns:
            errors.append(f"Missing column {col}")
    # Valid measurement types
    bad_types = df[~df['measurement_type'].isin(VALID_MEASUREMENTS)]
    if not bad_types.empty:
        errors.append(f"Invalid types: {bad_types['measurement_type'].unique().tolist()}")
    # Value range checks for numeric types
    # for m,(low,high) in RANGE_LIMITS.items():
    #     if m in df['measurement_type'].values:
    #         subset = df[df['measurement_type']==m]
    #         if subset['value'].dropna().between(low,high).all() is False:
    #             errors.append(f"Out-of-range values for {m}")
    if errors:
        jobs[job_id]['status']='failed'
        jobs[job_id]['message']='; '.join(errors)
        logger.error(f"Job {job_id}: validation errors {errors}")
        return False
    jobs[job_id]['progress']=50
    jobs[job_id]['message']='Validation passed'
    return True

def load_data(job_id: str, df: pd.DataFrame):
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        conn = psycopg2.connect(database_url)
    cur = conn.cursor()

    # Add deterministic UUIDs
    if 'id' not in df.columns:
        df.insert(
            0, 'id',
            df.apply(make_deterministic_id, axis=1)
        )


    # Upsert studies
    for s in df['study_id'].unique():
        cur.execute("INSERT INTO studies(study_id) VALUES (%s) ON CONFLICT DO NOTHING", (s,))

    # Upsert participants
    for pid in df['participant_id'].unique():
        cur.execute("""
            INSERT INTO participants (participant_id)
            VALUES (%s)
            ON CONFLICT DO NOTHING
        """, (pid,))

    #  Upsert participant enrollments
    for (pid, sid), grp in df.groupby(['participant_id', 'study_id']):
        first_seen = grp['timestamp'].min()        # pandas Timestamp
        cur.execute("""
            INSERT INTO participant_enrollments (participant_id, study_id, enrolled_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (participant_id, study_id) DO UPDATE
                SET enrolled_at =
                    LEAST(participant_enrollments.enrolled_at, EXCLUDED.enrolled_at)
        """, (pid, sid, first_seen.to_pydatetime()))


    # Upsert sites
    for site in df['site_id'].unique():
        cur.execute("INSERT INTO sites(site_id) VALUES (%s) ON CONFLICT DO NOTHING", (site,))

    # Bulk insert into clinical_measurements
    buf = io.StringIO()
    df_to_copy = df[['id','study_id','participant_id','measurement_type','value','unit','timestamp','site_id','quality_score','processed_at','created_at']]
    df_to_copy.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cur.copy_expert(
        "COPY clinical_measurements(id, study_id,participant_id,measurement_type,value,unit,timestamp,site_id,quality_score,processed_at,created_at) FROM STDIN WITH CSV",
        buf
    )

    conn.commit()
    inserted_ids = df['id'].tolist()
    upsert_measurement_aggs(inserted_ids)
    cur.close()
    conn.close()

    jobs[job_id]['progress'] = 90
    jobs[job_id]['message'] = 'Loaded into DB'
    logger.info(f"Job {job_id}: loaded {len(df)} rows into database")

def update_etl_job_status(job_id: str, status: str, progress: int = None, message: str = None):
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        conn = psycopg2.connect(database_url)
    
    cur = conn.cursor()
    
    now = datetime.utcnow()
    set_clauses = ["status = %s", "updated_at = %s"]
    values = [status, now]

    if status == 'completed':
        set_clauses.append("completed_at = %s")
        values.append(now)

    if progress is not None:
        set_clauses.append("progress = %s")
        values.append(progress)

    if message is not None:
        set_clauses.append("message = %s")
        values.append(message)

    query = f"""
        UPDATE etl_jobs 
        SET {', '.join(set_clauses)}
        WHERE id = %s
    """
    values.append(job_id)

    cur.execute(query, values)
    conn.commit()
    cur.close()
    conn.close()
    
    logger.info(f"Updated database status for job {job_id}: {status}")

def upsert_measurement_aggs(inserted_ids: list[str]) -> None:
    """
    Collapse the freshly-inserted clinical_measurements rows (identified
    by their UUIDs) into daily buckets and merge them into the
    measurement_aggregations table.

    Call immediately after COPY + commit:
        inserted_ids = df['id'].tolist()
        upsert_measurement_aggs(inserted_ids)
    """
    if not inserted_ids:       # nothing new
        return

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    with conn, conn.cursor() as cur:
        # 1️⃣  collapse only the fresh rows
        cur.execute(
            """
            WITH src AS (
                SELECT *
                FROM   clinical_measurements
                WHERE  id = ANY(%s::uuid[])
            ), agg AS (
                SELECT
                    DATE_TRUNC('day', "timestamp")::date   AS agg_day,
                    study_id, site_id, participant_id, measurement_type,
                    COUNT(*)                              AS measurement_count,
                    AVG(value_num)                        AS avg_value,
                    MIN(value_num)                        AS min_value,
                    MAX(value_num)                        AS max_value,
                    AVG(bp_systolic)                      AS avg_systolic,
                    AVG(bp_diastolic)                     AS avg_diastolic,
                    AVG(quality_score)                    AS avg_quality_score,
                    SUM((quality_score < 0.95)::INT)       AS low_quality_count
                FROM src
                GROUP BY 1,2,3,4,5
            )
            SELECT * FROM agg;
            """,
            (inserted_ids,),
        )
        rows = cur.fetchall()
        if not rows:
            return

        # 2️⃣  merge into measurement_aggregations
        execute_values(
            cur,
            """
            INSERT INTO measurement_aggregations (
                agg_day, study_id, site_id, participant_id, measurement_type,
                measurement_count, avg_value, min_value, max_value,
                avg_systolic, avg_diastolic,
                avg_quality_score, low_quality_count
            )
            VALUES %s
            ON CONFLICT (agg_day, study_id, site_id, participant_id, measurement_type)
            DO UPDATE SET
              measurement_count  = measurement_aggregations.measurement_count
                                   + EXCLUDED.measurement_count,
              min_value          = LEAST(measurement_aggregations.min_value,
                                         EXCLUDED.min_value),
              max_value          = GREATEST(measurement_aggregations.max_value,
                                           EXCLUDED.max_value),
              low_quality_count  = measurement_aggregations.low_quality_count
                                   + EXCLUDED.low_quality_count,
              avg_value          = (
                  measurement_aggregations.avg_value * measurement_aggregations.measurement_count
                + EXCLUDED.avg_value * EXCLUDED.measurement_count
              ) / (measurement_aggregations.measurement_count + EXCLUDED.measurement_count),
              avg_systolic       = COALESCE(
                  (measurement_aggregations.avg_systolic * measurement_aggregations.measurement_count
                 + EXCLUDED.avg_systolic * EXCLUDED.measurement_count)
                  / NULLIF(measurement_aggregations.measurement_count + EXCLUDED.measurement_count,0),
                  measurement_aggregations.avg_systolic),
              avg_diastolic      = COALESCE(
                  (measurement_aggregations.avg_diastolic * measurement_aggregations.measurement_count
                 + EXCLUDED.avg_diastolic * EXCLUDED.measurement_count)
                  / NULLIF(measurement_aggregations.measurement_count + EXCLUDED.measurement_count,0),
                  measurement_aggregations.avg_diastolic),
              avg_quality_score  = (
                  measurement_aggregations.avg_quality_score * measurement_aggregations.measurement_count
                + EXCLUDED.avg_quality_score * EXCLUDED.measurement_count
              ) / (measurement_aggregations.measurement_count + EXCLUDED.measurement_count);
            """,
            rows,
        )

def make_deterministic_id(row) -> str:
    """
    Build a v3-style UUID (MD5 namespace) from the unique content
    of a clinical measurement row.
    """
    key = "|".join(
        str(row[col]) for col in (
            'study_id', 'participant_id', 'timestamp',
            'measurement_type', 'value'
        )
    )
    # MD5 hash → UUID object → str
    return str(uuid.UUID(hashlib.md5(key.encode("utf-8")).hexdigest()))

async def process_etl_job(job_id: str, filename: str, study_id: Optional[str] = None):
    """
    Process an ETL job: extract, transform, validate, and load data.
    Background task - does not return HTTP responses.
    """
    
    # 1. File extraction
    df = await extract_file(job_id, filename)
    if df is None:  
        # Update database status to failed
        update_etl_job_status(job_id, "failed", message=jobs[job_id]["message"])                                  
        return
    if os.getenv('DEVELOPMENT'):
        await asyncio.sleep(10)  # Small delay for demonstration
    # 2. Data transformation 
    df = await transform_data(job_id, df)
    if df is None:
        # Update database status to failed
        update_etl_job_status(job_id, "failed", message=jobs[job_id]["message"])
        return
    if os.getenv('DEVELOPMENT'):
        await asyncio.sleep(10)
        
    # 3. Quality validation
    ok = await validate_data(job_id, df)
    if not ok: 
        update_etl_job_status(job_id, "failed", message=jobs[job_id]["message"])
        return
    if os.getenv('DEVELOPMENT'):
        await asyncio.sleep(10)

    # 4. Database loading
    try:
        load_data(job_id, df)
    except Exception as e:
        jobs[job_id]['status']='failed'
        jobs[job_id]['message']=f"Load error: {e}"
        update_etl_job_status(job_id, "failed", message=jobs[job_id]["message"])
        logger.exception(f"Job {job_id}: load failed")
        return
    if os.getenv('DEVELOPMENT'):
        await asyncio.sleep(10)
        
    # Finish
    jobs[job_id]['status']='completed'
    jobs[job_id]['progress']=100
    jobs[job_id]['message']='Job completed successfully'
    # IMPORTANT: Update database status to completed
    update_etl_job_status(job_id, "completed", progress=100, message="Job completed successfully")
    logger.info(f"Job {job_id}: completed successfully")

@app.post("/jobs", response_model=ETLJobResponse)
async def submit_job(job_request: ETLJobRequest, background_tasks: BackgroundTasks):
    """
    Submit a new ETL job for processing
    """
    job_id = job_request.jobId
    
    # Store job in memory (simplified for demo)
    jobs[job_id] = {
        "jobId": job_id,
        "filename": job_request.filename,
        "studyId": job_request.studyId,
        "status": "running",
        "progress": 0,
        "message": "Job started"
    }
    
    # Start ETL processing in background
    background_tasks.add_task(process_etl_job, job_id, job_request.filename, job_request.studyId)
    
    return ETLJobResponse(jobId=job_id, status="running", message="Job started – processing in background")


@app.get("/jobs/{job_id}/status", response_model=ETLJobStatus)
async def get_job_status(job_id: str):
    """
    Get the current status of an ETL job
    """
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    return ETLJobStatus(
        jobId=job_id,
        status=job["status"],
        progress=job.get("progress"),
        message=job.get("message")
    )

@app.get("/jobs/{job_id}")
async def get_job_details(job_id: str):
    """
    Get detailed information about an ETL job
    """
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return jobs[job_id]

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )