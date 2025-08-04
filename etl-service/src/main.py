from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import os
from rq import Queue
from redis_conn import redis_client
import psycopg2
import uvicorn
from typing import Optional, Dict, Any
from etl_logic import update_etl_job_status
from logger import logger

app = FastAPI(title="Clinical Data ETL Service", version="1.0.0")
# Initialize Redis Queue globally
q = Queue('etl_jobs', connection=redis_client)

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


@app.post("/jobs", response_model=ETLJobResponse)
async def submit_job(job_request: ETLJobRequest):
    """
    Submit a new ETL job for processing
    """
    job_id = job_request.jobId

    # Enqueue job instead of background tasks
    q.enqueue("etl_worker.process_etl_job", job_id, job_request.filename, job_request.studyId)
    
    update_etl_job_status(job_id, "queued", progress=0, message="Job queued for processing")
    
    return ETLJobResponse(jobId=job_id, status="queued", message="Job queued successfully")


@app.get("/jobs/{job_id}/status", response_model=ETLJobStatus)
async def get_job_status(job_id: str):
    database_url = os.getenv('DATABASE_URL')
    conn = psycopg2.connect(database_url)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, status, progress, message FROM etl_jobs WHERE id = %s
    """, (job_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    return ETLJobStatus(
        jobId=row[0],
        status=row[1],
        progress=row[2],
        message=row[3]
    )


@app.get("/jobs/{job_id}")
async def get_job_details(job_id: str):
    database_url = os.getenv('DATABASE_URL')
    conn = psycopg2.connect(database_url)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, filename, study_id, status, progress, message, created_at, updated_at, completed_at
        FROM etl_jobs WHERE id = %s
    """, (job_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "jobId": row[0],
        "filename": row[1],
        "studyId": row[2],
        "status": row[3],
        "progress": row[4],
        "message": row[5],
        "createdAt": row[6],
        "updatedAt": row[7],
        "completedAt": row[8],
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )