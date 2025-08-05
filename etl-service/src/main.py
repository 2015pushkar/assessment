from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import subprocess
import uvicorn
import os
import psycopg2
from datetime import datetime
from logger import logger
from helper import update_etl_job_status


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

@app.post("/jobs", response_model=ETLJobResponse)
async def submit_job(job_request: ETLJobRequest):
    """
    Submit a new ETL job for processing
    """
    job_id = job_request.jobId
    filename = job_request.filename
    
    # Store job in memory (simplified for demo)
    jobs[job_id] = {
        "jobId": job_id,
        "filename": job_request.filename,
        "studyId": job_request.studyId,
        "status": "running",
        "progress": 0,
        "message": "Spark ETL Job submitted",
        
    }
    
    # Update database
    update_etl_job_status(job_id, "submitted", progress=0, message="Spark job submitted via API")

     # Fire-and-forget spark-submit
    filepath = f"/data/{filename}"
    spark_master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    cmd = [
        "spark-submit",
        "--packages", "org.postgresql:postgresql:42.7.3",
        "--master", spark_master,
        "/app/src/etl_spark_job.py",
        filepath,
        job_id
    ]
    subprocess.Popen(cmd) # non-blocking
    logger.info(f"Launched Spark job for {filename}")

    
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