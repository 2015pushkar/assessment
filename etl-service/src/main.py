from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import uvicorn
import os
import pandas as pd
import logging
from datetime import datetime

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

        # timestamp parsing
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        # numeric coercion
        for col in ["value", "quality_score"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # trim string columns
        str_cols = [c for c in df.columns if df[c].dtype == "object"]
        for c in str_cols:
            df[c] = df[c].astype(str).str.strip()

        # metadata columns
        df["processed_at"] = datetime.utcnow().isoformat()
        df["job_id"] = job_id

        # update job state
        jobs[job_id]["progress"] = 30
        jobs[job_id]["message"] = "Data transformed"
        jobs[job_id]["transform_info"] = {
            "columns_after": list(df.columns),
            "row_count": len(df)
        }
        logger.info(f"Job {job_id}: transformed {len(df)} rows")

        
        # DEV: print a preview to stdout
        if os.getenv("DEVELOPMENT") == "true":
            preview_rows = 10
            logger.info(f"Job {job_id}: preview of transformed data (first {preview_rows} rows)\n{df.head(preview_rows)}")
        # -----------------------------------------------------------------

        return df
    except Exception as exc:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["message"] = f"Transform error: {exc}"
        logger.exception(f"Job {job_id}: transform failed – {exc}")
        return None




@app.post("/jobs", response_model=ETLJobResponse)
async def submit_job(job_request: ETLJobRequest):
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
    
    # TODO: Implement actual ETL processing
    # This is where the candidate would implement:
    # 1. File extraction
    df = await extract_file(job_id, job_request.filename)
    if df is None:                                    
        raise HTTPException(status_code=400, detail=jobs[job_id]["message"])
    # 2. Data transformation 
    df = await transform_data(job_id, df)
    if df is None:
        raise HTTPException(status_code=400, detail=jobs[job_id]["message"])
    # 3. Quality validation
    # 4. Database loading
    
    return ETLJobResponse(
        jobId=job_id,
        status="running",
        message="Job submitted successfully"
    )

@app.get("/jobs/{job_id}/extract", include_in_schema=False)
async def dev_extract(job_id: str):
    """DEV‑only endpoint to debug extraction. Enabled when DEVELOPMENT=true."""

    if os.getenv("DEVELOPMENT") != "true":
        raise HTTPException(status_code=404, detail="Endpoint disabled")

    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    filename = jobs[job_id]["filename"]

    df = await extract_file(job_id, filename)
    if df is None:
        raise HTTPException(status_code=400, detail=jobs[job_id]["message"])

    return {
        "jobId": job_id,
        "rows_extracted": len(df),
        "columns": list(df.columns),
        "info": jobs[job_id]["message"],
    }

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