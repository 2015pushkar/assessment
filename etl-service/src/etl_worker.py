import os
from redis_conn import redis_client
from rq import Worker, Queue, Connection
import asyncio
from etl_logic import (
    extract_file,
    transform_data,
    validate_data,
    load_data,
    update_etl_job_status
)
from logger import logger

async def process_etl_job(job_id: str, filename: str, study_id: str = None):
    logger.info(f"Worker started job {job_id}")

    update_etl_job_status(job_id, "running", progress=0, message="Job processing started")

    if os.getenv('DEVELOPMENT'):
        await asyncio.sleep(10)
    
    df = await extract_file(job_id, filename)
    if df is None:
        update_etl_job_status(job_id, "failed", message="File extraction failed")
        return

    if os.getenv('DEVELOPMENT'):
        await asyncio.sleep(10)
    
    df = await transform_data(job_id, df)
    if df is None:
        update_etl_job_status(job_id, "failed", message="Data transformation failed")
        return

    if os.getenv('DEVELOPMENT'):
        await asyncio.sleep(10)
    
    ok = await validate_data(job_id, df)
    if not ok:
        update_etl_job_status(job_id, "failed", message="Validation failed")
        return

    if os.getenv('DEVELOPMENT'):
        await asyncio.sleep(10)
    
    try:
        load_data(job_id, df)
    except Exception as e:
        logger.exception(f"Job {job_id} failed during DB load")
        update_etl_job_status(job_id, "failed", message=str(e))
        return
    
    update_etl_job_status(job_id, "completed", progress=100, message="Job completed successfully")
    logger.info(f"Job {job_id}: completed successfully")

# RQ Worker
if __name__ == "__main__":
    with Connection(redis_client):
        worker = Worker(['etl_jobs'])
        worker.work()
