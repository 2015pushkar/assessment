import os
import psycopg2
from datetime import datetime
from logger import logger
from psycopg2 import OperationalError, DatabaseError
from typing import Tuple
from psycopg2.extras import execute_values



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


def get_db_cursor() -> Tuple[psycopg2.extensions.connection, psycopg2.extensions.cursor]:
    """
    New psycopg2 connection using DATABASE_URL env var,
    and return (conn, cur).
    Raises RuntimeError if DATABASE_URL is not set,
    OperationalError/DatabaseError on connection issues.
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL environment variable is not set")
        raise RuntimeError("DATABASE_URL is required to connect to the database")

    try:
        conn = psycopg2.connect(database_url)
        cur  = conn.cursor()
        return conn, cur

    except OperationalError as oe:
        logger.exception("Could not connect to the database")
        raise

    except DatabaseError as de:
        logger.exception("Database error occurred")
        # make sure we close if partially opened
        try:
            conn.close()
        except Exception:
            pass
        raise

def make_row_id(study_id, participant_id, timestamp, measurement_type, value):
    row = {
        "study_id": study_id,
        "participant_id": participant_id,
        "timestamp": timestamp,
        "measurement_type": measurement_type,
        "value": value
    }
    return make_deterministic_id(row)




def upsert_measurement_aggs(inserted_ids: list[str]) -> None:
    """
    Collapse the freshly-inserted clinical_measurements rows (identified
    by their UUIDs) into daily buckets and merge them into the
    measurement_aggregations table.
    """
    if not inserted_ids:
        return

    # Use your helper to open the connection
    conn, cur = get_db_cursor()
    try:
        # 1️⃣ collapse only the fresh rows into aggregated rows
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

        # 2️⃣ merge into measurement_aggregations
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

        conn.commit()
        update_etl_job_status(
            job_id,
            "running",
            progress=98,
            message="Measurement aggregations upserted"
        )
    finally:
        cur.close()
        conn.close()
