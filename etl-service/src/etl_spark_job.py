import sys
import os
from pyspark.sql import SparkSession, DataFrame
import psycopg2
from datetime import datetime
from logger import logger
from helper import update_etl_job_status, get_db_cursor, make_row_id, upsert_measurement_aggs
from pyspark.sql.functions import trim, to_timestamp, col, to_date, count, avg, min, max, when, udf
from pyspark.sql.types import StringType

# register it
id_udf = udf(make_row_id, StringType())

def spark_extract(spark, job_id, file_path):
    if not os.path.exists(file_path):
        update_etl_job_status(job_id, "failed", message=f"File not found: {file_path}")
        sys.exit(1)
    df = spark.read.option("header", True).csv(file_path)
    count = df.count()
    update_etl_job_status(job_id, "running", progress=10, message=f"Extracted {count} rows")
    return df

def spark_transform(job_id, df):
    df2 = (
      df
      .withColumn("study_id"     , trim(col("study_id")))
      .withColumn("participant_id", trim(col("participant_id")))
      .withColumn("timestamp"    , to_timestamp(col("timestamp")))
      .withColumn("quality_score", col("quality_score").cast("double"))
      # clip quality_score between 0 and 1:
      .withColumn("quality_score", 
          when(col("quality_score")<0, 0.)
          .when(col("quality_score")>1, 1.)
          .otherwise(col("quality_score"))
      )
      .withColumn(
            "id",
            id_udf(
                col("study_id"),
                col("participant_id"),
                col("timestamp"),
                col("measurement_type"),
                col("value")
            )
    )
    )
    update_etl_job_status(job_id, "running", progress=30, message="Data transformed")
    return df2


VALID_MEASUREMENTS = ["glucose", "cholesterol", "weight", "height", "blood_pressure", "heart_rate"]
def spark_validate(job_id, df):
    missing   = [c for c in ["study_id","participant_id",'measurement_type',"value","timestamp"] if c not in df.columns]
    if missing:
      update_etl_job_status(job_id, "failed", message=f"Missing cols: {missing}")
      sys.exit(1)

    badTypes = df.filter(~col("measurement_type").isin(VALID_MEASUREMENTS)).count()
    if badTypes>0:
      update_etl_job_status(job_id, "failed", message="Invalid measurement types present")
      sys.exit(1)

    update_etl_job_status(job_id, "running", progress=50, message="Validation passed")
    return df


def spark_load(job_id: str, df):
    """
    Write the transformed DataFrame into Postgres via Spark’s JDBC sink.
    """
    # Convert DATABASE_URL to JDBC format
    # e.g. postgresql://user:pass@postgres:5432/clinical_data
    # → jdbc:postgresql://postgres:5432/clinical_data
    jdbc_url = os.getenv("DATABASE_URL") \
        .replace("postgresql://", "jdbc:postgresql://")

    # Credentials
    user = os.getenv("POSTGRES_USER", "")
    pwd  = os.getenv("POSTGRES_PASSWORD", "")

    # 1) Bulk-append into the fact table in truly distributed fashion
    (
      df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "clinical_measurements")
        .option("driver", "org.postgresql.Driver")
        .option("user", user)
        .option("password", pwd)
        .mode("append")
        .save()
    )
    update_etl_job_status(job_id,
                          "running",
                          progress=90,
                          message="Bulk write to clinical_measurements complete")

    upsert_dimensions(job_id, df)
    update_etl_job_status(job_id, "running", progress=95, message="All tables loaded")


def upsert_dimensions_and_aggregations(job_id: str, df: DataFrame):
    """
    1) Upsert dimension tables (studies, participants, sites, enrollments)
    2) Bulk‐collect the 'id' column and call the existing upsert_measurement_aggs()
    """
    # ─── 1) Dimension upserts ────────────────────────────────────────────────

    # a) Collect distinct keys
    study_ids       = [row.study_id       for row in df.select("study_id").distinct().collect()]
    participant_ids = [row.participant_id for row in df.select("participant_id").distinct().collect()]
    site_ids        = [row.site_id        for row in df.select("site_id").distinct().collect()]

    # b) Compute first_seen per (participant_id, study_id)
    enroll_df = (
        df
        .groupBy("participant_id", "study_id")
        .agg({"timestamp": "min"})
        .withColumnRenamed("min(timestamp)", "enrolled_at")
    )
    enrollments = enroll_df.collect()  # list of Rows with fields: participant_id, study_id, enrolled_at

    # c) Run upsert SQL
    conn, cur = get_db_cursor()
    try:
        # Studies
        for sid in study_ids:
            cur.execute(
                "INSERT INTO studies(study_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (sid,)
            )

        # Participants
        for pid in participant_ids:
            cur.execute(
                "INSERT INTO participants(participant_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (pid,)
            )

        # Sites
        for st in site_ids:
            cur.execute(
                "INSERT INTO sites(site_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (st,)
            )

        # Enrollments
        for row in enrollments:
            cur.execute(
                """
                INSERT INTO participant_enrollments(participant_id, study_id, enrolled_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (participant_id, study_id) DO UPDATE
                  SET enrolled_at = LEAST(participant_enrollments.enrolled_at, EXCLUDED.enrolled_at)
                """,
                (row.participant_id, row.study_id, row.enrolled_at)
            )

        conn.commit()
        update_etl_job_status(job_id, "running", progress=92, message="Dimensions upserted")

    finally:
        cur.close()
        conn.close()

    # ─── 2) Measurement aggregations ─────────────────────────────────────────

    # Collect newly written IDs back on the driver
    inserted_ids = [r.id for r in df.select("id").collect()]

    # Call your existing pure-Python helper
    upsert_measurement_aggs(inserted_ids)

    update_etl_job_status(
        job_id,
        "running",
        progress=98,
        message="Measurement aggregations upserted"
    )


def main():
    spark    = SparkSession.builder.appName("ClinicalDataETL").getOrCreate()
    file, job_id = sys.argv[1], sys.argv[2]

    try:
        df = spark_extract(spark, job_id, file)
        df = spark_transform(job_id, df)
        df = spark_validate(job_id, df)
        spark_load(job_id, df)
        update_etl_job_status(job_id, "completed", progress=100, message="ETL succeeded")
    except Exception as e:
        logger.exception(f"Job {job_id} failed")
        update_etl_job_status(job_id, "failed", message=str(e))
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

