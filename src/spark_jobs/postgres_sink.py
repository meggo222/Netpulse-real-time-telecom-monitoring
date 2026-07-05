"""
PostgreSQL Sink — shared helper for writing streaming micro-batches to
PostgreSQL via JDBC, used inside `foreachBatch` by both the raw
ingestion job and the alert detection job.

Credentials are read from environment variables (injected by
docker-compose from the project's .env file) rather than hardcoded,
so this file is safe to commit to a public GitHub repo.
"""

import os

DB_HOST = os.environ.get("APP_DB_HOST", "postgres_app")
DB_PORT = os.environ.get("APP_DB_PORT", "5432")
DB_NAME = os.environ.get("APP_DB_NAME", "telecom_monitoring")
DB_USER = os.environ.get("APP_DB_USER", "telecom_admin")
DB_PASSWORD = os.environ.get("APP_DB_PASSWORD", "")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"


def write_batch_to_postgres(batch_df, epoch_id, table_name: str):
    """
    Writes one streaming micro-batch DataFrame to a PostgreSQL table.
    Intended to be wrapped with functools.partial to bind `table_name`
    before passing to `.foreachBatch(...)`, e.g.:

        from functools import partial
        query = df.writeStream.foreachBatch(
            partial(write_batch_to_postgres, table_name="alerts")
        ).start()
    """
    if batch_df.rdd.isEmpty():
        return  # nothing to write this cycle — avoids an unnecessary JDBC round-trip

    (
        batch_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
