#!/bin/bash
# Airflow's metadata and the pipeline's warehouse are separate databases on the
# same server. Sharing one is how an `airflow db reset` takes the warehouse with
# it, and how a metadata migration locks tables a load is writing to.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    CREATE DATABASE airflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO $POSTGRES_USER;
SQL
