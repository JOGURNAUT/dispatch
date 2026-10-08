FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# The slim list, not the dev one -- see requirements-airflow.txt for why.
# Installing the full set failed here: dbt-postgres pulls psycopg2 (the source
# build, not -binary), which needs pg_config from libpq-dev. The fix was not to
# add libpq-dev but to stop installing packages this image never imports.
# With nothing left that compiles, build-essential came out of the layer above.
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

ENV PYTHONPATH=/opt/dispatch
WORKDIR /opt/dispatch
