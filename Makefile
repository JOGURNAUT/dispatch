.PHONY: help demo test lint clean up down marts prove

help:
	@echo "demo   generate events and run the pipeline end to end (no Docker, ~2s)"
	@echo "test   run the test suite"
	@echo "prove  show the logic runs with no engine installed"
	@echo "marts  build and print the dbt marts against the local warehouse"
	@echo "up     bring up Kafka, Spark, Postgres and Airflow"

demo:
	python -m generator.produce --trips 5000 --days 14 --out data/raw/events.jsonl
	python -m transforms.run_pipeline all --source data/raw/events.jsonl --as-of 2026-09-16T00:00:00

test:
	python -m pytest tests/ -q

prove:
	python scripts/prove_separation.py

lint:
	ruff check dispatch transforms generator streaming dags

marts:
	cd dbt && dbt build --target local

clean:
	rm -rf data/bronze data/silver data/raw data/warehouse.db data/_checkpoints

up:
	docker compose up -d
	@echo "airflow http://localhost:8080 (airflow/airflow)  spark http://localhost:8081"

down:
	docker compose down -v
