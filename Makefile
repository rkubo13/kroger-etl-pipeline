.PHONY: install generate test lint run docker-up docker-down clean

install:
	pip install -r requirements.txt

generate:
	python scripts/generate_data.py

test:
	PYTHONPATH=. pytest tests/ -v

lint:
	python -m py_compile etl/pipeline.py
	python -m py_compile etl/extractors/csv_extractor.py
	python -m py_compile etl/transformers/clean.py
	python -m py_compile etl/transformers/enrich.py
	python -m py_compile etl/transformers/aggregate.py
	python -m py_compile etl/loaders/warehouse_loader.py

run: generate
	PYTHONPATH=. python -m etl.pipeline

docker-up:
	docker-compose up --build -d

docker-down:
	docker-compose down

docker-run:
	docker-compose --profile cli run pipeline

clean:
	rm -rf data/raw/*.csv data/processed/* data/warehouse/*.duckdb
	find . -type d -name __pycache__ -exec rm -rf {} +
