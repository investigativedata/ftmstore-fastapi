export LOG_LEVEL ?= info
export COMPOSE ?= docker-compose.yml
export FTM_STORE_URI = sqlite:///followthemoney.store

api: followthemoney.store
	CATALOG=./tests/fixtures/catalog.json DEBUG=1 uvicorn ftmstore_fastapi.api:app --reload --port 5000

followthemoney.store:
	poetry run ftmq --store-dataset ec_meetings -i ./tests/fixtures/ec_meetings.ftm.json -o $(FTM_STORE_URI)
	poetry run ftmq --store-dataset eu_authorities -i ./tests/fixtures/eu_authorities.ftm.json -o $(FTM_STORE_URI)
	poetry run ftmq --store-dataset gdho -i ./tests/fixtures/gdho.ftm.json -o $(FTM_STORE_URI)

test: followthemoney.store
	poetry run pytest -s --cov=ftmstore_fastapi --cov-report term-missing

typecheck:
	# pip install types-python-jose
	# pip install types-passlib
	# pip install pandas-stubs
	poetry run mypy ftmstore_fastapi

lint:
	poetry run flake8 ftmstore_fastapi --count --select=E9,F63,F7,F82 --show-source --statistics
	poetry run flake8 ftmstore_fastapi --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

docker:
	docker-compose -f $(COMPOSE) up -d

redis:
	docker run -p 6379:6379 redis

clean:
	rm -rf followthemoney.store
