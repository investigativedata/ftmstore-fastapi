export LOG_LEVEL ?= info
export COMPOSE ?= docker-compose.yml

all: install api

install:
	pip install -e .


api: testdata  # for developement
	CATALOG=./tests/fixtures/catalog.json DEBUG=1 uvicorn ftmstore_fastapi.api:app --reload


install.dev:
	pip install coverage nose moto pytest pytest-cov black flake8 isort ipdb mypy bump2version

testdata: clean
	ftm store write -d ec_meetings -i ./tests/fixtures/ec_meetings.ftm.json
	ftm store write -d eu_authorities -i ./tests/fixtures/eu_authorities.ftm.json
	ftm store write -d gdho -i ./tests/fixtures/gdho.ftm.json

test: install.dev testdata
	pip install types-python-jose
	pip install types-passlib
	pip install pandas-stubs
	pytest -s --cov=ftmstore_fastapi --cov-report term-missing
	mypy ftmstore_fastapi


docker:
	docker-compose -f $(COMPOSE) up -d

redis:
	docker run -p 6379:6379 redis

clean:
	rm -rf followthemoney.store
