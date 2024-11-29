FROM ghcr.io/investigativedata/ftm-docker:latest

COPY ftmq_api /app/ftmq_api
COPY setup.py /app/setup.py
COPY pyproject.toml /app/pyproject.toml
COPY README.md /app/README.md

WORKDIR /app
RUN pip install gunicorn uvicorn
RUN pip install .

USER 1000

ENV FTM_STORE_URI=sqlite:////data/followthemoney.store
ENV CATALOG=/data/catalog.json

ENTRYPOINT ["gunicorn", "ftmq_api.api:app", "--bind", "0.0.0.0:8000", "--worker-class", "uvicorn.workers.UvicornWorker"]
