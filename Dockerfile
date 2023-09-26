FROM ghcr.io/investigativedata/ftm-docker:main

COPY ftmstore_fastapi /app/ftmstore_fastapi
COPY setup.py /app/setup.py
COPY setup.cfg /app/setup.cfg
COPY pyproject.toml /app/pyproject.toml
COPY VERSION /app/VERSION
COPY README.md /app/README.md

WORKDIR /app
RUN pip install gunicorn uvicorn
RUN pip install .

USER 1000

ENV FTM_STORE_URI=sqlite:////data/followthemoney.store
ENV CATALOG=/data/catalog.json

CMD ["uvicorn", "ftmstore_fastapi.api:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8000"]
ENTRYPOINT ["gunicorn", "ftmstore_fastapi.api:app", "--bind", "0.0.0.0:8000", "--worker-class", "uvicorn.workers.UvicornWorker"]
