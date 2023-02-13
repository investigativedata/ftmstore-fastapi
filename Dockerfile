FROM python:3-bullseye

RUN apt-get update && apt-get -y upgrade

COPY ftmstore_fastapi /app/ftmstore_fastapi
COPY setup.py /app/setup.py
COPY setup.cfg /app/setup.cfg
COPY VERSION /app/VERSION
COPY README.md /app/README.md

WORKDIR /app
RUN pip install -U pip setuptools
RUN pip install gunicorn uvicorn
RUN pip install followthemoney-store nomenklatura pyicu # build caching
RUN pip install -e .

# Run a single unicorn, scale on container level
CMD ["uvicorn", "ftmstore_fastapi.api:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8000"]
