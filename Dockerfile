FROM ghcr.io/investigativedata/ftm-docker:main

COPY ftmstore_fastapi /app/ftmstore_fastapi
COPY setup.py /app/setup.py
COPY setup.cfg /app/setup.cfg
COPY VERSION /app/VERSION
COPY README.md /app/README.md

WORKDIR /app
RUN pip install gunicorn uvicorn
RUN pip install .

# Run a single unicorn, scale on container level
CMD ["uvicorn", "ftmstore_fastapi.api:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8000"]
