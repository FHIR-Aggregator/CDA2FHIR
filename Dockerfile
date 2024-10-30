FROM python:3.12

WORKDIR /app

COPY . /app/CDA2FHIR

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip && \
    pip install --no-cache-dir charset_normalizer idna certifi requests pydantic pytest click \
    pathlib orjson tqdm uuid openpyxl pandas inflection iteration_utilities fhir.resources==7.1.0 \
    sqlalchemy==2.0.31 gen3-tracker>=0.0.7rc1

RUN pip install -e /app/CDA2FHIR

ENTRYPOINT ["/bin/bash"]
