FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt

COPY aurora_data/ ./aurora_data/

RUN pip install --no-cache-dir -e .

ENTRYPOINT ["aurora-data"]
