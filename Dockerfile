FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt

COPY rcd_data/ ./rcd_data/

RUN pip install --no-cache-dir -e .

ENTRYPOINT ["rcd-data"]
