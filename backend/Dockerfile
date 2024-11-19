FROM python:3.12-slim
WORKDIR /app
COPY . /app/

# Install PostgreSQL development libraries
RUN apt-get update && \
    apt-get install -y libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip
RUN pip install --no-cache-dir pycryptodome python-binance psycopg2 python-dotenv fastapi uvicorn psycopg2-binary
CMD ["uvicorn", "main:app --reload --host 0.0.0.0 --port 8000"]