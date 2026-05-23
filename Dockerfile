# Container image for the AIStor webhook FastAPI service.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY airflow_handler.py .
COPY config.py .

EXPOSE 30380

CMD ["uvicorn", "main:aistor_webhook", "--host", "0.0.0.0", "--port", "30380"]
