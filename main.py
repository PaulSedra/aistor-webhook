"""AIStor webhook application entry point.

This module owns the FastAPI app, request authentication, AIStor event parsing,
and dispatch to configured integration handlers.
"""

import logging
from urllib.parse import unquote_plus

import requests
from fastapi import FastAPI, HTTPException, Request

from airflow_handler import AirflowEventHandler
from config import AppConfig, load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger("aistor_webhook")

aistor_webhook = FastAPI()


def load_event_handlers(config: AppConfig) -> list:
    """Build integration handlers from the loaded application config.

    Args:
        config: Parsed application configuration.

    Returns:
        Integration handler instances enabled by the configuration.
    """
    event_handlers = []

    airflow_config = config.integrations.airflow
    if airflow_config:
        event_handlers.append(AirflowEventHandler(airflow_config))

    logger.info("Loaded %s integration handler(s)", len(event_handlers))
    return event_handlers


config = load_config()
WEBHOOK_TOKEN = config.webhook.token
event_handlers = load_event_handlers(config)


@aistor_webhook.post("/aistor/events")
async def aistor_event(request: Request):
    """Receive AIStor event notifications and dispatch each record.

    Args:
        request: Incoming FastAPI request containing an AIStor event payload.

    Returns:
        A response containing all integration actions triggered by the received
        records.

    Raises:
        HTTPException: The request does not include the configured bearer token.
        requests.RequestException: A configured integration failed while handling
            the event.
    """
    if WEBHOOK_TOKEN:
        auth = request.headers.get("Authorization")

        if auth != f"Bearer {WEBHOOK_TOKEN}":
            logger.warning("Rejected request with invalid token")
            raise HTTPException(status_code=401, detail="Invalid token")

    event = await request.json()
    records = event.get("Records", [])

    logger.info("Received AIStor event with %s record(s)", len(records))

    triggered = []

    for record in records:
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        event_name = record["eventName"]

        logger.info("Processing event=%s bucket=%s key=%s", event_name, bucket, key)

        try:
            for event_handler in event_handlers:
                triggered.extend(event_handler.handle_record(bucket, key, event_name))

        except requests.RequestException:
            logger.exception(
                "Failed to handle AIStor event bucket=%s key=%s",
                bucket,
                key,
            )
            raise

    return {
        "ok": True,
        "triggered": triggered,
    }
