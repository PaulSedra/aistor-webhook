"""Airflow integration handler for AIStor object events.

This module maps AIStor bucket events to Airflow DAG runs using the configured
bucket and key-prefix routes.
"""

import datetime as dt
import logging
import uuid

import requests

from config import AirflowIntegrationConfig

logger = logging.getLogger("aistor_airflow_handler")


class AirflowEventHandler:
    """Triggers Airflow DAG runs for matching AIStor event routes."""

    def __init__(self, config: AirflowIntegrationConfig):
        """Initialize the handler from typed Airflow integration config.

        Args:
            config: Airflow connection and routing configuration.
        """
        self.base_url = config.base_url.rstrip("/")
        self.username = config.username
        self.password = config.password
        self.routes = config.routes

        logger.info("Loaded Airflow handler with %s route(s)", len(self.routes))

    def handle_record(self, bucket: str, key: str, event_name: str) -> list[dict]:
        """Trigger Airflow routes that match one AIStor event record.

        Args:
            bucket: AIStor bucket name from the event record.
            key: URL-decoded object key from the event record.
            event_name: AIStor event name from the event record.

        Returns:
            Metadata for each Airflow DAG run triggered by the record.

        Raises:
            requests.RequestException: Airflow authentication or DAG run creation
                failed.
        """
        triggered = []

        for route in self.routes:
            if bucket != route.bucket or not key.startswith(route.prefix):
                continue

            dag_id = route.dag_id

            logger.info(
                "Matched Airflow route bucket=%s prefix=%s dag_id=%s",
                route.bucket,
                route.prefix,
                dag_id,
            )

            self.trigger_dag(dag_id, bucket, key, event_name)

            logger.info("Triggered Airflow DAG dag_id=%s for key=%s", dag_id, key)

            triggered.append(
                {
                    "integration": "airflow",
                    "dag_id": dag_id,
                    "bucket": bucket,
                    "key": key,
                }
            )

        if not triggered:
            logger.info("No Airflow route matched bucket=%s key=%s", bucket, key)

        return triggered

    def get_token(self) -> str:
        """Request an Airflow access token for the configured user.

        Returns:
            Airflow access token.

        Raises:
            requests.RequestException: Airflow rejected the token request or the
                request failed.
            KeyError: Airflow returned a successful response without an
                `access_token` field.
        """
        response = requests.post(
            f"{self.base_url}/auth/token",
            json={
                "username": self.username,
                "password": self.password,
            },
            timeout=10,
        )

        response.raise_for_status()
        return response.json()["access_token"]

    def trigger_dag(
        self,
        dag_id: str,
        bucket: str,
        key: str,
        event_name: str,
    ) -> None:
        """Create an Airflow DAG run containing the AIStor event details.

        Args:
            dag_id: Airflow DAG ID to trigger.
            bucket: AIStor bucket name from the event record.
            key: URL-decoded object key from the event record.
            event_name: AIStor event name from the event record.

        Raises:
            requests.RequestException: Airflow rejected the DAG run request or the
                request failed.
            KeyError: Airflow returned a successful token response without an
                `access_token` field.
        """
        token = self.get_token()
        now = dt.datetime.now(dt.timezone.utc)

        response = requests.post(
            f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={
                "dag_run_id": (
                    f"aistor__{now.strftime('%Y%m%dT%H%M%S')}__"
                    f"{uuid.uuid4().hex[:8]}"
                ),
                "logical_date": now.isoformat(),
                "conf": {
                    "bucket": bucket,
                    "key": key,
                    "event_name": event_name,
                },
            },
            timeout=10,
        )

        response.raise_for_status()
