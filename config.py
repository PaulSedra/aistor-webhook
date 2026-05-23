"""Typed configuration loading for the AIStor webhook service.

This module translates the YAML configuration file into dataclasses so the rest
of the application does not depend on raw dictionaries.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class WebhookConfig:
    """Incoming webhook authentication settings."""

    token: str | None = None


@dataclass(frozen=True)
class AirflowRoute:
    """Bucket and key-prefix match rule for one Airflow DAG."""

    bucket: str
    prefix: str
    dag_id: str


@dataclass(frozen=True)
class AirflowIntegrationConfig:
    """Connection and routing settings for the Airflow integration."""

    base_url: str
    username: str
    password: str
    routes: list[AirflowRoute] = field(default_factory=list)


@dataclass(frozen=True)
class IntegrationsConfig:
    """Optional integration-specific configuration blocks."""

    airflow: AirflowIntegrationConfig | None = None


@dataclass(frozen=True)
class AppConfig:
    """Top-level application configuration."""

    webhook: WebhookConfig = field(default_factory=WebhookConfig)
    integrations: IntegrationsConfig = field(default_factory=IntegrationsConfig)


def load_config(config_path: Path | None = None) -> AppConfig:
    """Load and parse the YAML configuration file.

    Args:
        config_path: Optional path to a YAML config file. Defaults to
            `config.yaml` next to this module.

    Returns:
        Parsed application configuration.

    Raises:
        RuntimeError: The config file does not exist.
        ValueError: The config file shape is invalid.
        yaml.YAMLError: The file contains invalid YAML.
    """
    path = config_path or Path(__file__).parent / "config.yaml"

    if not path.exists():
        raise RuntimeError(f"Missing config file: {path}")

    with path.open("r", encoding="utf-8") as file:
        raw_config = yaml.safe_load(file) or {}

    return parse_config(raw_config)


def parse_config(raw_config: Any) -> AppConfig:
    """Parse a raw YAML mapping into typed application config.

    Args:
        raw_config: YAML value loaded from the config file.

    Returns:
        Parsed application configuration.

    Raises:
        ValueError: The loaded YAML value is not a valid config mapping.
    """
    config = _mapping(raw_config, "config")

    return AppConfig(
        webhook=_parse_webhook(config.get("webhook") or {}),
        integrations=_parse_integrations(config.get("integrations") or {}),
    )


def _parse_webhook(raw_webhook: Any) -> WebhookConfig:
    """Parse the optional webhook configuration block.

    Args:
        raw_webhook: Raw `webhook` YAML value.

    Returns:
        Parsed webhook configuration.

    Raises:
        ValueError: The webhook block is not a mapping or has invalid fields.
    """
    webhook = _mapping(raw_webhook, "webhook")
    token = webhook.get("token")

    if token is not None and not isinstance(token, str):
        raise ValueError("webhook.token must be a string")

    return WebhookConfig(token=token)


def _parse_integrations(raw_integrations: Any) -> IntegrationsConfig:
    """Parse optional integration configuration blocks.

    Args:
        raw_integrations: Raw `integrations` YAML value.

    Returns:
        Parsed integrations configuration.

    Raises:
        ValueError: The integrations block or one of its enabled integrations is
            invalid.
    """
    integrations = _mapping(raw_integrations, "integrations")
    airflow = integrations.get("airflow")

    return IntegrationsConfig(
        airflow=_parse_airflow(airflow) if airflow is not None else None,
    )


def _parse_airflow(raw_airflow: Any) -> AirflowIntegrationConfig:
    """Parse the Airflow integration configuration block.

    Args:
        raw_airflow: Raw `integrations.airflow` YAML value.

    Returns:
        Parsed Airflow integration configuration.

    Raises:
        ValueError: The Airflow block is not a mapping or is missing required
            string fields.
    """
    airflow = _mapping(raw_airflow, "integrations.airflow")

    return AirflowIntegrationConfig(
        base_url=_required_str(airflow, "base_url", "integrations.airflow"),
        username=_required_str(airflow, "username", "integrations.airflow"),
        password=_required_str(airflow, "password", "integrations.airflow"),
        routes=_parse_airflow_routes(airflow.get("routes") or []),
    )


def _parse_airflow_routes(raw_routes: Any) -> list[AirflowRoute]:
    """Parse Airflow route entries from the configuration.

    Args:
        raw_routes: Raw `integrations.airflow.routes` YAML value.

    Returns:
        Parsed Airflow route entries.

    Raises:
        ValueError: The route list or one of its route entries is invalid.
    """
    if not isinstance(raw_routes, list):
        raise ValueError("integrations.airflow.routes must be a list")

    routes = []

    for index, raw_route in enumerate(raw_routes):
        path = f"integrations.airflow.routes[{index}]"
        route = _mapping(raw_route, path)

        routes.append(
            AirflowRoute(
                bucket=_required_str(route, "bucket", path),
                prefix=_required_str(route, "prefix", path),
                dag_id=_required_str(route, "dag_id", path),
            )
        )

    return routes


def _mapping(value: Any, path: str) -> dict:
    """Return a mapping value or raise a path-specific validation error.

    Args:
        value: YAML value to validate.
        path: Human-readable config path used in error messages.

    Returns:
        The input value as a dictionary.

    Raises:
        ValueError: The value is not a mapping.
    """
    if not isinstance(value, dict):
        raise ValueError(f"{path} must be a mapping")

    return value


def _required_str(mapping: dict, key: str, path: str) -> str:
    """Return a required string field or raise a validation error.

    Args:
        mapping: Config mapping that should contain the field.
        key: Field name to read from the mapping.
        path: Human-readable parent config path used in error messages.

    Returns:
        The non-empty string field value.

    Raises:
        ValueError: The field is missing, empty, or not a string.
    """
    value = mapping.get(key)

    if not isinstance(value, str) or not value:
        raise ValueError(f"{path}.{key} must be a non-empty string")

    return value
