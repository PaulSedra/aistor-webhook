# AIStor Webhook

FastAPI service that receives AIStor bucket event notifications and dispatches matching events to configured integrations.

The current integration triggers Airflow DAG runs when an AIStor object event matches a configured bucket and object-key prefix.

## What It Does

The service exposes one webhook endpoint:

```text
POST /aistor/events
```

For each AIStor event record it receives, the service:

1. Reads the bucket name, object key, and event name.
2. Sends the record to each configured integration handler.
3. For the Airflow integration, compares the bucket and object key against configured routes.
4. Authenticates to Airflow.
5. Creates a DAG run for each matching Airflow route.
6. Passes the bucket, key, and event name to the DAG run `conf`.

## Project Files

```text
.
├── Dockerfile
├── airflow_handler.py
├── config.py
├── config.yaml.example
├── main.py
├── README.md
└── requirements.txt
```

The app requires a runtime `config.yaml` file next to `main.py`. `config.yaml` should contain environment-specific settings and credentials, so it is ignored by git. Use `config.yaml.example` as the public template.

## Configuration

Create a local config from the example:

```powershell
Copy-Item config.yaml.example config.yaml
```

### Configuration Reference

| Field                                  | Required                     | Description                                                                                    |
| -------------------------------------- | ---------------------------- | ---------------------------------------------------------------------------------------------- |
| `webhook.token`                        | No                           | Optional shared bearer token for incoming webhook requests.                                    |
| `integrations.airflow`                 | No                           | Enables the Airflow integration when present.                                                  |
| `integrations.airflow.base_url`        | Yes, when Airflow is enabled | Base URL for the Airflow API. The app calls `/auth/token` and `/api/v2/dags/{dag_id}/dagRuns`. |
| `integrations.airflow.username`        | Yes, when Airflow is enabled | Airflow username used to request an access token.                                              |
| `integrations.airflow.password`        | Yes, when Airflow is enabled | Airflow password used to request an access token.                                              |
| `integrations.airflow.routes`          | No                           | Airflow route list. If omitted, no Airflow DAGs are triggered.                                 |
| `integrations.airflow.routes[].bucket` | Yes, per route               | Bucket name to match.                                                                          |
| `integrations.airflow.routes[].prefix` | Yes, per route               | Object-key prefix to match.                                                                    |
| `integrations.airflow.routes[].dag_id` | Yes, per route               | Airflow DAG ID to trigger when the route matches.                                              |

If `webhook.token` is set, requests must include:

```text
Authorization: Bearer shared-webhook-token
```

The Airflow user must have these permissions:

- `can create` on `DAG Runs`
- `can edit` on `DAGs`

## Local Development

Create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
pip install -r requirements.txt
```

Create `config.yaml`, then run the service:

```powershell
uvicorn main:aistor_webhook --host 0.0.0.0 --port 30380 --reload
```

The webhook will be available at:

```text
http://localhost:30380/aistor/events
```

## Docker

Build the image:

```powershell
docker build -t local/aistor-webhook:latest .
```

Run the container with a mounted config file:

```powershell
docker run --rm `
  -p 30380:30380 `
  -v "${PWD}\config.yaml:/app/config.yaml:ro" `
  local/aistor-webhook:latest
```

## AIStor Setup

Configure the AIStor bucket notification target to send events to:

```text
http://<webhook-host>:30380/aistor/events
```

If `webhook.token` is configured, set the notification target to include the matching `Authorization: Bearer ...` header.

## Operational Notes

- `config.yaml` is loaded at application startup. Restart the service after changing integrations, routes, or credentials.
- Object keys are URL-decoded before route matching.
- An Airflow route matches when the bucket is equal and the object key starts with the configured prefix.
- If no route matches, the service logs the event and returns success without triggering a DAG.
- If Airflow authentication or DAG triggering fails, the request fails and the error is logged.
