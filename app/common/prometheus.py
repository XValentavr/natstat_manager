from typing import Any

import prometheus_client
from fastapi import FastAPI
from prometheus_client import Counter, Gauge, Histogram
from prometheus_fastapi_instrumentator import Instrumentator, metrics


cron_job_last_success = Gauge(
    "cron_job_last_success_timestamp",
    "Timestamp of last successful cron job execution",
    ["job_name"],
)
cron_job_last_failure = Gauge(
    "cron_job_last_failure_timestamp", "Timestamp of last failed cron job execution", ["job_name"]
)
cron_job_execution_count = Counter(
    "cron_job_execution_count", "Number of cron job executions", ["job_name", "status"]
)

games_by_type_gauge = Gauge(
    "storage_games_by_type_count", "Number of games by type in inmemory storage", ["type"]
)
storage_size_gauge = Gauge("storage_size_bytes", "Size of the in-memory storage")

background_event_loop_uptime = Counter(
    "background_event_loop_uptime_seconds", "Uptime of background event loops", ["loop_name"]
)
background_event_loop_last_heartbeat = Gauge(
    "background_event_loop_last_heartbeat_timestamp",
    "Timestamp of the last heartbeat from background event loops",
    ["loop_name"],
)

client_requests = Counter("client_requests_total", "Number of client requests", ["endpoint"])
client_request_duration = Histogram(
    "client_request_duration_seconds", "Duration of client requests", ["endpoint"]
)

_original_generate_latest = prometheus_client.generate_latest


def init_prometheus(fastapi_app: FastAPI, namespace: str) -> Instrumentator:
    def new_generate_latest(*args: Any, **kwargs: Any) -> bytes:
        original_output = _original_generate_latest(*args, **kwargs).decode("utf-8")
        namespace_prefix = f"{namespace}_"
        new_output = ""

        for line in original_output.splitlines():
            if not line.startswith("#") and not line.startswith(namespace_prefix):
                new_output += namespace_prefix

            new_output += line + "\n"

        output = new_output.encode("utf-8")

        return output

    prometheus_client.generate_latest = new_generate_latest

    instrumentator = Instrumentator()
    instrumentator.add(metrics.default(metric_namespace=namespace))
    instrumentator.instrument(fastapi_app).expose(fastapi_app)

    return instrumentator
