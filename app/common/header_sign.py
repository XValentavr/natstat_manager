from typing import Optional

from app.common.config import settings

SERVICE_HEADER = {"Agent-Details": f"Broker service. {'PROD' if settings.IS_PROD else 'LOCAL'}"}


def add_agent_details(headers: Optional[dict], source: Optional[str]) -> dict:
    """
    the logic add an Agent-Details
    """
    if headers is None:
        headers = SERVICE_HEADER

    if source:
        headers["Agent-Details"] += f" - {source}"

    return headers
