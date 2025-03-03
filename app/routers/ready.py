from fastapi import APIRouter
from starlette.responses import Response

from app.common.config import settings
from app.common.logging import get_logger

router = APIRouter(tags=["Ready"])


@router.get("/ready/")
def get_ready_status() -> Response:
    if not settings.SERVICE_READY:
        get_logger().info("get_ready_status, service is not ready, return 503")
        return Response(status_code=503)

    get_logger().info("get_ready_status, service is ready, return 200")
    return Response(status_code=200)
