import sentry_sdk

from app.common.config import settings
from app.common.logging import log


def initialize_sentry(ingest_url, production_only: bool):
    if not production_only or settings.IS_PROD:
        log("initializing sentry")
        sentry_sdk.init(
            ingest_url,
            # Set traces_sample_rate to 1.0 to capture 100%
            # of transactions for performance monitoring.
            # We recommend adjusting this value in production,
            traces_sample_rate=0,
            profiles_sample_rate=0,
            environment="production" if settings.IS_PROD else "local",
        )
