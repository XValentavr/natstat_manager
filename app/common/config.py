import os
from typing import Optional

import aiohttp
from hypercorn import Config
from pydantic.v1 import BaseSettings


ASYNC_CONCURENT_BY_HOST = 1


hypercorn_config = Config()
hypercorn_config.bind = ["0.0.0.0:8080"]


proxy = "http://18.159.218.109:8888"
proxy_auth = aiohttp.BasicAuth("pinnacle", "123445")


class EndpointsConfig(BaseSettings):
    picks_host: str = "https://picks.octopol.io"
    sing_host: str = "https://sing-scrapper.octopol.io"
    matching_host: str = "https://matching-read-only-api.octopol.io"
    shotsy_reassess_host: str = "https://shotsy-reassess.octopol.io"
    gbt_service_host: str = "https://gbt.octopol.io"
    molly_host: str = "https://molly-proxy.octopol.io"
    shotsy_offline_odds_host: str = "https://shotsy-offline-odds.octopol.io"
    sbc_host: str = "https://sbc.octopol.io"
    shotsy_bf_host: str = "https://shotsy-bf.octopol.io"


class DBConfig(BaseSettings):
    prod_db_username: Optional[str] = None
    prod_db_password: Optional[str] = None
    prod_db_host: Optional[str] = None
    prod_db_port: Optional[int] = None
    prod_db_database: Optional[str] = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


endpoints_config = EndpointsConfig()
db_config = DBConfig()


class Settings(BaseSettings):
    IS_PROD: bool = True
    SERVICE_READY: bool = False


if os.getenv("ENV_RESTART"):
    settings = Settings()
else:
    settings = Settings(
        IS_PROD=False,
    )


print(settings)
