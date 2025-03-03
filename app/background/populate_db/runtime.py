import asyncio

from app.common.logging import get_logger
from .scrapper import (
    populate_data,
    fetch_and_save_missing_games,
    fetch_and_save_seasons,
    fetch_v3_games_data,
)


def log(msg: str):
    get_logger().info(msg)


def run_populate_data():
    asyncio.run(populate_data())


def run_missing_games():
    asyncio.run(fetch_and_save_missing_games())


def run_fetch_and_save_seasons():
    asyncio.run(fetch_and_save_seasons())


def run_fetch_v3_games_data():
    asyncio.run(fetch_v3_games_data())
