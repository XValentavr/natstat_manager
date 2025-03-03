import asyncio
import os
import datetime
import json
import hashlib
from typing import Coroutine, Any

from pytz import timezone, utc
import pytz
import jmespath


ny_tz = timezone("America/New_York")


def abs_path(path):
    """
    use it to get absolute path from relative path
    like
    with open(abs_path("app/scraper/data/get_prices.json"), "r") as f:
    :param path:
    :return:
    """
    # do 2 steps up, because we are in app/common/utils.py
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", path))


def get_london_now() -> datetime.datetime:
    """

    :rtype: object
    """
    # get london tz
    london_tz = pytz.timezone("Europe/London")
    now = datetime.datetime.now(london_tz)
    # delete tzinfo
    now = now.replace(tzinfo=None)
    return now


def get_utc_now() -> datetime.datetime:
    """

    :rtype: object
    """
    # get london tz
    utc_tz = pytz.timezone("UTC")
    now = datetime.datetime.now(utc_tz)
    # delete tzinfo
    now = now.replace(tzinfo=None)
    return now


def calculate_hash(data):
    data_str = json.dumps(data, sort_keys=True)
    return hashlib.sha256(data_str.encode("utf-8")).hexdigest()


def natstat_get_str(key: str, data: dict) -> str | None:
    """
    Handle case when we expect to have string as value,
    but sometimes Natstat may have {} dictionary instead of string value.
    """
    value = jmespath.search(key, data)
    if isinstance(value, dict):
        return None
    return value


async def gather_throttle(simultaneous_tasks_limit: int, *tasks: list[Coroutine]) -> Any:
    """
    Helps to gather coroutines with a concurrency limit,
    for example to not overload an external API.
    """
    semaphore = asyncio.Semaphore(simultaneous_tasks_limit)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))


def get_game_datetime(game_data: dict) -> datetime.datetime:
    datetime_str = game_data["gameday"]
    # edge case when gameday may be 2014-10-00
    if datetime_str.endswith("-00"):
        datetime_str = datetime_str.replace("-00", "-01")

    if isinstance(game_data["starttime"], dict):
        gamedatetime = datetime.datetime.strptime(datetime_str, "%Y-%m-%d")
    else:
        datetime_str += " " + game_data["starttime"]
        gamedatetime = datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
    gamedatetime = ny_tz.localize(gamedatetime).astimezone(utc)
    gamedatetime = gamedatetime.replace(tzinfo=None)
    return gamedatetime


def get_natstat_value(data, path, value_type):
    value = jmespath.search(path, data)
    if isinstance(value, dict):
        return None
    if value is not None:
        return value_type(value)
    return None


def transform_sequence(sequence_str):
    if sequence_str:
        return sequence_str.split("-")[-1]
    return None


def is_game_final(game_status: str) -> bool:
    return "final" in game_status.lower()


def get_stat_value(item, key, value_type):
    try:
        return get_natstat_value(item, key, value_type)
    except ValueError:
        return None
