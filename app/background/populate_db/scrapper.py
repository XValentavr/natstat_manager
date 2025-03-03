import asyncio
from datetime import datetime

import aiohttp
import aiohttp.client_exceptions
from starlette.concurrency import run_in_threadpool
from time import time
import pytz

from app.common.logging import get_logger
from app.db.db_client import DbClient
from app.db.database import Sport
from app.common.config import proxy, proxy_auth
from app.common.clients import NatstatClient, NatstatClientV3
from app.common.exceptions import NatstatFetchError

from .db_client import PopulateDbClient
from .types import GameDataWithSport

ny_tz = pytz.timezone("America/New_York")


def log(msg: str):
    get_logger().info(msg)


async def fetch_v3_games_data():
    db_client = PopulateDbClient()
    client_v3 = NatstatClientV3()
    sport_code = "AMERBB"
    end_season = 2012
    current_season = 2024

    while current_season >= end_season:
        offset = 0

        while True:
            log(f"Making v3 games request {offset=} {current_season=} {sport_code=}")
            response = await client_v3.fetch_games_by_season(sport_code, current_season, offset)
            log(f"Got v3 games response {offset=} {current_season=}")
            # TODO: resp is None
            await _wait_for_rate_limit_expire(response)

            if (
                response.get("success") == "0"
                and response.get("error", {}).get("message") == "NO_DATA"
            ):
                log(f"Got v3 games unsuccess no data response {offset=} {current_season=}")
                break

            games_data = list(response["games"].values())
            log(f"Saving v3 {len(games_data)} games {offset=} {current_season=}")
            await run_in_threadpool(
                db_client.save_v3_games,
                season=current_season,
                sport_code=sport_code,
                games_data=games_data,
            )
            log(f"Saved v3 games {offset=} {current_season=}")

            log(f"remaming {response['user']}")

            offset += 100

        current_season = current_season - 1


async def fetch_v3_games_data_by_date():
    log("Starting fetch_v3_games_data_by_date")
    db_client = PopulateDbClient()
    client_v3 = NatstatClientV3()
    sport_code = "EUROBB"

    i = 0
    dates = """"""
    dates_list = dates.split("\n")
    end = len(dates_list)

    while i < end:
        date = dates_list[i]
        offset = 0

        while True:
            log(f"Making v3 games request {offset=} {date=} {i=}")
            response = await client_v3.fetch_games_by_date(sport_code, date, offset)
            log(f"Got v3 games response {offset=} {date=} {i=}")

            await _wait_for_rate_limit_expire(response)

            if (
                response.get("success") == "0"
                and response.get("error", {}).get("message") == "NO_DATA"
            ):
                log(f"Got v3 games unsuccess no data response {offset=} {date=}")
                break

            games_data = list(response["games"].values())
            log(f"Saving v3 {len(games_data)} games {offset=} {date=} {i=}")
            await run_in_threadpool(
                db_client.save_v3_games, season=None, sport_code=sport_code, games_data=games_data
            )
            log(f"Saved v3 games {offset=} {date=} {i=}")

            log(f"remaming {response['user']}")

            if "page-next" not in response["meta"]:
                break

            offset += 100

        i += 1


async def _wait_for_rate_limit_expire(response):
    user_info = response["user"]
    remaining = int(user_info["ratelimit-remaining"])
    reset_time = user_info.get("ratelimit-reset")

    if remaining == 0 and reset_time:
        reset_datetime = datetime.strptime(reset_time, "%Y-%m-%d %H:%M:%S")
        reset_datetime = ny_tz.localize(reset_datetime)
        current_time = datetime.now(ny_tz)
        wait_time = (reset_datetime - current_time).total_seconds()
        if wait_time > 0:
            minutes, seconds = divmod(int(wait_time), 60)
            log(f"Rate limit reached. Waiting for {minutes} minutes and {seconds} seconds.")
            await asyncio.sleep(wait_time + 60 * 2)


async def fetch_and_save_seasons():
    db_client = PopulateDbClient()
    sports = await run_in_threadpool(db_client.get_sports)
    all_leagues = await run_in_threadpool(db_client.get_leagues)
    client = NatstatClient(timeout=90)

    for sport in sports:
        if sport.code == "FC":
            continue

        log(f"Start season fetch for {sport.code}")
        data = await client.fetch_sport_status(sport.code)

        if data is None:
            log(f"Got no data for {sport.code} status")
            raise NatstatFetchError("sport status", sport.code)

        sport_status = list(data["status"].values())[0]
        last_season: str = sport_status["last"]
        in_play: str = sport_status["inplay"]
        seasons = sport_status["seasons"].values()

        for league in all_leagues:
            if league.sport_code != sport.code:
                continue

            log(f"Saving seasons for {league.name} league fetch for {sport.code}")
            await run_in_threadpool(
                db_client.save_seasons_for_league,
                sport_code=sport.code,
                seasons_data=seasons,
                last_season=last_season,
                last_season_in_play=in_play,
                league=league,
            )


async def fetch_and_save_missing_games():
    log("fetch_and_save_missing_games start")

    log("fetch_and_save_missing_games high lvl data start")
    # db_client = DbClient()
    # sports = await run_in_threadpool(db_client.get_basketball_sports)

    # skip WBPRO, because it returns 500 for all endpoints
    # sports = [sport for sport in sports if sport.code != "WBPRO"]
    # for sport in sports:
    #     await populate_games_by_month_for_sport(sport)
    # log("fetch_and_save_missing_games high lvl data end")

    log("fetch_and_save_missing_games fine grained game data start")
    await populate_fine_grained_game_data()
    log("fetch_and_save_missing_games fine grained game data end")


async def populate_data():
    log("Populate data start")

    # log("Populate high lvl data start")
    # await populate_high_lvl_data()
    # log("Populate high lvl data end")

    log("Populate fine grained game data start")
    await populate_fine_grained_nonbasketball_game_data()
    log("Populate fine grained game data end")


async def populate_high_lvl_data():
    db_client = DbClient()
    sports = await run_in_threadpool(db_client.get_sports_except_basketball_and_football)

    log("Populate leagues start")
    await populate_leagues(sports)
    log("Populate leagues end")

    for sport in sports:
        log(f"Populate players by sport {sport.code} start")
        await populate_players_by_sport(sport)
        log(f"Populate players by sport {sport.code} end")

    for sport in sports:
        log(f"Populate teams by sport {sport.code} start")
        await populate_teams_by_sport(sport)
        log(f"Populate teams by sport {sport.code} end")

    for sport in sports:
        log(f"Populate games by sport {sport.code} start")
        await populate_games_by_sport(sport)
        log(f"Populate games by sport {sport.code} end")


async def populate_leagues(sports: list[Sport]) -> None:
    db_client = DbClient()
    client = NatstatClient()

    log(f"Populate leagues for {len(sports)} sports")
    for sport in sports:
        log(f"Populate leagues fetch for {sport.code} start")

        data = await client.fetch_leagues(sport.code)
        log(f"Populate leagues fetch for {sport.code} end")

        if data is None:
            log(f"Got no data for {sport.code} leagues")
            raise NatstatFetchError("leagues", sport.code)

        if data["success"] == "0" and data["error"]["message"] == "NO_DATA":
            log(f"Got no data leagues for {sport.code}")
            continue

        log("Populate leagues bulk save start")
        await run_in_threadpool(
            db_client.save_leagues, leagues=data["leagues"].values(), sport_code=sport.code
        )
        log("Populate leagues bulk save end")


async def populate_players_by_sport(sport: Sport):
    db_client = DbClient()
    client = NatstatClient(timeout=90)

    stats_begin = sport.statsbegin if sport.statsbegin else sport.first
    start_from = max(2000, stats_begin)
    for season in range(start_from, sport.last + 1):
        log(f"Populate players by sport fetch {sport.code} {season} season start")
        data = await client.fetch_players(sport.code, season)
        log(f"Populate players by sport fetch {sport.code} {season} season end")

        if data is None:
            log(f"Got data is None for {sport.code} players")
            continue

        if data["success"] == "0" and data["error"]["message"] == "NO_DATA":
            log(f"Got no data for {sport.code} players")
            continue

        log(f"Populate {len(data['players'])} players by {sport.code}" " save for {season} season")
        values = list(data["players"].values())
        for start in range(0, len(values), 500):
            await run_in_threadpool(
                db_client.save_players, sport_code=sport.code, players=values[start : start + 500]
            )
        log(f"Populate players by sport save for {sport.code} end")


async def populate_teams_by_sport(sport: Sport):
    db_client = DbClient()
    client = NatstatClient(timeout=90)

    log("Populate teams by sport fetch start")
    data = await client.fetch_teams(sport.code)
    log("Populate teams by sport fetch end")

    if data is None:
        log(f"Got no data for {sport.code} teams")
        raise NatstatFetchError("teams", sport.code)

    if data["success"] == "0" and data["error"]["message"] == "NO_DATA":
        log(f"Got no data teams for {sport.code}")
        return

    log(f"Populate {len(data['teams'])} teams by sport save for {sport.code} start")
    await run_in_threadpool(
        db_client.save_teams, sport_code=sport.code, teams=data["teams"].values()
    )
    log(f"Populate teams by sport save for {sport.code} end")


async def populate_games_by_sport(sport: Sport):
    db_client = DbClient()
    client = NatstatClient(timeout=90)

    batch_size = 0
    stats_begin = sport.statsbegin if sport.statsbegin else sport.first
    start_from = max(2000, stats_begin)
    for start in range(start_from, sport.last + 1):
        end = min(sport.last, start + batch_size)
        log(f"Populate games by sport fetch batch {sport.code} {start}-{end} start")
        data = await client.fetch_games_in_season_range(sport.code, start, end)

        if data is None:
            log(f"Got no data for {sport.code} games")
            raise NatstatFetchError("games", sport.code)

        log(f"Populate games by sport fetch batch {start}-{end} end")
        if data["success"] == "0" and data["error"].get("message") == "NO_DATA":
            log(f"Populate games by sport fetch batch {start}-{end} NO DATA")
            continue

        log(f"Populate {len(data['games'])} games by sport save for {sport.code} start")
        values = list(data["games"].values())
        for _start in range(0, len(values), 500):
            await run_in_threadpool(
                db_client.upsert_games, sport_code=sport.code, games=values[_start : _start + 500]
            )
        log(f"Populate games by sport save for {sport.code}  {start}-{end} end")


async def populate_fine_grained_nonbasketball_game_data():
    log("Populate game details start")
    populate_db_client = PopulateDbClient()

    games = await run_in_threadpool(populate_db_client.get_nonbaskteball_notscheduled_games)
    log(f"Populate game details for {len(games)} games start")

    db_client = DbClient()
    sports = await run_in_threadpool(db_client.get_sports_except_basketball_and_football)
    sport2type = {sport.code: sport.sport for sport in sports}

    semaphore = asyncio.Semaphore(20)
    batch_size = 45
    total_batches = (len(games) + batch_size - 1) // batch_size
    LAST_CHECKPOINT = 247735

    async with aiohttp.ClientSession() as session:
        for i in range(LAST_CHECKPOINT, len(games), batch_size):
            start_time = time()
            log(f"Populate game details batch#{i} fetch start")
            tasks = [
                fetch_game_details(semaphore, session, sport_code, gid)
                for gid, sport_code in games[i : i + batch_size]
            ]
            all_games = [i for i in await asyncio.gather(*tasks) if i is not None]
            log(f"Populate game details batch#{i} fetch end")

            log(f"Have {len(all_games)} games")

            log(f"Populate game details batch#{i} save start")
            await run_in_threadpool(
                populate_db_client.save_game_details, games=all_games, sport2type=sport2type
            )
            log(f"Populate game details batch#{i} save end")

            duration = int((time() - start_time) * 1000)
            log(f"Populate game details batch#{i} took {duration}ms")

            remaining_batches = total_batches - (i // batch_size + 1)
            eta = int(remaining_batches * duration / 1000)
            log(f"Estimated time remaining: {eta} seconds")


async def fetch_game_details(
    semaphore, session, sport_code: str, game_id
) -> GameDataWithSport | None:
    max_retries = 2

    async with semaphore:
        for attempts in range(max_retries):
            try:
                async with session.get(
                    f"https://interst.at/game/{sport_code}/{game_id}",
                    timeout=40,
                    proxy=proxy,
                    proxy_auth=proxy_auth,
                ) as response:
                    if response.status == 500:
                        # Error on their side
                        # log(f"Server error for {sport_code} game {game_id}")
                        return None
                    data = await response.json()

                    # Sometimes Natstat can't fetch by game id and instead gives
                    # list of games for year as game id
                    # ex: /game/nba/1976
                    if (
                        "games" not in data
                        or len(data["games"]) > 1
                        or f"game_{game_id}" not in data["games"]
                    ):
                        log(f"Can't fetch games for {sport_code} game {game_id}")
                        return None

                    return sport_code, data["games"][f"game_{game_id}"]
            except aiohttp.client_exceptions.ServerConnectionError:
                log(f"Server connection error for {sport_code} game {game_id}")
            except asyncio.exceptions.TimeoutError:
                log(f"Timeout error for {sport_code} game {game_id}")

            if attempts < max_retries - 1:
                log(f"Retrying {sport_code} game {game_id} (attempt {attempts + 1})")
            else:
                log(
                    f"Failed to fetch details for {sport_code} game {game_id} "
                    f"after {max_retries} attempts"
                )
                return None


async def populate_fine_grained_game_data():
    log("Populate game details start")

    db_client = DbClient()
    # games = await run_in_threadpool(db_client.get_notscheduled_games)
    games = await run_in_threadpool(db_client.get_basketball_games_missing_data)
    # games = await run_in_threadpool(db_client.get_games_missing_data)

    log(f"Populate game details for {len(games)} games start")

    semaphore = asyncio.Semaphore(23)
    batch_size = 50
    total_batches = (len(games) + batch_size - 1) // batch_size
    LAST_CHECKPOINT = 38400

    async with aiohttp.ClientSession() as session:
        for i in range(LAST_CHECKPOINT, len(games), batch_size):
            start_time = time()
            log(f"Populate game details batch#{i} fetch start")
            tasks = [
                fetch_game_details(semaphore, session, sport_code, gid)
                for gid, sport_code in games[i : i + batch_size]
            ]
            all_games = [i for i in await asyncio.gather(*tasks) if i is not None]
            log(f"Populate game details batch#{i} fetch end")

            if len(all_games) == 0:
                continue

            log(f"Populate game details batch#{i} save start")
            await run_in_threadpool(db_client.save_game_details, games=all_games)
            log(f"Populate game details batch#{i} save end")

            log(f"Populate game details batch#{i} save start")
            await run_in_threadpool(db_client.save_pbps, games=all_games, is_live=False)
            log(f"Populate game details batch#{i} save end")

            duration = int((time() - start_time) * 1000)
            log(f"Populate game details batch#{i} took {duration}ms")

            remaining_batches = total_batches - (i // batch_size + 1)
            eta = int(remaining_batches * duration / 1000)
            log(f"Estimated time remaining: {eta} seconds")


async def populate_games_by_month_for_sport(sport: Sport):
    db_client = DbClient()
    client = NatstatClient(timeout=90)

    stats_begin = sport.statsbegin if sport.statsbegin else sport.first

    for current_year in reversed(range(stats_begin, sport.last + 1)):
        for month in range(1, 13):
            start_date = f"{current_year}-{month:02d}-01"
            if month == 12:
                end_date = f"{current_year + 1}-01-01"
            else:
                end_date = f"{current_year}-{month + 1:02d}-01"

            log(f"Populate games by sport fetch batch {sport.code} {start_date} to {end_date}")
            data = await client.fetch_games_in_date_range(sport.code, start_date, end_date)

            if data is None:
                log(f"Got no data for {sport.code} games")
                raise NatstatFetchError("games", sport.code)

            if data["success"] == "0" and data["error"].get("message") == "NO_DATA":
                log(f"Populate games by sport fetch batch {start_date}-{end_date} NO DATA")
                continue

            log(f"Populate {len(data['games'])} games by sport save for {sport.code} start")
            values = list(data["games"].values())
            for _start in range(0, len(values), 500):
                await run_in_threadpool(
                    db_client.upsert_games,
                    sport_code=sport.code,
                    games=values[_start : _start + 500],
                )
            log(f"Populate games by sport save for {sport.code}  {start_date}-{end_date} end")
