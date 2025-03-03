import asyncio
import aiohttp

from app.common.logging import get_logger
from app.common.config import proxy, proxy_auth

from app.common.prometheus import (
    client_request_duration,
    client_requests,
)


def log(msg: str):
    get_logger().info(msg)


class NatstatClientV3:
    def __init__(self, max_retries: int = 2, timeout: int = 60) -> None:
        self.max_retries = max_retries
        self.timeout = timeout

    async def fetch_games_by_season(self, sport_code: str, season: int, offset: int):
        url = f"https://api3.natst.at/53d3-7bb5aa/games/{sport_code}/{season}/{offset}"
        log(f"Making request to {url}")

        client_requests.labels(endpoint="fetch_games_v3").inc()
        with client_request_duration.labels(endpoint="fetch_games_v3").time():
            response = await self._get(url)
        return response

    async def fetch_games_by_date(self, sport_code: str, date: str, offset: int):
        url = f"https://api3.natst.at/53d3-7bb5aa/games/{sport_code}/{date}/{offset}"

        client_requests.labels(endpoint="fetch_games_v3").inc()
        with client_request_duration.labels(endpoint="fetch_games_v3").time():
            response = await self._get(url)
        return response

    async def _get(self, url: str) -> dict | None:
        for attempts in range(self.max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url,
                        timeout=self.timeout,
                    ) as response:
                        if response.status == 500:
                            log(f"NatstatClient: for 500 error for {url}")
                            return None
                        return await response.json()

            except asyncio.TimeoutError as e:
                log(f"NatstatClient: Attempt {attempts} failed with error: {e}")
                if attempts < self.max_retries - 1:
                    log(f"NatstatClient: Retrying for {url} (attempt {attempts + 1})")
                else:
                    log(
                        f"NatstatClient: Failed to fetch data for {url} "
                        "after {self.max_retries} attempts."
                    )
                    raise e


class NatstatClient:
    def __init__(self, max_retries: int = 2, timeout: int = 60) -> None:
        self.max_retries = max_retries
        self.timeout = timeout

    async def fetch_sports(self) -> dict | None:
        url = "https://interst.at/meta/allsports"

        client_requests.labels(endpoint="fetch_sports").inc()
        with client_request_duration.labels(endpoint="fetch_sports").time():
            response = await self._get(url)
        return response

    async def fetch_sport_status(self, sport_code: str) -> dict | None:
        url = f"https://interst.at/meta/{sport_code}/status"

        client_requests.labels(endpoint="fetch_sport_status").inc()
        with client_request_duration.labels(endpoint="fetch_sport_status").time():
            response = await self._get(url)
        return response

    async def fetch_games_in_season_range(
        self, sport_code: str, start: int, end: int
    ) -> dict | None:
        url = f"https://interst.at/game/{sport_code}/{start}-{end}"

        client_requests.labels(endpoint="fetch_games_in_season_range").inc()
        with client_request_duration.labels(endpoint="fetch_games_in_season_range").time():
            response = await self._get(url)
        return response

    async def fetch_games_in_date_range(
        self, sport_code: str, start_date: str, end_date: str
    ) -> dict | None:
        url = f"https://interst.at/game/{sport_code}/{start_date},{end_date}"

        client_requests.labels(endpoint="fetch_games_in_date_range").inc()
        with client_request_duration.labels(endpoint="fetch_games_in_date_range").time():
            response = await self._get(url)
        return response

    async def fetch_teams(self, sport_code: str) -> dict | None:
        url = f"https://interst.at/meta/{sport_code}/teams"

        client_requests.labels(endpoint="fetch_teams").inc()
        with client_request_duration.labels(endpoint="fetch_teams").time():
            response = await self._get(url)
        return response

    async def fetch_players(self, sport_code: str, season: int) -> dict | None:
        url = f"https://interst.at/meta/{sport_code}/players,{season}"

        client_requests.labels(endpoint="fetch_players").inc()
        with client_request_duration.labels(endpoint="fetch_players").time():
            response = await self._get(url)
        return response

    async def fetch_leagues(self, sport_code: str) -> dict | None:
        url = f"https://interst.at/meta/{sport_code}/leagues"

        client_requests.labels(endpoint="fetch_leagues").inc()
        with client_request_duration.labels(endpoint="fetch_leagues").time():
            response = await self._get(url)
        return response

    async def fetch_games_in_range(self, sport_code: str, start: str, end: str) -> dict | None:
        url = f"https://interst.at/game/{sport_code}/{start},{end}"

        client_requests.labels(endpoint="fetch_games_in_range").inc()
        with client_request_duration.labels(endpoint="fetch_games_in_range").time():
            response = await self._get(url)
        return response

    async def fetch_game(self, sport_code: str, game_id: int) -> dict | None:
        url = f"https://interst.at/game/{sport_code}/{game_id}"

        client_requests.labels(endpoint="fetch_game").inc()
        with client_request_duration.labels(endpoint="fetch_game").time():
            response = await self._get(url)
        return response

    async def _get(self, url: str) -> dict | None:
        for attempts in range(self.max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url,
                        timeout=self.timeout,
                        proxy=proxy,
                        proxy_auth=proxy_auth,
                    ) as response:
                        if response.status == 500:
                            log(f"NatstatClient: for 500 error for {url}")
                            return None
                        return await response.json()

            except asyncio.TimeoutError as e:
                log(f"NatstatClient: Attempt {attempts} failed with error: {e}")
                if attempts < self.max_retries - 1:
                    log(f"NatstatClient: Retrying for {url} (attempt {attempts + 1})")
                else:
                    log(
                        f"NatstatClient: Failed to fetch data for {url} "
                        "after {self.max_retries} attempts."
                    )
                    raise e
