import asyncio
from datetime import timedelta
import time

from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel

from app.common.logging import get_logger
from app.db.db_client import DbClient
from app.db.database import Sport
from app.common.utils import get_utc_now, gather_throttle
from app.common.clients import NatstatClient
from app.common.types import GameType

from .storage import InmemoryStorage
from .manager import GameUpdateManager

from app.common.prometheus import (
    background_event_loop_uptime,
    background_event_loop_last_heartbeat,
    cron_job_execution_count,
    cron_job_last_success,
)


def log(msg: str):
    get_logger().info(msg)


class FetchedGamesResult(BaseModel):
    is_successful: bool
    games_data: list[dict]


class FutureGamesUpdateRuntime:
    @staticmethod
    def run():
        log("FutureGamesUpdateRuntime: start run")
        try:
            obj = FutureGamesUpdateRuntime()
            asyncio.run(obj.async_run())
            cron_job_last_success.labels(job_name="FutureGamesUpdate").set(time.time())
            cron_job_execution_count.labels(job_name="FutureGamesUpdate", status="success").inc()
        except Exception as e:
            cron_job_execution_count.labels(job_name="FutureGamesUpdate", status="failure").inc()
            log(f"FutureGamesUpdateRuntime failed: {str(e)}")
            raise e

    def __init__(self) -> None:
        self.db_client = DbClient()
        self.client = NatstatClient()
        self.reference_dt = get_utc_now()
        log(f"FutureGamesUpdateRuntime: using reference datetime {self.reference_dt}")

    async def async_run(self):
        log("FutureGamesUpdateRuntime: start async run")

        log("FutureGamesUpdateRuntime: getting sports")
        sports = await run_in_threadpool(self.db_client.get_basketball_sports)
        log(f"FutureGamesUpdateRuntime: got {len(sports)} sports")

        log("FutureGamesUpdateRuntime: getting games")
        tasks = [self.get_future_games_by_sport(sport.code) for sport in sports]
        fetched_games_results = await gather_throttle(10, *tasks)
        log("FutureGamesUpdateRuntime: got games")

        log("FutureGamesUpdateRuntime: upserting games")
        await self.upsert_future_games(sports, fetched_games_results)
        log("FutureGamesUpdateRuntime: upserted games")

    async def get_future_games_by_sport(self, sport_code: str) -> FetchedGamesResult:
        log(f"FutureGamesUpdateRuntime: getting games for {sport_code}")
        response = await self.get_response(sport_code)
        results = self.parse_games_response(response)
        log(f"FutureGamesUpdateRuntime: got {len(results.games_data)} games for {sport_code}")
        return results

    async def upsert_future_games(
        self,
        sports: list[Sport],
        fetched_games_results: list[FetchedGamesResult],
    ) -> None:
        for sport, fetched_games_result in zip(sports, fetched_games_results):
            if not fetched_games_result.is_successful:
                continue

            games = fetched_games_result.games_data
            log(f"FutureGamesUpdateRuntime: upserting {len(games)} games for {sport.code}")

            await self.mark_removed_games(sport.code, games)
            await self.upsert_games_in_batches(sport.code, games)

    async def mark_removed_games(self, sport_code: str, fetched_games: list[dict]) -> None:
        ten_days_later = self.reference_dt + timedelta(days=10)

        existing_games = await run_in_threadpool(
            self.db_client.get_games_in_date_range,
            sport_code=sport_code,
            start_date=self.reference_dt,
            end_date=ten_days_later,
        )

        existing_game_ids = {game.id for game in existing_games}
        fetched_game_ids = {int(game["id"]) for game in fetched_games}
        game_ids_to_remove = existing_game_ids - fetched_game_ids

        if game_ids_to_remove:
            log(
                f"FutureGamesUpdateRuntime: marking {game_ids_to_remove} games "
                f"as removed for {sport_code}"
            )
            await run_in_threadpool(
                self.db_client.mark_games_as_removed,
                sport_code=sport_code,
                game_ids=list(game_ids_to_remove),
            )
            log(
                f"FutureGamesUpdateRuntime: marked {len(game_ids_to_remove)} games "
                f"as removed for {sport_code}"
            )

    async def upsert_games_in_batches(
        self, sport_code: str, games: list[dict], batch_size: int = 500
    ) -> None:
        for start in range(0, len(games), batch_size):
            batch = games[start : start + batch_size]
            await run_in_threadpool(self.db_client.upsert_games, games=batch, sport_code=sport_code)
        log(f"FutureGamesUpdateRuntime: Upserted {len(games)} games for {sport_code}")

    async def get_response(self, sport_code: str) -> dict | None:
        start = self.reference_dt.strftime("%Y-%m-%d")
        end = (self.reference_dt + timedelta(days=100)).strftime("%Y-%m-%d")
        return await self.client.fetch_games_in_range(sport_code, start, end)

    def parse_games_response(self, raw_response: dict | None) -> FetchedGamesResult:
        if raw_response is None or raw_response["success"] == "0":
            log("FutureGamesUpdateRuntime: got bad response")
            return FetchedGamesResult(is_successful=False, games_data=[])

        return FetchedGamesResult(
            is_successful=True, games_data=list(raw_response["games"].values())
        )


class FillInmemoryRuntime:
    @staticmethod
    def run(storage: InmemoryStorage):
        log("FillInmemoryRuntime: start run")
        try:
            obj = FillInmemoryRuntime(storage=storage)
            asyncio.run(obj.async_run())
            cron_job_last_success.labels(job_name="FillInmemoryStorage").set(time.time())
            cron_job_execution_count.labels(job_name="FillInmemoryStorage", status="success").inc()
        except Exception as e:
            cron_job_execution_count.labels(job_name="FillInmemoryStorage", status="failure").inc()
            log(f"FillInmemoryRuntime failed: {str(e)}")
            raise e

    def __init__(self, storage: InmemoryStorage):
        self.storage = storage

    async def async_run(self):
        db_client = DbClient()
        games = await run_in_threadpool(db_client.get_recent_and_upcoming_games)
        self.storage.fill_inmemory_storage(games)


class CleanOldInmemoryRecordsRuntime:
    @staticmethod
    def run(storage: InmemoryStorage):
        log("CleanOldInmemoryRecordsRuntime: start run")
        obj = CleanOldInmemoryRecordsRuntime(storage=storage)
        asyncio.run(obj.async_run())

    def __init__(self, storage: InmemoryStorage):
        self.storage = storage

    async def async_run(self):
        log("CleanOldInmemoryRecordsRuntime: start async run")
        start_time = time.time()
        loop_name = "inmemory_storage_gc_loop"

        while True:
            self.storage.clean_old_records()

            current_time = time.time()
            uptime = current_time - start_time
            background_event_loop_uptime.labels(loop_name=loop_name).inc(uptime)
            background_event_loop_last_heartbeat.labels(loop_name=loop_name).set(current_time)

            await asyncio.sleep(60 * 30)


class GameDetailsUpdateRuntime:
    @staticmethod
    def run(game_update_manager: GameUpdateManager):
        log("GameDetailsUpdateRuntime: start run")
        obj = GameDetailsUpdateRuntime(game_update_manager=game_update_manager)
        asyncio.run(obj.async_run())

    def __init__(self, game_update_manager: GameUpdateManager):
        self.game_update_manager = game_update_manager

    async def async_run(self):
        log("GameDetailsUpdateRuntime: start async run")
        loop = asyncio.get_event_loop()
        tasks = [
            loop.create_task(self.run_update_game_details(GameType.today, 60 * 10)),
            loop.create_task(self.run_update_game_details(GameType.early, 60 * 2)),
            loop.create_task(self.run_update_game_details(GameType.live, 2)),
            loop.create_task(self.run_update_game_details(GameType.early_final, 60 * 5)),
        ]
        await asyncio.gather(*tasks)
        log("GameDetailsUpdateRuntime: end async run")

    async def run_update_game_details(self, game_type: GameType, sleep_duration: int):
        log(f"GameDetailsUpdateRuntime: start event loop for {game_type}")
        start_time = time.time()
        loop_name = f"game_update_{game_type.value}"

        while True:
            await self.game_update_manager.game_details_update(game_type)

            current_time = time.time()
            uptime = current_time - start_time
            background_event_loop_uptime.labels(loop_name=loop_name).inc(uptime)
            background_event_loop_last_heartbeat.labels(loop_name=loop_name).set(current_time)

            await asyncio.sleep(sleep_duration)
