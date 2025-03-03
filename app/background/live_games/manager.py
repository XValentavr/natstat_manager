from starlette.concurrency import run_in_threadpool

from app.common.logging import get_logger
from app.db.db_client import DbClient
from app.common.utils import gather_throttle
from app.common.clients import NatstatClient
from app.common.types import GameShort, GameChanges, GameType

from .storage import InmemoryStorage
from .converter import GameChangeConvertor


def log(msg: str):
    get_logger().info(msg)


class GameUpdateManager:
    def __init__(
        self,
        storage: InmemoryStorage,
        db_client: DbClient,
        client: NatstatClient,
        game_change_convertor: GameChangeConvertor,
    ) -> None:
        self.storage = storage
        self.db_client = db_client
        self.client = client
        self.game_change_convertor = game_change_convertor

    async def game_details_update(self, game_type: GameType) -> None:
        games_short = self.storage.get_games_by_type(game_type)

        tasks = [self.get_game_changes(game_short) for game_short in games_short]
        changes = await gather_throttle(10, *tasks)

        changes = [change for change in changes if change is not None]

        self.storage.update_games_list(changes)

        if game_type == GameType.early_final:
            await self.save_final_games(changes)
        else:
            await run_in_threadpool(self.db_client.upsert_live_games, changes=changes)

    async def get_game_changes(self, game_short: GameShort) -> GameChanges | None:
        response = await self.client.fetch_game(game_short.sport_code, game_short.game_id)
        game_data = self.response_to_game_data(response)

        if game_data is None:
            return None

        game_last_info = self.storage.get_last_info(game_short.key)

        game_changes = self.game_change_convertor.get_game_changes(
            game_short, game_data, game_last_info
        )

        return game_changes

    def response_to_game_data(self, raw_response: dict | None) -> dict | None:
        if (
            raw_response is None
            or raw_response["success"] == "0"
            or "games" not in raw_response
            or len(raw_response["games"]) > 1
        ):
            log("GameUpdateManager: got bad response")
            return None
        return list(raw_response["games"].values())[0]

    async def save_final_games(self, changes: list[GameChanges]):
        for change in changes:
            await run_in_threadpool(
                self.db_client.upsert_games,
                sport_code=change.game_short.sport_code,
                games=[change.game_data],
            )
        games_data_with_sport = [(item.game_short.sport_code, item.game_data) for item in changes]
        if games_data_with_sport:
            await run_in_threadpool(self.db_client.save_game_details, games=games_data_with_sport)
            await run_in_threadpool(
                self.db_client.save_pbps, games=games_data_with_sport, is_live=True
            )
