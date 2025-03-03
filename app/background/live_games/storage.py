from datetime import datetime, timedelta
import sys

from app.common.logging import get_logger
from app.db.database import Game
from app.common.utils import get_utc_now
from app.common.utils import get_game_datetime, is_game_final
from app.common.types import GameShort, GameLastInfo, GameChanges, GameType

from app.common.prometheus import (
    storage_size_gauge,
    games_by_type_gauge,
)


def log(msg: str):
    get_logger().info(msg)


class InmemoryStorage:
    def __init__(self):
        self._storage: dict[str, GameLastInfo] = {}

    def update_metrics(self):
        size = sys.getsizeof(self._storage)
        for key, value in self._storage.items():
            size += sys.getsizeof(key) + value.sizeof()

        storage_size_gauge.set(size)

        for game_type in GameType:
            count = len(self.get_games_by_type(game_type))
            games_by_type_gauge.labels(type=game_type.value).set(count)

    def fill_inmemory_storage(self, games: list[Game]):
        log("InmemoryStorage: fill_inmemory_storage start")
        for game in games:
            game_short = GameShort(game_id=game.id, sport_code=game.sport_code)

            game_last_info = GameLastInfo(
                game_short=game_short,
                gamedatetime=game.gamedatetime,
                status=game.status,
                score_visitor=game.score_visitor,
                score_home=game.score_home,
                score_overtime=game.score_overtime,
            )

            if game_short.key not in self._storage:
                self._storage[game_short.key] = game_last_info

        self.update_metrics()
        log("InmemoryStorage: fill_inmemory_storage end")

    def clean_old_records(self):
        log("InmemoryStorage: cleaning old records")
        threshold_time = datetime.now() - timedelta(hours=5)
        new_storage = {
            key: info for key, info in self._storage.items() if info.gamedatetime >= threshold_time
        }
        self._storage = new_storage

        self.update_metrics()
        log("InmemoryStorage: cleaned old records")

    def get_games_by_type(self, game_type: GameType) -> list[GameShort]:
        current_time = get_utc_now()

        three_hours_ago = current_time - timedelta(hours=3)
        five_hours_future = current_time + timedelta(hours=3)
        today_start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = current_time.replace(hour=23, minute=59, second=59, microsecond=999999)

        games_short = []

        for game_last_info in self._storage.values():
            gamedatetime = game_last_info.gamedatetime

            if game_type == GameType.early_final and is_game_final(game_last_info.status):
                games_short.append(game_last_info.game_short)
            elif game_type == GameType.live and (
                (current_time <= gamedatetime < five_hours_future)
                or (
                    game_last_info.status != "Scheduled"
                    and three_hours_ago <= gamedatetime < current_time
                )
            ):
                games_short.append(game_last_info.game_short)
            elif game_type == GameType.early and current_time < gamedatetime <= current_time:
                games_short.append(game_last_info.game_short)
            elif game_type == GameType.today and today_start <= gamedatetime <= today_end:
                games_short.append(game_last_info.game_short)

        return games_short

    def get_last_info(self, key: str) -> GameLastInfo | None:
        return self._storage.get(key)

    def update_games_list(self, games_changes: list[GameChanges]):
        generic_keys = [
            "status",
            "gamedatetime",
            "score_visitor",
            "score_home",
            "score_overtime",
        ]

        for game_changes in games_changes:
            last_info = self.get_last_info(game_changes.game_short.key)

            if last_info is None:
                last_info = GameLastInfo(
                    game_short=game_changes.game_short,
                    gamedatetime=get_game_datetime(game_changes.game_data),
                    status=game_changes.game_data["status"],
                )
                self._storage[last_info.game_short.key] = last_info

            for key in generic_keys:
                val = getattr(game_changes, key)
                if val is not None:
                    setattr(last_info, key, val)

            if game_changes.playbyplay_changes:
                playbyplay_ids = {int(item["id"]) for item in game_changes.playbyplay_changes}
                if last_info.playbyplay_ids is None:
                    last_info.playbyplay_ids = playbyplay_ids
                else:
                    last_info.playbyplay_ids.update(playbyplay_ids)

    def get_game_current_status(self, key: str) -> str:
        return self._storage[key].status
