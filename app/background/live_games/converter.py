from datetime import datetime

import jmespath

from app.common.logging import get_logger
from app.common.utils import get_game_datetime, get_natstat_value
from app.common.types import GameShort, GameLastInfo, GameChanges


def log(msg: str):
    get_logger().info(msg)


class GameChangeConvertor:
    def get_game_changes(
        self, game_short: GameShort, game_data: dict, last_info: GameLastInfo | None
    ) -> GameChanges:
        """
        if last info is None. add all changes
        """

        status = self.get_changed_status(last_info, game_data)
        gamedatetime = self.get_changed_gamedatetime(last_info, game_data)
        score_visitor = self.get_changed_score_visitor(last_info, game_data)
        score_home = self.get_changed_score_home(last_info, game_data)
        score_overtime = self.get_changed_score_overtime(last_info, game_data)

        playbyplay_changes = self.get_play_by_play_changes(last_info, game_data)

        return GameChanges(
            game_short=game_short,
            game_data=game_data,
            status=status,
            gamedatetime=gamedatetime,
            score_visitor=score_visitor,
            score_home=score_home,
            score_overtime=score_overtime,
            playbyplay_changes=playbyplay_changes,
        )

    def get_changed_status(self, last_info: GameLastInfo | None, game_data: dict) -> str | None:
        if last_info is None or last_info.status != game_data["status"]:  # new game
            return game_data["status"]
        return None

    def get_changed_gamedatetime(
        self, last_info: GameLastInfo | None, game_data: dict
    ) -> datetime | None:
        new_gamedt = get_game_datetime(game_data)
        if last_info is None or new_gamedt != last_info.gamedatetime:
            return new_gamedt
        return None

    def get_changed_score_visitor(
        self, last_info: GameLastInfo | None, game_data: dict
    ) -> int | None:
        score_visitor = get_natstat_value(game_data, "score.visitor", int)
        if last_info is None or last_info.score_visitor != score_visitor:  # new game
            return score_visitor
        return None

    def get_changed_score_home(self, last_info: GameLastInfo | None, game_data: dict) -> int | None:
        score_home = get_natstat_value(game_data, "score.home", int)
        if last_info is None or last_info.score_home != score_home:  # new game
            return score_home
        return None

    def get_changed_score_overtime(
        self, last_info: GameLastInfo | None, game_data: dict
    ) -> str | None:
        score_overtime = get_natstat_value(game_data, "score.overtime", str)
        if last_info is None or last_info.score_overtime != score_overtime:  # new game
            return score_overtime
        return None

    def get_play_by_play_changes(
        self, last_info: GameLastInfo | None, game_data: dict
    ) -> list[dict] | None:
        pbps = jmespath.search("stats.playbyplay", game_data)
        if pbps is None:
            return None
        pbps = list(pbps.values())

        online_pbps_ids = {int(item["id"]) for item in pbps}
        if (
            last_info is None or last_info.playbyplay_ids is None
        ):  # new game or no previous playbyplay
            new_pbps_ids = online_pbps_ids
        else:
            new_pbps_ids = online_pbps_ids - last_info.playbyplay_ids

        new_pbps = [item for item in pbps if int(item["id"]) in new_pbps_ids]
        return new_pbps
