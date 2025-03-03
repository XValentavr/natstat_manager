import sys
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel


class GameType(StrEnum):
    today = "today"
    early = "early"
    live = "live"
    early_final = "early_final"


class GameShort(BaseModel):
    game_id: int
    sport_code: str

    @property
    def key(self):
        return f"{self.sport_code}_{self.game_id}"

    def sizeof(self):
        return (
            sys.getsizeof(self)
            + sys.getsizeof(self.game_id)
            + sys.getsizeof(self.sport_code)
            + sys.getsizeof(self.key)
        )


class GameLastInfo(BaseModel):
    game_short: GameShort
    gamedatetime: datetime
    status: str
    score_visitor: int | None = None
    score_home: int | None = None
    score_overtime: str | None = None
    playbyplay_ids: set[int] | None = None

    def sizeof(self):
        size = (
            sys.getsizeof(self)
            + self.game_short.sizeof()
            + sys.getsizeof(self.gamedatetime)
            + sys.getsizeof(self.status)
            + sys.getsizeof(self.score_visitor)
            + sys.getsizeof(self.score_home)
            + sys.getsizeof(self.score_overtime)
            + sys.getsizeof(self.playbyplay_ids)
        )
        if self.playbyplay_ids:
            size += sum(sys.getsizeof(id) for id in self.playbyplay_ids)
        return size


class GameChanges(BaseModel):
    game_short: GameShort

    status: str | None
    gamedatetime: datetime | None
    score_visitor: int | None = None
    score_home: int | None = None
    score_overtime: str | None = None
    playbyplay_changes: list[dict] | None = None

    game_data: dict
