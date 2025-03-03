from enum import StrEnum
from typing import TypeAlias


GameDataWithSport: TypeAlias = tuple[str, dict]


class SportTypes(StrEnum):
    baseball = "Baseball"
    hockey = "Hockey"
    american_football = "American Football"
