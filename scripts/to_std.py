"""
This script does:
1. Transform Basketball Games to standardized games table
2. Transform Basketball Teamstatlines to standardized box score table
"""

from typing import TypedDict

import pytz
import sqlalchemy as sa
from sqlalchemy import and_, or_
from sqlalchemy.orm import aliased
from tqdm import tqdm

from app.db.database import (
    BasketballBoxScores,
    BasketballGameStandardized,
    Game,
    GameTeamStatline,
    Season,
    Team,
)
from app.db.session_manager import create_session


class LeagueMapping(TypedDict):
    natstat_id: int
    sport_code: str
    natstat_league_code: str
    natstat_league_name: str
    br_league_name: str | None
    br_league_id: int | None
    notes: str


natstat_to_br_mapping: list[LeagueMapping] = [
    # { # v3
    #     "natstat_id": 112,
    #     "sport_code": "EUROBB",
    #     "natstat_league_code": "EL",
    #     "natstat_league_name": "Euroleague",
    #     "br_league_name": "Turkish Airlines EuroLeague",
    #     "br_league_id": 11,
    #     "notes": "Direct match"
    # },
    # { # v3
    #     "natstat_id": 118,
    #     "sport_code": "EUROBB",
    #     "natstat_league_code": "VTB",
    #     "natstat_league_name": "VTB United League",
    #     "br_league_name": "VTB United League",
    #     "br_league_id": 4,
    #     "notes": "Direct match"
    # },
    # { # v3
    #     "natstat_id": 131,
    #     "sport_code": "EUROBB",
    #     "natstat_league_code": "FRA-A",
    #     "natstat_league_name": "FRA: Ligue Nationale de Basket Pro A",
    #     "br_league_name": "Betclic Élite",
    #     "br_league_id": 10,
    #     "notes": "Same league, different names"
    # },
    # { # v3
    #     "natstat_id": 138,
    #     "sport_code": "EUROBB",
    #     "natstat_league_code": "GRE",
    #     "natstat_league_name": "GRE: Greek HEBA A1",
    #     "br_league_name": "Stoiximan Basket League",
    #     "br_league_id": 9,
    #     "notes": "Same league, different names"
    # },
    # { # both interstat and v3
    #     "natstat_id": 142,
    #     "sport_code": "EUROBB",
    #     "natstat_league_code": "ISR",
    #     "natstat_league_name": "ISR: Basketball Premier League",
    #     "br_league_name": "Ligat Winner",
    #     "br_league_id": 8,
    #     "notes": "Same league, different names"
    # },
    # { # v3
    #     "natstat_id": 143,
    #     "sport_code": "EUROBB",
    #     "natstat_league_code": "ITA",
    #     "natstat_league_name": "ITA: Lega Basket Serie A",
    #     "br_league_name": "Lega Basket Serie A UnipolSai",
    #     "br_league_id": 7,
    #     "notes": "Direct match"
    # },
    # { # v3
    #     "natstat_id": 161,
    #     "sport_code": "EUROBB",
    #     "natstat_league_code": "ESP",
    #     "natstat_league_name": "ESP: Liga ACB",
    #     "br_league_name": "Liga Endesa",
    #     "br_league_id": 6,
    #     "notes": "Same league, different names"
    # },
    # { # v3
    #     "natstat_id": 166,
    #     "sport_code": "EUROBB",
    #     "natstat_league_code": "TUR",
    #     "natstat_league_name": "TUR: Basketball Super League",
    #     "br_league_name": "Türkiye Sigorta Basketbol Süper Ligi",
    #     "br_league_id": 5,
    #     "notes": "Same league, different names"
    # },
    # {
    #     "natstat_id": 171,
    #     "sport_code": "ASIABB",
    #     "natstat_league_code": "AUS",
    #     "natstat_league_name": "AUS: National Basketball League",
    #     "br_league_name": "Hungry Jack's National Basketball League",
    #     "br_league_id": 14,
    #     "notes": "Same league, different names",
    # },
    # {
    #     "natstat_id": 176,
    #     "sport_code": "ASIABB",
    #     "natstat_league_code": "CHN",
    #     "natstat_league_name": "CHN: Chinese Basketball Association",
    #     "br_league_name": "Chinese Basketball Association",
    #     "br_league_id": 13,
    #     "notes": "Direct match",
    # },
    # {
    #     "natstat_id": 189,
    #     "sport_code": "ASIABB",
    #     "natstat_league_code": "KOR",
    #     "natstat_league_name": "KOR: Korea Basketball League",
    #     "br_league_name": None,
    #     "br_league_id": None,
    #     "notes": "No match",
    # },
    # {
    #     "natstat_id": 180,
    #     "sport_code": "ASIABB",
    #     "natstat_league_code": "JPN",
    #     "natstat_league_name": "JPN: B.League",
    #     "br_league_name": None,
    #     "br_league_id": None,
    #     "notes": "No match",
    # },
]


class GameOutsideSeason(Exception):
    ...


#########################
# Main script
#########################


def fetch_natstat_seasons(sport_code, natstat_league_id, session):
    return (
        session.query(Season)
        .filter(
            Season.sport_code == sport_code,
            Season.league_id == natstat_league_id,
        )
        .all()
    )


def get_season_info(date, natstat_seasons):
    matching_natstat_season = None
    for season in natstat_seasons:
        if season.first_game <= date.date() <= season.last_game:
            matching_natstat_season = season
            break

    if matching_natstat_season is None:
        raise GameOutsideSeason()
    return matching_natstat_season


def convert_to_timezone(dt, timezone):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)

    return dt.astimezone(pytz.timezone(timezone))


def safe_division(numerator, denominator, default=0.0):
    return numerator / denominator if denominator != 0 else default


def add_games():
    total_missed_game = 0
    total_saved_game = 0
    existing_games = {}

    with create_session() as original_session:
        existing_games = {g.id for g in original_session.query(BasketballGameStandardized.id).all()}

    print("Got", len(existing_games))

    for league_map in natstat_to_br_mapping:
        natstat_league_id = league_map["natstat_id"]
        sport_code = league_map["sport_code"]
        natstat_league_code = league_map["natstat_league_code"]
        natstat_league_name = league_map["natstat_league_name"]
        print("Start", natstat_league_code)

        with create_session() as original_session:
            natstat_seasons = fetch_natstat_seasons(sport_code, natstat_league_id, original_session)
            oldest_season_start_date = min(s.first_game for s in natstat_seasons)

            # team_league_code = natstat_league_code.split('-')[0]
            team_league_code = natstat_league_code
            isr_teams = (
                original_session.query(Team.code, Team.id)
                .filter(Team.sport_code == sport_code, Team.league_code == team_league_code)
                .subquery()
            )

            HomeTeam = aliased(Team)
            VisitorTeam = aliased(Team)

            # different leagues should be defined differently
            # EUROBB has accurate games_v3.league code to define league
            # others defined by team code prefix
            games_query = (
                original_session.query(
                    # sa.distinct(Game.id),
                    Game,
                    HomeTeam,
                    VisitorTeam,
                )
                # .join(GameV3Data, sa.and_(
                #     Game.sport_code == GameV3Data.sport_code,
                #     Game.id == GameV3Data.id,
                # ))
                .join(
                    HomeTeam,
                    sa.and_(
                        Game.home_code == HomeTeam.code,
                        Game.home_id == HomeTeam.id,
                        HomeTeam.sport_code == sport_code,
                    ),
                )
                .join(
                    VisitorTeam,
                    sa.and_(
                        Game.visitor_code == VisitorTeam.code,
                        Game.visitor_id == VisitorTeam.id,
                        VisitorTeam.sport_code == sport_code,
                    ),
                )
                .filter(
                    Game.status == "Final",
                    Game.sport_code == sport_code,
                    # GameV3Data.league == natstat_league_code,
                    sa.and_(
                        sa.exists().where(
                            sa.and_(
                                isr_teams.c.code == Game.home_code, isr_teams.c.id == Game.home_id
                            )
                        ),
                        sa.exists().where(
                            sa.and_(
                                isr_teams.c.code == Game.visitor_code,
                                isr_teams.c.id == Game.visitor_id,
                            )
                        ),
                    ),
                    Game.gamedatetime >= oldest_season_start_date,
                )
                .order_by(Game.gamedatetime.desc())
            )

            games = games_query.all()
            print(len(games))

            missed_game = 0
            saved_game = 0
            # existing_ids = set()

            for game, home_team, vistor_team in games:
                season = get_season_info(game.gamedatetime, natstat_seasons)
                saved_game += 1

                new_game_id = f"{game.sport_code}/{game.id}"
                if new_game_id in existing_games:
                    continue

                # if new_game_id in existing_ids:
                #     continue
                # existing_ids.add(new_game_id)

                new_game = BasketballGameStandardized(
                    id=new_game_id,
                    season_id=season.id,
                    season_name=season.season_name,
                    league_name=natstat_league_name,
                    league_id=natstat_league_id,
                    game_date_origin=game.gamedatetime,
                    game_date_gmt=convert_to_timezone(game.gamedatetime, "GMT"),
                    game_date_utc=game.gamedatetime,
                    game_date_uk=convert_to_timezone(game.gamedatetime, "Europe/London"),
                    away_team_id=vistor_team.id,
                    away_team=vistor_team.name,
                    away_pts=game.score_visitor,
                    home_team_id=home_team.id,
                    home_team=home_team.name,
                    home_pts=game.score_home,
                    overtimes=game.score_overtime != "N",
                    box_score_href=f"/natstat/{game.sport_code}/{game.id}",
                )
                original_session.add(new_game)

            original_session.commit()
            print(sport_code, natstat_league_code, f"{len(games)=}, {missed_game=}, {saved_game=}")
            total_missed_game += missed_game
            total_saved_game += saved_game
    print(f"END {total_missed_game=} {total_saved_game=}")


def add_box_scores():
    with create_session() as original_session:
        std_games = (
            original_session.query(BasketballGameStandardized)
            .filter(BasketballGameStandardized.league_id == 180)
            .all()
        )
        print("got std games for box score", len(std_games))
        batch_size = 1000
        existing_box_scores = set(original_session.query(BasketballBoxScores.event_id).all())

        for offset in tqdm(range(0, len(std_games), batch_size)):
            batch = std_games[offset : offset + batch_size]
            game_ids_sport_codes = [
                (sport_code, int(game_id))
                for sport_code, game_id in (std_game.id.split("/") for std_game in batch)
            ]
            home_statline = aliased(GameTeamStatline)
            away_statline = aliased(GameTeamStatline)

            filter_conditions = [
                and_(Game.sport_code == sport_code, Game.id == game_id)
                for sport_code, game_id in game_ids_sport_codes
            ]

            game_data = (
                original_session.query(Game, home_statline, away_statline)
                .join(
                    home_statline,
                    and_(
                        Game.id == home_statline.game_id,
                        Game.sport_code == home_statline.sport_code,
                        Game.home_code == home_statline.team_code,
                    ),
                )
                .join(
                    away_statline,
                    and_(
                        Game.id == away_statline.game_id,
                        Game.sport_code == away_statline.sport_code,
                        Game.visitor_code == away_statline.team_code,
                    ),
                )
                .filter(or_(*filter_conditions))
                .all()
            )

            statline_dict = {
                (home.sport_code, home.game_id): (home, away) for _, home, away in game_data
            }
            print("Got game data", len(game_data))

            box_scores = []
            for std_game in batch:
                if std_game.id in existing_box_scores:
                    continue

                sport_code, game_id = std_game.id.split("/")
                try:
                    home_stat, away_stat = statline_dict[(sport_code, int(game_id))]
                except KeyError:
                    print(sport_code, int(game_id), "no stats")
                    continue

                home_DRB = None
                away_DRB = None
                home_poss = None
                away_poss = None
                avg_poss = None
                if home_stat.rebounds is not None and home_stat.field_goals_attempted is not None:
                    home_DRB = float(home_stat.rebounds) - float(home_stat.offensive_rebounds)
                    away_DRB = float(away_stat.rebounds) - float(away_stat.offensive_rebounds)

                    home_poss = (
                        float(home_stat.field_goals_attempted)
                        + 0.4 * float(home_stat.free_throws_attempted)
                        - 1.07
                        * safe_division(
                            float(home_stat.offensive_rebounds),
                            float(home_stat.offensive_rebounds) + away_DRB,
                        )
                        * (
                            float(home_stat.field_goals_attempted)
                            - float(home_stat.field_goals_made)
                        )
                        + float(home_stat.turnovers)
                    )
                    away_poss = (
                        float(away_stat.field_goals_attempted)
                        + 0.4 * float(away_stat.free_throws_attempted)
                        - 1.07
                        * safe_division(
                            float(away_stat.offensive_rebounds),
                            float(away_stat.offensive_rebounds) + home_DRB,
                        )
                        * (
                            float(away_stat.field_goals_attempted)
                            - float(away_stat.field_goals_made)
                        )
                        + float(away_stat.turnovers)
                    )
                    avg_poss = 0.5 * (home_poss + away_poss)

                home_2PA = None
                if home_stat.field_goals_attempted is not None:
                    home_2PA = float(home_stat.field_goals_attempted) - float(
                        home_stat.three_pointers_attempted
                    )
                away_2PA = None
                if away_stat.field_goals_attempted is not None:
                    away_2PA = float(away_stat.field_goals_attempted) - float(
                        away_stat.three_pointers_attempted
                    )

                home_2P = None
                if home_stat.field_goals_made is not None:
                    home_2P = float(home_stat.field_goals_made) - float(
                        home_stat.three_pointers_made
                    )
                away_2P = None
                if away_stat.field_goals_made is not None:
                    away_2P = float(away_stat.field_goals_made) - float(
                        away_stat.three_pointers_made
                    )

                box_score = BasketballBoxScores(
                    season_id=std_game.season_id,
                    event_id=std_game.id,
                    game_date_et=std_game.game_date_origin,
                    home_Tm=home_stat.minutes,
                    home_team_id=std_game.home_team_id,
                    home_MP=home_stat.minutes,
                    home_PTS=home_stat.points,
                    home_FG=home_stat.field_goals_made,
                    home_FGA=home_stat.field_goals_attempted,
                    home_3P=home_stat.three_pointers_made,
                    home_3PA=home_stat.three_pointers_attempted,
                    home_FT=home_stat.free_throws_made,
                    home_FTA=home_stat.free_throws_attempted,
                    home_ORB=home_stat.offensive_rebounds,
                    home_DRB=home_DRB,
                    home_TRB=home_stat.rebounds,
                    home_AST=home_stat.assists,
                    home_STL=home_stat.steals,
                    home_BLK=home_stat.blocks,
                    home_TOV=home_stat.turnovers,
                    home_PF=home_stat.fouls,
                    away_Tm=away_stat.minutes,
                    away_team_id=std_game.away_team_id,
                    away_MP=away_stat.minutes,
                    away_PTS=away_stat.points,
                    away_FG=away_stat.field_goals_made,
                    away_FGA=away_stat.field_goals_attempted,
                    away_3P=away_stat.three_pointers_made,
                    away_3PA=away_stat.three_pointers_attempted,
                    away_FT=away_stat.free_throws_made,
                    away_FTA=away_stat.free_throws_attempted,
                    away_ORB=away_stat.offensive_rebounds,
                    away_DRB=away_DRB,
                    away_TRB=away_stat.rebounds,
                    away_AST=away_stat.assists,
                    away_STL=away_stat.steals,
                    away_BLK=away_stat.blocks,
                    away_TOV=away_stat.turnovers,
                    away_PF=away_stat.fouls,
                    avg_poss=avg_poss,
                    home_poss=home_poss,
                    away_poss=away_poss,
                    home_2PA=home_2PA,
                    away_2PA=away_2PA,
                    home_2P=home_2P,
                    away_2P=away_2P,
                )
                box_scores.append(box_score)
            original_session.bulk_save_objects(box_scores)
            original_session.commit()


def main():
    ...
    # add_games()
    # add_box_scores()


if __name__ == "__main__":
    main()
