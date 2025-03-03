from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import hashlib
import time

from pytz import utc
from sqlalchemy import func, select, text, and_, update, or_, exists
from sqlalchemy.orm import Session
import jmespath

from app.common.logging import get_logger
from app.common.logging import log_with_time_info
from app.db.database import (
    Game,
    GameStatPlayByPlay,
    League,
    GamePlayerStatline,
    GamePlayer,
    GameTeamStatline,
    GameLineup,
    GameText,
    GamePeriodScore,
    Player,
    Sport,
    Team,
    GamesToMonitor,
    GameUpdates,
    GameUpdatesSSOT,
)
from app.db.session_manager import SessionProd, provide_session
from app.common.utils import (
    get_utc_now,
    natstat_get_str,
    get_natstat_value,
    transform_sequence,
    ny_tz,
)
from app.common.types import GameChanges


def log(msg: str):
    get_logger().info(msg)


class DbClient:
    @log_with_time_info
    @provide_session(SessionProd)
    def test(self, session: Optional[Session]):
        query = text("SELECT 1")
        session.execute(query)

    @log_with_time_info
    @provide_session(SessionProd)
    def save_sports(self, session: Optional[Session], sports):
        for item in sports:
            sport = Sport(
                code=item["code"],
                name=item["name"],
                sport=item["sport"],
                seasons=int(item["seasons"]),
                first=int(item["first"]),
                statsbegin=int(item["statsbegin"]) if "statsbegin" in item else None,
                last=int(item["last"]),
                inplay=item["inplay"] == "Y",
            )
            session.merge(sport)
        session.commit()

    @log_with_time_info
    @provide_session(SessionProd)
    def get_basketball_sports(self, session: Optional[Session]) -> list[Sport]:
        basketball_sports = session.query(Sport).filter(Sport.sport == "Basketball").all()
        return basketball_sports

    @log_with_time_info
    @provide_session(SessionProd)
    def get_sports_except_basketball_and_football(self, session: Session) -> list[Sport]:
        return (
            session.query(Sport)
            .filter(Sport.sport != "Basketball", Sport.sport != "World Football")
            .all()
        )

    @log_with_time_info
    @provide_session(SessionProd)
    def get_notscheduled_games(self, session: Optional[Session]) -> list[Sport]:
        games = session.query(Game.id, Game.sport_code).filter(Game.status != "Scheduled").all()
        return games

    @log_with_time_info
    @provide_session(SessionProd)
    def get_basketball_games_missing_data(self, session: Optional[Session]) -> list[Game]:
        return (
            session.query(Game.id, Game.sport_code)
            .join(Sport, Game.sport_code == Sport.code)
            .filter(
                Sport.sport.in_(["Basketball"]),
                (
                    ~exists().where(
                        (GameText.game_id == Game.id) & (GameText.sport_code == Game.sport_code)
                    )
                )
                | (
                    ~exists().where(
                        (GamePeriodScore.game_id == Game.id)
                        & (GamePeriodScore.sport_code == Game.sport_code)
                    )
                )
                | (
                    ~exists().where(
                        (GamePlayer.game_id == Game.id) & (GamePlayer.sport_code == Game.sport_code)
                    )
                ),
                Game.status != "Scheduled",
            )
            .all()
        )

    @log_with_time_info
    @provide_session(SessionProd)
    def get_games_missing_data(self, session: Optional[Session]) -> list[Game]:
        return (
            session.query(Game.id, Game.sport_code)
            .filter(
                (
                    ~exists().where(
                        (GameText.game_id == Game.id) & (GameText.sport_code == Game.sport_code)
                    )
                )
                | (
                    ~exists().where(
                        (GamePeriodScore.game_id == Game.id)
                        & (GamePeriodScore.sport_code == Game.sport_code)
                    )
                )
                | (
                    ~exists().where(
                        (GamePlayer.game_id == Game.id) & (GamePlayer.sport_code == Game.sport_code)
                    )
                ),
                Game.status != "Scheduled",
            )
            .all()
        )

    @log_with_time_info
    @provide_session(SessionProd)
    def get_games_to_monitor_all(self, session: Optional[Session]) -> list[Sport]:
        games = session.query(GamesToMonitor).order_by(GamesToMonitor.created).all()
        return games

    @log_with_time_info
    @provide_session(SessionProd)
    def get_future_games(self, session: Optional[Session]) -> list[Sport]:
        current_time = get_utc_now()
        query = select(Game).where(Game.gamedatetime > current_time).order_by(Game.gamedatetime)
        return session.execute(query).scalars().all()

    @log_with_time_info
    @provide_session(SessionProd)
    def get_recent_and_upcoming_games(self, session: Optional[Session]) -> list[Game]:
        """
        Fetch games to have live updates.
        Currently we need to watch only for basketball games.
        """
        current_time = get_utc_now()
        four_hours_ago = current_time - timedelta(hours=4)
        one_day_future = current_time + timedelta(days=1)

        query = (
            select(Game)
            .join(Sport, Game.sport_code == Sport.code)
            .where(
                and_(
                    Sport.sport.in_(["Basketball"]),
                    Game.gamedatetime > four_hours_ago,
                    Game.gamedatetime < one_day_future,
                    or_(
                        Game.removed_from_natstat == False,  # noqa
                        Game.removed_from_natstat == None,  # noqa
                    ),
                )
            )
            .order_by(Game.gamedatetime)
        )

        return session.execute(query).scalars().all()

    @log_with_time_info
    @provide_session(SessionProd)
    def get_future_games_to_monitor(self, session: Optional[Session]) -> list[Sport]:
        current_time = get_utc_now()
        two_days_later = current_time + timedelta(days=5)
        query = (
            select(Game)
            .where(and_(Game.gamedatetime > current_time, Game.gamedatetime < two_days_later))
            .order_by(Game.gamedatetime)
        )
        return session.execute(query).scalars().all()

    @log_with_time_info
    @provide_session(SessionProd)
    def get_games_wo_pbp(self, session: Optional[Session]) -> list[Sport]:
        """Not Scheduled games without play by play statistics"""
        subquery = (
            session.query(GameStatPlayByPlay.game_id)
            .filter(GameStatPlayByPlay.sport_code == Game.sport_code)
            .distinct()
        )

        return (
            session.query(Game.id, Game.sport_code)
            .filter(Game.status != "Scheduled")
            .filter(~Game.id.in_(subquery))
            .all()
        )

    @log_with_time_info
    @provide_session(SessionProd)
    def get_pbp_count(self, session: Optional[Session]) -> list[Sport]:
        return session.query(func.count(GameStatPlayByPlay.id)).scalar()

    @log_with_time_info
    @provide_session(SessionProd)
    def get_teams_count(self, session: Optional[Session]) -> list[Sport]:
        return session.query(func.count(Player.id)).scalar()

    @log_with_time_info
    @provide_session(SessionProd)
    def get_top_pbp(self, session: Optional[Session]) -> list[Sport]:
        return session.query(GameStatPlayByPlay).limit(100).all()

    @log_with_time_info
    @provide_session(SessionProd)
    def get_pbp_stats(self, session: Optional[Session]) -> list[Sport]:
        return (
            session.query(
                GameStatPlayByPlay.sport_code, func.count(GameStatPlayByPlay.id).label("count")
            )
            .group_by(GameStatPlayByPlay.sport_code)
            .order_by(func.count(GameStatPlayByPlay.id).desc())
            .all()
        )

    @log_with_time_info
    @provide_session(SessionProd)
    def add_gtm_if_notexists(self, session: Optional[Session], game: Game):
        if (
            not session.query(GamesToMonitor)
            .filter_by(game_id=game.id, sport_code=game.sport_code)
            .first()
        ):
            entry = GamesToMonitor(
                game_id=game.id,
                sport_code=game.sport_code,
                startdatetime=game.gamedatetime,
                status=game.status,
                created=get_utc_now(),
                last_checked=None,
            )
            session.add(entry)
        session.commit()

    @log_with_time_info
    @provide_session(SessionProd)
    def get_games_to_monitor(self, session: Optional[Session]) -> list[GamesToMonitor]:
        current_time = get_utc_now()
        five_hours_ago = current_time - timedelta(hours=5)
        six_hours_future = current_time + timedelta(hours=6)
        return (
            session.query(GamesToMonitor)
            .where(
                GamesToMonitor.startdatetime > five_hours_ago,
                GamesToMonitor.startdatetime < six_hours_future,
            )
            .all()
        )

    @log_with_time_info
    @provide_session(SessionProd)
    def get_game_updates(
        self, session: Optional[Session], sport_code: str, game_id: int
    ) -> list[GameUpdates]:
        return (
            session.query(GameUpdates)
            .where(
                and_(
                    GameUpdates.sport_code == sport_code,
                    GameUpdates.game_id == game_id,
                )
            )
            .all()
        )

    @log_with_time_info
    @provide_session(SessionProd)
    def handle_game_update(
        self, session: Optional[Session], gamem: GamesToMonitor, game_data: Dict
    ):
        last_update = (
            session.query(GameUpdates)
            .filter_by(game_id=gamem.game_id, sport_code=gamem.sport_code)
            .order_by(GameUpdates.update_time.desc())
            .first()
        )

        score_visitor = jmespath.search("score.visitor", game_data)
        if score_visitor is not None:
            score_visitor = int(score_visitor)
        score_home = jmespath.search("score.home", game_data)
        if score_home is not None:
            score_home = int(score_home)
        score_overtime = jmespath.search("score.overtime", game_data)

        status = game_data["status"]
        pbp = jmespath.search("stats.playbyplay", game_data)
        pbp_json = None if pbp is None else json.dumps(pbp, sort_keys=True)
        pbp_hash = None if pbp is None else hashlib.sha256(pbp_json.encode("utf-8")).hexdigest()

        is_changed = (
            last_update is None
            or status != last_update.status
            or score_visitor != last_update.score_visitor
            or score_home != last_update.score_home
            or score_overtime != last_update.score_overtime
            or pbp_hash != last_update.play_by_play_hash
        )
        # Final status case have many
        # one of case example: "Final - Forfeit Home"
        is_final = "final" in game_data["status"].lower()

        if is_changed:
            new_update = GameUpdates(
                game_id=gamem.game_id,
                sport_code=gamem.sport_code,
                status=status,
                update_time=get_utc_now(),
                score_visitor=score_visitor,
                score_home=score_home,
                score_overtime=score_overtime,
                play_by_play_hash=pbp_hash,
                play_by_play_json=pbp_json,
                is_final=is_final,
            )
            session.add(new_update)

        update_values = {"last_checked": get_utc_now()}

        # if is_final:
        #     update_values['stop_monitor_at'] = get_utc_now() + timedelta(minutes=10)

        stmt = (
            update(GamesToMonitor)
            .where(
                GamesToMonitor.game_id == gamem.game_id,
                GamesToMonitor.sport_code == gamem.sport_code,
            )
            .values(**update_values)
        )
        session.execute(stmt)
        session.commit()

    @log_with_time_info
    @provide_session(SessionProd)
    def save_game_update_ssot(
        self, session: Optional[Session], gamem: GamesToMonitor, data_text: str
    ):
        entry = GameUpdatesSSOT(
            game_id=gamem.game_id,
            sport_code=gamem.sport_code,
            created=get_utc_now(),
            data_text=data_text,
        )
        session.add(entry)
        session.commit()

    @log_with_time_info
    @provide_session(SessionProd)
    def set_monitor_stuck(self, session: Optional[Session], gamem: GamesToMonitor):
        gamem.is_stuck_scheduled = True
        session.commit()

    @log_with_time_info
    @provide_session(SessionProd)
    def save_teams(self, session: Optional[Session], sport_code: str, teams: List[Dict]):
        for team_data in teams:
            if "code" not in team_data or isinstance(team_data["code"], dict):
                # Natstat has some anomalies in teams data
                log(f"Got None code team for {sport_code} {team_data.get('id')}")
                continue

            league_code = None
            if "-" in team_data["code"]:
                league_code = team_data["code"].split("-")[0]
            team = Team(
                id=int(team_data["id"]),
                code=team_data["code"],
                sport_code=sport_code,
                league_code=league_code,
                name=natstat_get_str("name", team_data),
                nickname=natstat_get_str("nickname", team_data),
                fullname=natstat_get_str("fullname", team_data),
                active=team_data.get("active", "") == "Y",
            )
            session.merge(team)
        session.commit()

    @log_with_time_info
    @provide_session(SessionProd)
    def save_players(self, session: Optional[Session], sport_code: str, players: List[Dict]):
        to_add = []
        ids = [int(data["id"]) for data in players]

        existing_players = set(
            session.query(Player)
            .filter(and_(Player.id.in_(ids), Player.sport_code == sport_code))
            .all()
        )
        existing_players_ids = {game.id for game in existing_players}

        for player_data in players:
            player_id = int(player_data["id"])
            if player_id in existing_players_ids:
                continue

            player = Player(
                id=int(player_data["id"]),
                sport_code=sport_code,
                name=natstat_get_str("name", player_data),
                position=natstat_get_str("position", player_data),
                jersey=natstat_get_str("jersey", player_data),
                experience=natstat_get_str("experience", player_data),
            )
            to_add.append(player)
        session.bulk_save_objects(to_add)
        session.commit()

    @provide_session(SessionProd)
    def get_games_in_date_range(
        self, session: Session, sport_code: str, start_date: datetime, end_date: datetime
    ) -> List[Game]:
        return (
            session.query(Game)
            .filter(
                and_(
                    Game.sport_code == sport_code,
                    Game.gamedatetime >= start_date,
                    Game.gamedatetime <= end_date,
                )
            )
            .all()
        )

    @provide_session(SessionProd)
    def mark_games_as_removed(self, session: Session, sport_code: str, game_ids: List[int]) -> None:
        session.query(Game).filter(
            and_(Game.sport_code == sport_code, Game.id.in_(game_ids))
        ).update({Game.removed_from_natstat: True}, synchronize_session=False)
        session.commit()

    @log_with_time_info
    @provide_session(SessionProd)
    def upsert_games(self, session: Optional[Session], sport_code: str, games: List[Dict]):
        game_ids = [int(game_data["id"]) for game_data in games]

        existing_games = set(
            session.query(Game)
            .filter(and_(Game.id.in_(game_ids), Game.sport_code == sport_code))
            .all()
        )
        {game.id for game in existing_games}
        db_games_dict = {game.id: game for game in existing_games}

        to_add = []
        for game_data in games:
            game_id = int(game_data["id"])

            datetime_str = game_data["gameday"]
            # edge case when gameday may be 2014-10-00
            if datetime_str.endswith("-00"):
                datetime_str = datetime_str.replace("-00", "-01")

            if isinstance(game_data["starttime"], dict):
                gamedatetime = datetime.strptime(datetime_str, "%Y-%m-%d")
            else:
                datetime_str += " " + game_data["starttime"]
                gamedatetime = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
            gamedatetime = ny_tz.localize(gamedatetime).astimezone(utc)
            gamedatetime = gamedatetime.replace(tzinfo=None)

            if game_id in db_games_dict:
                game = db_games_dict[game_id]
            else:
                game = Game(id=int(game_data["id"]))

            game.removed_from_natstat = False
            game.sport_code = sport_code
            game.gamedatetime = gamedatetime
            game.status = game_data["status"]
            game.visitor_id = int(game_data["visitor"]["id"])
            game.home_id = int(game_data["home"]["id"])

            game.visitor_code = (
                game_data["visitor"]["code"]
                if isinstance(game_data["visitor"]["code"], str)
                else None
            )
            game.home_code = (
                game_data["home"]["code"] if isinstance(game_data["home"]["code"], str) else None
            )
            if "winner" in game_data and game_data["winner"]:
                winner_id = game_data["winner"]["id"]
                # Edge case. they just have array for some reason with 2 same values
                is_arr_val = isinstance(winner_id, list)
                game.winner_id = int(game_data["winner"]["id"]) if not is_arr_val else winner_id[0]
                game.winner_code = (
                    game_data["winner"]["code"]
                    if isinstance(game_data["winner"]["code"], str)
                    else None
                )
                if is_arr_val:
                    game.winner_code = game_data["winner"]["code"][0]
            else:
                game.winner_id = None
                game.winner_code = None

            if "loser" in game_data and game_data["loser"]:
                losser_id = game_data["loser"]["id"]
                is_arr_val = isinstance(losser_id, list)
                game.loser_id = int(game_data["loser"]["id"]) if not is_arr_val else losser_id[0]
                game.loser_code = (
                    game_data["loser"]["code"]
                    if isinstance(game_data["loser"]["code"], str)
                    else None
                )
                if is_arr_val:
                    game.loser_code = game_data["loser"]["code"][0]
            else:
                game.loser_id = None
                game.loser_code = None

            if "score" in game_data and game_data["score"]:
                game.score_visitor = int(game_data["score"]["visitor"])
                game.score_home = int(game_data["score"]["home"])
                game.score_overtime = get_natstat_value(game_data, "score.overtime", str)
            else:
                game.score_visitor = None
                game.score_home = None
                game.score_overtime = None

            if game_id in db_games_dict:
                session.merge(game)
            else:
                to_add.append(game)

        session.bulk_save_objects(to_add)
        session.commit()

    @log_with_time_info
    @provide_session(SessionProd)
    def save_pbp(self, session: Optional[Session], sport_code: str, game_data):
        if "playbyplay" not in game_data["stats"]:
            return

        to_add = []
        for play_data in game_data["stats"]["playbyplay"].values():
            play_by_play = GameStatPlayByPlay(
                id=int(play_data["id"]),
                game_id=int(game_data["id"]),
                sport_code=sport_code,
                event=play_data.get("event"),
                period=int(play_data["period"]),
                sequence=transform_sequence(play_data.get("sequence")),
                explanation=play_data.get("explanation"),
                team_id=int(play_data["team"]["id"]),
                team_code=play_data["team"]["code"],
                scoringplay=play_data.get("scoringplay"),
                tags=play_data.get("tags"),
                thediff=play_data.get("thediff"),
            )
            if "players" in play_data:
                play_by_play.player_primary_id = (
                    int(play_data["players"]["primary"]["id"])
                    if "primary" in play_data["players"]
                    and isinstance(play_data["players"]["primary"]["id"], int)
                    else None
                )
                play_by_play.player_secondary_id = (
                    int(play_data["players"]["secondary"]["id"])
                    if "secondary" in play_data["players"]
                    else None
                )
                play_by_play.player_pitcher_id = (
                    int(play_data["players"]["pitcher"]["id"])
                    if "pitcher" in play_data["players"]
                    else None
                )
            to_add.append(play_by_play)
        session.bulk_save_objects(to_add)
        session.commit()

    @log_with_time_info
    @provide_session(SessionProd)
    def save_pbps(self, session: Optional[Session], games, is_live=False):
        created_at = get_utc_now()
        to_add = []
        for sport_code, game_data in games:
            if game_data and "playbyplay" not in game_data["stats"]:
                continue
            game_id = int(game_data["id"])

            values = list(game_data["stats"]["playbyplay"].values())
            for start in range(0, len(values), 500):
                values_batch = values[start : start + 500]
                existing_pbp_data = (
                    session.query(GameStatPlayByPlay.id, GameStatPlayByPlay.distance)
                    .filter(
                        and_(
                            GameStatPlayByPlay.game_id == game_id,
                            GameStatPlayByPlay.sport_code == sport_code,
                            GameStatPlayByPlay.id.in_(
                                [int(play_data["id"]) for play_data in values_batch]
                            ),
                        )
                    )
                    .all()
                )
                existing_pbp = {pbp.id: pbp.distance for pbp in existing_pbp_data}

                for play_data in values_batch:
                    play_id = int(play_data["id"])
                    distance = get_natstat_value(play_data, "distance", int)
                    if play_id in existing_pbp and existing_pbp[play_id] != distance:
                        # Temporary solution to add new distance value to existing play by plays
                        session.query(GameStatPlayByPlay).filter_by(
                            id=play_id, game_id=game_id, sport_code=sport_code
                        ).update({"distance": distance})
                        continue
                    if play_id in existing_pbp:
                        continue

                    play_by_play = {
                        "id": play_id,
                        "game_id": game_id,
                        "sport_code": sport_code,
                        "event": play_data.get("event"),
                        "period": get_natstat_value(play_data, "period", str),
                        "sequence": transform_sequence(play_data.get("sequence")),
                        "explanation": (
                            play_data.get("explanation")
                            if not isinstance(play_data.get("explanation"), dict)
                            else None
                        ),
                        "team_id": get_natstat_value(play_data, "team.id", int),
                        "team_code": get_natstat_value(play_data, "team.code", str),
                        "scoringplay": play_data.get("scoringplay"),
                        "tags": play_data.get("tags"),
                        "thediff": play_data.get("thediff"),
                        "player_primary_id": get_natstat_value(
                            play_data, "players.primary.id", int
                        ),
                        "player_secondary_id": get_natstat_value(
                            play_data, "players.secondary.id", int
                        ),
                        "player_pitcher_id": get_natstat_value(
                            play_data, "players.pitcher.id", int
                        ),
                        "distance": get_natstat_value(play_data, "distance", int),
                    }
                    if is_live:
                        play_by_play["created_at"] = created_at
                    to_add.append(play_by_play)

        if to_add:
            sql = text(
                """
                INSERT INTO game_stat_playbyplay (
                    id, game_id, sport_code, event, period, sequence,
                    explanation, team_id, team_code, scoringplay, tags,
                    thediff, player_primary_id, player_secondary_id, player_pitcher_id, distance
                )
                VALUES (:id, :game_id, :sport_code, :event, :period, :sequence,
                        :explanation, :team_id, :team_code, :scoringplay, :tags,
                        :thediff, :player_primary_id, :player_secondary_id, :player_pitcher_id,
                        :distance)
            """
            )

            session.execute(sql, to_add)
        session.commit()

    @log_with_time_info
    @provide_session(SessionProd)
    def save_leagues(
        self, session: Optional[Session], sport_code: str, leagues: list[dict[str, str]]
    ):
        for league in leagues:
            code = natstat_get_str("code", league)
            # Example nba doesn't have code.
            # it has {} as value.
            if code is None:
                log(f"Got None code league for {sport_code}")
                code = sport_code
            entry = League(
                id=int(league["id"]),
                code=code,
                sport_code=sport_code,
                name=league.get("name"),
                factor=league.get("factor"),
                active=league.get("active", "") == "Y",
            )
            session.merge(entry)
        session.commit()

    @log_with_time_info
    @provide_session(SessionProd)
    def save_game_details(self, session: Optional[Session], games):
        start_time = time.time()

        text_to_add = []
        periods_to_add = []
        players_to_add = []
        players_startline_to_add = []
        teams_startline_to_add = []
        lineups_to_add = []

        total_text_time = 0
        total_periods_time = 0
        total_players_time = 0
        total_playerstat_time = 0
        total_teamstat_time = 0
        total_lineup_time = 0

        existing_game_text_start = time.time()
        game_ids_and_sport_codes = [
            (int(game_data["id"]), sport_code) for sport_code, game_data in games
        ]
        existing_game_texts = (
            session.query(GameText.game_id, GameText.sport_code)
            .filter(
                or_(
                    and_(GameText.game_id == game_id, GameText.sport_code == sport_code)
                    for game_id, sport_code in game_ids_and_sport_codes
                )
            )
            .all()
        )
        existing_game_text_set = {
            (game_id, sport_code) for game_id, sport_code in existing_game_texts
        }
        existing_game_text_time = time.time() - existing_game_text_start

        periods_start = time.time()
        existing_periods = (
            session.query(
                GamePeriodScore.game_id,
                GamePeriodScore.sport_code,
                GamePeriodScore.period,
                GamePeriodScore.is_visitor,
            )
            .filter(
                or_(
                    and_(
                        GamePeriodScore.game_id == game_id, GamePeriodScore.sport_code == sport_code
                    )
                    for game_id, sport_code in game_ids_and_sport_codes
                )
            )
            .all()
        )

        existing_periods_dict = {}
        for game_id, sport_code, period, is_visitor in existing_periods:
            if (game_id, sport_code) not in existing_periods_dict:
                existing_periods_dict[(game_id, sport_code)] = set()
            existing_periods_dict[(game_id, sport_code)].add((period, is_visitor))
        total_periods_time += time.time() - periods_start

        for sport_code, game_data in games:
            game_id = int(game_data["id"])

            text_start = time.time()
            if (game_id, sport_code) not in existing_game_text_set:
                text_to_add.append(
                    GameText(
                        game_id=game_id,
                        sport_code=sport_code,
                        story=get_natstat_value(game_data, "text.story", str),
                        boxheader=get_natstat_value(game_data, "text.boxheader", str),
                        boxscore=get_natstat_value(game_data, "text.boxscore", str),
                        star=get_natstat_value(game_data, "text.star", str),
                    )
                )
            total_text_time += time.time() - text_start

            periods_start = time.time()
            periods_to_add += self.save_gameperiods(game_data, sport_code, existing_periods_dict)
            total_periods_time += time.time() - periods_start

            if "players" in game_data:
                players_start = time.time()
                players_to_add += self.save_gameplayers(
                    session, game_data, sport_code, game_data["players"]
                )
                total_players_time += time.time() - players_start

            if "stats" not in game_data:
                continue

            if "playerstatline" in game_data["stats"]:
                playerstat_start = time.time()
                players_startline_to_add += self.save_playerstatlines(
                    session, game_data, sport_code, game_data["stats"]["playerstatline"]
                )
                total_playerstat_time += time.time() - playerstat_start

            if "teamstatline" in game_data["stats"]:
                teamstat_start = time.time()
                teams_startline_to_add += self.save_teamstatlines(
                    session, game_data, sport_code, game_data["stats"]["teamstatline"]
                )
                total_teamstat_time += time.time() - teamstat_start

            if "lineups" in game_data["stats"]:
                lineup_start = time.time()
                lineups_to_add += self.save_lineups(
                    session, game_data, sport_code, game_data["stats"]["lineups"]
                )
                total_lineup_time += time.time() - lineup_start

        bulk_save_start = time.time()
        session.bulk_save_objects(text_to_add)

        if periods_to_add:
            sql = text(
                """
                INSERT INTO game_periodscores (
                    game_id, sport_code, period, score, is_visitor
                )
                VALUES (:game_id, :sport_code, :period, :score, :is_visitor)
                """
            )

            session.execute(sql, periods_to_add)

        session.bulk_save_objects(players_to_add)
        session.bulk_save_objects(players_startline_to_add)
        session.bulk_save_objects(teams_startline_to_add)
        if lineups_to_add:
            sql = text(
                """
                INSERT INTO game_lineups (
                    id, game_id, sport_code, team_id, team_code, lineup_players, possessions,
                    offensive_points_per_possession, defensive_points_per_possession,
                    efficiency_margin, points_scored, points_allowed, plus_minus,
                    field_goals_made, field_goals_allowed,
                    field_goals_margin, field_goals_attempted, field_goals_attempted_allowed,
                    field_goals_attempted_margin, three_pointers_made, three_pointers_allowed,
                    three_pointers_margin, three_pointers_attempted,
                    three_pointers_attempted_allowed,
                    free_throws_made, free_throws_allowed, free_throws_attempted,
                    free_throws_attempted_allowed, rebounds, rebounds_allowed, rebounds_margin,
                    assists, assists_allowed, assists_margin, blocks, blocks_allowed,
                    blocks_margin, steals, steals_allowed, steals_margin, turnovers,
                    turnovers_allowed, turnovers_margin,
                    personal_fouls, personal_fouls_drawn, second_chance_points,
                    second_chance_points_allowed,
                    second_chance_points_margin, fast_break_points, fast_break_points_allowed,
                    fast_break_points_margin, points_off_turnovers, points_off_turnovers_allowed,
                    points_off_turnovers_margin, points_in_the_paint, points_in_the_paint_allowed,
                    points_in_the_paint_margin, player_1_id, player_2_id, player_3_id,
                    player_4_id, player_5_id
                ) VALUES (
                    :id, :game_id, :sport_code, :team_id, :team_code, :lineup_players, :possessions,
                    :offensive_points_per_possession, :defensive_points_per_possession,
                    :efficiency_margin, :points_scored, :points_allowed, :plus_minus,
                    :field_goals_made, :field_goals_allowed,
                    :field_goals_margin, :field_goals_attempted, :field_goals_attempted_allowed,
                    :field_goals_attempted_margin, :three_pointers_made, :three_pointers_allowed,
                    :three_pointers_margin, :three_pointers_attempted,
                    :three_pointers_attempted_allowed,
                    :free_throws_made, :free_throws_allowed, :free_throws_attempted,
                    :free_throws_attempted_allowed, :rebounds, :rebounds_allowed, :rebounds_margin,
                    :assists, :assists_allowed, :assists_margin, :blocks, :blocks_allowed,
                    :blocks_margin, :steals, :steals_allowed, :steals_margin, :turnovers,
                    :turnovers_allowed, :turnovers_margin,
                    :personal_fouls, :personal_fouls_drawn, :second_chance_points,
                    :second_chance_points_allowed,
                    :second_chance_points_margin, :fast_break_points, :fast_break_points_allowed,
                    :fast_break_points_margin, :points_off_turnovers, :points_off_turnovers_allowed,
                    :points_off_turnovers_margin, :points_in_the_paint,
                    :points_in_the_paint_allowed,
                    :points_in_the_paint_margin, :player_1_id, :player_2_id, :player_3_id,
                    :player_4_id, :player_5_id
                )
            """
            )

            session.execute(sql, lineups_to_add)

        session.commit()
        bulk_save_time = time.time() - bulk_save_start

        total_time = time.time() - start_time

        log(f"Time taken to get existing_game_text_set: {existing_game_text_time:.2f} seconds")
        log(f"Total time taken for text processing: {total_text_time:.2f} seconds")
        log(f"Total time taken for periods: {total_periods_time:.2f} seconds")
        log(f"Total time taken for players: {total_players_time:.2f} seconds")
        log(f"Total time taken for player stats: {total_playerstat_time:.2f} seconds")
        log(f"Total time taken for team stats: {total_teamstat_time:.2f} seconds")
        log(f"Total time taken for lineups: {total_lineup_time:.2f} seconds")
        log(f"Time taken for bulk saves and commit: {bulk_save_time:.2f} seconds")
        log(f"Total time taken for save_game_details: {total_time:.2f} seconds")

    def save_gameperiods(self, game_data, sport_code: str, existing_periods_dict):
        game_id = int(game_data["id"])
        to_add = []

        existing_periods_set = existing_periods_dict.get((game_id, sport_code), set())

        for is_visitor in [True, False]:
            line_data = game_data["visitor" if is_visitor else "home"].get("line", {})
            for period, score in line_data.items():
                pnum = int(period[1:])
                if (pnum, is_visitor) in existing_periods_set:
                    continue
                if isinstance(score, dict):
                    continue
                to_add.append(
                    {
                        "game_id": game_id,
                        "sport_code": sport_code,
                        "period": pnum,
                        "score": int(score),
                        "is_visitor": is_visitor,
                    }
                )

        return to_add

    def save_gameplayers(
        self, session: Optional[Session], game_data, sport_code: str, players: Dict
    ):
        to_add = []
        game_id = int(game_data["id"])

        existing_player_ids = set(
            i[0]
            for i in session.query(GamePlayer.player_id)
            .filter(
                and_(
                    GamePlayer.player_id.in_([int(data["id"]) for data in players.values()]),
                    GamePlayer.sport_code == sport_code,
                    GamePlayer.game_id == game_id,
                )
            )
            .with_entities(GamePlayer.player_id)
            .all()
        )

        for player_data in players.values():
            player_id = int(player_data["id"])
            if player_id in existing_player_ids:
                continue
            to_add.append(
                GamePlayer(
                    game_id=game_id,
                    sport_code=sport_code,
                    player_id=player_id,
                    team_id=get_natstat_value(player_data, "team.id", int),
                    team_code=get_natstat_value(player_data, "team.code", str),
                    position=get_natstat_value(player_data, "position", str),
                    starter=get_natstat_value(player_data, "starter", str),
                )
            )
        return to_add

    def save_playerstatlines(
        self, session: Optional[Session], game_data, sport_code: str, playerstatlines: Dict
    ):
        to_add = []
        game_id = int(game_data["id"])

        existing_statline_ids = set(
            i[0]
            for i in session.query(GamePlayerStatline.id)
            .filter(
                and_(
                    GamePlayerStatline.id.in_(
                        [int(data["id"]) for data in playerstatlines.values()]
                    ),
                    GamePlayerStatline.sport_code == sport_code,
                    GamePlayerStatline.game_id == game_id,
                )
            )
            .with_entities(GamePlayerStatline.id)
            .all()
        )

        for statline_data in playerstatlines.values():
            statline_id = get_natstat_value(statline_data, "id", int)
            if statline_id is None or statline_id in existing_statline_ids:
                continue
            to_add.append(
                GamePlayerStatline(
                    id=statline_id,
                    game_id=game_id,
                    sport_code=sport_code,
                    player_id=get_natstat_value(statline_data, "player_id", int),
                    position=get_natstat_value(statline_data, "position", str),
                    starter=get_natstat_value(statline_data, "starter", str),
                    team_id=get_natstat_value(statline_data, "team.id", int),
                    team_code=get_natstat_value(statline_data, "team.code", str),
                    minutes=get_natstat_value(statline_data, "min", str),
                    points=get_natstat_value(statline_data, "pts", str),
                    field_goals_made=get_natstat_value(statline_data, "fgm", str),
                    field_goals_attempted=get_natstat_value(statline_data, "fga", str),
                    three_pointers_made=get_natstat_value(statline_data, "threefm", str),
                    three_pointers_attempted=get_natstat_value(statline_data, "threefa", str),
                    free_throws_made=get_natstat_value(statline_data, "ftm", str),
                    free_throws_attempted=get_natstat_value(statline_data, "fta", str),
                    rebounds=get_natstat_value(statline_data, "reb", str),
                    assists=get_natstat_value(statline_data, "ast", str),
                    steals=get_natstat_value(statline_data, "stl", str),
                    blocks=get_natstat_value(statline_data, "blk", str),
                    offensive_rebounds=get_natstat_value(statline_data, "oreb", str),
                    turnovers=get_natstat_value(statline_data, "to", str),
                    personal_fouls=get_natstat_value(statline_data, "pf", str),
                    field_goal_percentage=get_natstat_value(statline_data, "fgpct", str),
                    two_point_field_goal_percentage=get_natstat_value(
                        statline_data, "twofgpct", str
                    ),
                    free_throw_percentage=get_natstat_value(statline_data, "ftpct", str),
                    usage_percentage=get_natstat_value(statline_data, "usgpct", str),
                    efficiency=get_natstat_value(statline_data, "eff", str),
                    performance_score=get_natstat_value(statline_data, "perfscore", str),
                    performance_score_season_avg=get_natstat_value(
                        statline_data, "perfscoreseasonavg", str
                    ),
                    performance_score_season_avg_deviation=get_natstat_value(
                        statline_data, "perfscoreseasonavgdev", str
                    ),
                    statline=get_natstat_value(statline_data, "statline", str),
                )
            )

        return to_add

    def save_teamstatlines(
        self, session: Optional[Session], game_data, sport_code: str, teamstatlines: Dict
    ):
        to_add = []
        game_id = int(game_data["id"])

        existing_statline_ids = set(
            i[0]
            for i in session.query(GameTeamStatline.id)
            .filter(
                and_(
                    GameTeamStatline.id.in_([int(data["id"]) for data in teamstatlines.values()]),
                    GameTeamStatline.sport_code == sport_code,
                    GameTeamStatline.game_id == game_id,
                )
            )
            .with_entities(GameTeamStatline.id)
            .all()
        )

        for statline_data in teamstatlines.values():
            statline_id = int(statline_data["id"])
            if statline_id in existing_statline_ids:
                continue
            to_add.append(
                GameTeamStatline(
                    id=statline_id,
                    game_id=game_id,
                    sport_code=sport_code,
                    team_id=get_natstat_value(statline_data, "team.id", int),
                    team_code=get_natstat_value(statline_data, "team.code", str),
                    minutes=get_natstat_value(statline_data["stats"], "min", str),
                    points=get_natstat_value(statline_data["stats"], "pts", str),
                    field_goals_made=get_natstat_value(statline_data["stats"], "fgm", str),
                    field_goals_attempted=get_natstat_value(statline_data["stats"], "fga", str),
                    three_pointers_made=get_natstat_value(statline_data["stats"], "threefm", str),
                    three_pointers_attempted=get_natstat_value(
                        statline_data["stats"], "threefa", str
                    ),
                    free_throws_made=get_natstat_value(statline_data["stats"], "ftm", str),
                    free_throws_attempted=get_natstat_value(statline_data["stats"], "fta", str),
                    rebounds=get_natstat_value(statline_data["stats"], "reb", str),
                    assists=get_natstat_value(statline_data["stats"], "ast", str),
                    steals=get_natstat_value(statline_data["stats"], "stl", str),
                    blocks=get_natstat_value(statline_data["stats"], "blk", str),
                    offensive_rebounds=get_natstat_value(statline_data["stats"], "oreb", str),
                    turnovers=get_natstat_value(statline_data["stats"], "to", str),
                    fouls=get_natstat_value(statline_data["stats"], "f", str),
                    team_points=get_natstat_value(statline_data["stats"], "teampts", str),
                )
            )

        return to_add

    def save_playbyplays(
        self, session: Optional[Session], game_data, sport_code: str, playbyplays: list
    ):
        to_add = []
        game_id = int(game_data["id"])

        existing_playbyplay_ids = set(
            i[0]
            for i in session.query(GameStatPlayByPlay.id)
            .filter(
                and_(
                    GameStatPlayByPlay.id.in_([int(data["id"]) for data in playbyplays]),
                    GameStatPlayByPlay.sport_code == sport_code,
                    GameStatPlayByPlay.game_id == game_id,
                )
            )
            .with_entities(GameStatPlayByPlay.id)
            .all()
        )

        for pbp_data in playbyplays:
            pbp_id = int(pbp_data["id"])
            entry = GameStatPlayByPlay(
                id=pbp_id,
                game_id=game_id,
                sport_code=sport_code,
                event=pbp_data.get("event"),
                period=get_natstat_value(pbp_data, "period", int),
                sequence=transform_sequence(jmespath.search("sequence", pbp_data)),
                explanation=get_natstat_value(pbp_data, "explanation", str),
                player_primary_id=(
                    int(pbp_data["players"]["primary"]["id"])
                    if pbp_data.get("players")
                    and "primary" in pbp_data["players"]
                    and isinstance(pbp_data["players"]["primary"]["id"], str)
                    else None
                ),
                player_secondary_id=(
                    int(pbp_data["players"]["secondary"]["id"])
                    if pbp_data.get("players")
                    and "secondary" in pbp_data["players"]
                    and isinstance(pbp_data["players"]["secondary"]["id"], str)
                    else None
                ),
                player_pitcher_id=(
                    int(pbp_data["players"]["pitcher"]["id"])
                    if pbp_data.get("players")
                    and "pitcher" in pbp_data["players"]
                    and isinstance(pbp_data["players"]["pitcher"]["id"], str)
                    else None
                ),
                team_id=get_natstat_value(pbp_data, "team.id", int),
                team_code=natstat_get_str("team.code", pbp_data),
                scoringplay=get_natstat_value(pbp_data, "scoringplay", str),
                tags=get_natstat_value(pbp_data, "tags", str),
                thediff=get_natstat_value(pbp_data, "thediff", str),
                distance=get_natstat_value(pbp_data, "distance", int),
            )
            if pbp_id in existing_playbyplay_ids:
                session.merge(entry)
            else:
                to_add.append(entry)

        session.bulk_save_objects(to_add)

    def save_lineups(self, session: Optional[Session], game_data, sport_code: str, lineups: Dict):
        to_add = []
        game_id = int(game_data["id"])

        existing_lineup_ids = set(
            i[0]
            for i in session.query(GameLineup.id)
            .filter(
                and_(
                    GameLineup.id.in_([int(data["id"]) for data in lineups.values()]),
                    GameLineup.sport_code == sport_code,
                    GameLineup.game_id == game_id,
                )
            )
            .with_entities(GameLineup.id)
            .all()
        )

        for lineup_data in lineups.values():
            lineup_id = int(lineup_data["id"])
            if lineup_id in existing_lineup_ids:
                continue

            lineup_args = {
                "id": lineup_id,
                "game_id": game_id,
                "sport_code": sport_code,
                "team_id": get_natstat_value(lineup_data, "teamid", int),
                "team_code": get_natstat_value(lineup_data, "teamcode", str),
                "lineup_players": lineup_data.get("lineupplayers"),
                "possessions": get_natstat_value(lineup_data, "possessions", str),
                "offensive_points_per_possession": get_natstat_value(lineup_data, "oppp", str),
                "defensive_points_per_possession": get_natstat_value(lineup_data, "dppp", str),
                "efficiency_margin": get_natstat_value(lineup_data, "effmargin", str),
                "points_scored": get_natstat_value(lineup_data, "points", str),
                "points_allowed": get_natstat_value(lineup_data, "points_d", str),
                "plus_minus": get_natstat_value(lineup_data, "plusminus", str),
                "field_goals_made": get_natstat_value(lineup_data, "fgm", str),
                "field_goals_allowed": get_natstat_value(lineup_data, "fgm_d", str),
                "field_goals_margin": get_natstat_value(lineup_data, "fgm_m", str),
                "field_goals_attempted": get_natstat_value(lineup_data, "fga", str),
                "field_goals_attempted_allowed": get_natstat_value(lineup_data, "fga_d", str),
                "field_goals_attempted_margin": get_natstat_value(lineup_data, "fga_m", str),
                "three_pointers_made": get_natstat_value(lineup_data, "threefm", str),
                "three_pointers_allowed": get_natstat_value(lineup_data, "threefm_d", str),
                "three_pointers_margin": get_natstat_value(lineup_data, "threefm_m", str),
                "three_pointers_attempted": get_natstat_value(lineup_data, "threefa", str),
                "three_pointers_attempted_allowed": get_natstat_value(
                    lineup_data, "threefa_d", str
                ),
                "free_throws_made": get_natstat_value(lineup_data, "ftm", str),
                "free_throws_allowed": get_natstat_value(lineup_data, "ftm_d", str),
                "free_throws_attempted": get_natstat_value(lineup_data, "fta", str),
                "free_throws_attempted_allowed": get_natstat_value(lineup_data, "fta_d", str),
                "rebounds": get_natstat_value(lineup_data, "reb", str),
                "rebounds_allowed": get_natstat_value(lineup_data, "reb_d", str),
                "rebounds_margin": get_natstat_value(lineup_data, "reb_m", str),
                "assists": get_natstat_value(lineup_data, "ast", str),
                "assists_allowed": get_natstat_value(lineup_data, "ast_d", str),
                "assists_margin": get_natstat_value(lineup_data, "ast_m", str),
                "blocks": get_natstat_value(lineup_data, "blk", str),
                "blocks_allowed": get_natstat_value(lineup_data, "blk_d", str),
                "blocks_margin": get_natstat_value(lineup_data, "blk_m", str),
                "steals": get_natstat_value(lineup_data, "stl", str),
                "steals_allowed": get_natstat_value(lineup_data, "stl_d", str),
                "steals_margin": get_natstat_value(lineup_data, "stl_m", str),
                "turnovers": get_natstat_value(lineup_data, "tov", str),
                "turnovers_allowed": get_natstat_value(lineup_data, "tov_d", str),
                "turnovers_margin": get_natstat_value(lineup_data, "tov_m", str),
                "personal_fouls": get_natstat_value(lineup_data, "pf", str),
                "personal_fouls_drawn": get_natstat_value(lineup_data, "pf_d", str),
                "second_chance_points": get_natstat_value(lineup_data, "pts2ch", str),
                "second_chance_points_allowed": get_natstat_value(lineup_data, "pts2ch_d", str),
                "second_chance_points_margin": get_natstat_value(lineup_data, "pts2ch_m", str),
                "fast_break_points": get_natstat_value(lineup_data, "ptsbrk", str),
                "fast_break_points_allowed": get_natstat_value(lineup_data, "ptsbrk_d", str),
                "fast_break_points_margin": get_natstat_value(lineup_data, "ptsbrk_m", str),
                "points_off_turnovers": get_natstat_value(lineup_data, "ptsoffto", str),
                "points_off_turnovers_allowed": get_natstat_value(lineup_data, "ptsoffto_d", str),
                "points_off_turnovers_margin": get_natstat_value(lineup_data, "ptsoffto_m", str),
                "points_in_the_paint": get_natstat_value(lineup_data, "ptspaint", str),
                "points_in_the_paint_allowed": get_natstat_value(lineup_data, "ptspaint_d", str),
                "points_in_the_paint_margin": get_natstat_value(lineup_data, "ptspaint_m", str),
            }

            for i in range(1, 6):
                player_key = f"player_{i}_id"
                lineup_args[player_key] = None
                player_obj = lineup_data["players"].get(f"player_{i}")
                if player_obj is None:
                    continue

                player_id = get_natstat_value(player_obj, "id", int)
                if player_id is None or player_id == 0:
                    lineup_args[player_key] = player_id

            to_add.append(lineup_args)

        return to_add

    @log_with_time_info
    @provide_session(SessionProd)
    def upsert_live_games(self, session: Optional[Session], changes: list[GameChanges]):
        created_at = get_utc_now()
        to_add = []

        for game_changes in changes:
            game_id = game_changes.game_short.game_id
            sport_code = game_changes.game_short.sport_code

            game = session.query(Game).get((game_id, sport_code))
            if not game:
                log(f"Game not found: game_id={game_id}, sport_code={sport_code}")
                continue

            update_game_fields(game, game_changes)
            to_add.extend(
                prepare_playbyplay_entries(session, game_changes, game_id, sport_code, created_at)
            )

        if to_add:
            session.bulk_save_objects(to_add)
        session.commit()


def update_game_fields(game: Game, game_changes: GameChanges):
    if game_changes.gamedatetime is not None:
        game.gamedatetime = game_changes.gamedatetime

    if game_changes.status is not None:
        game.status = game_changes.status

    if game_changes.score_visitor is not None:
        game.score_visitor = game_changes.score_visitor

    if game_changes.score_home is not None:
        game.score_home = game_changes.score_home

    if game_changes.score_overtime is not None:
        game.score_overtime = game_changes.score_overtime


def prepare_playbyplay_entries(
    session, game_changes: GameChanges, game_id: int, sport_code: str, created_at: datetime
) -> list[GameStatPlayByPlay]:
    to_add = []
    if game_changes.playbyplay_changes is None or len(game_changes.playbyplay_changes) == 0:
        return to_add

    playbyplay_changes = game_changes.playbyplay_changes
    existing_pbp_data = (
        session.query(GameStatPlayByPlay.id, GameStatPlayByPlay.distance)
        .filter(
            and_(
                GameStatPlayByPlay.game_id == game_id,
                GameStatPlayByPlay.sport_code == sport_code,
                GameStatPlayByPlay.id.in_(
                    [int(play_data["id"]) for play_data in playbyplay_changes]
                ),
            )
        )
        .all()
    )
    existing_pbp = {pbp.id for pbp in existing_pbp_data}

    for play_data in playbyplay_changes:
        play_id = int(play_data["id"])
        if play_id in existing_pbp:
            continue

        pbp = GameStatPlayByPlay(
            id=play_id,
            game_id=game_id,
            sport_code=sport_code,
            event=play_data.get("event"),
            period=get_natstat_value(play_data, "period", str),
            sequence=transform_sequence(play_data.get("sequence")),
            explanation=(
                play_data.get("explanation")
                if not isinstance(play_data.get("explanation"), dict)
                else None
            ),
            team_id=get_natstat_value(play_data, "team.id", int),
            team_code=(
                play_data["team"]["code"]
                if "code" in play_data["team"] and isinstance(play_data["team"]["code"], str)
                else None
            ),
            scoringplay=play_data.get("scoringplay"),
            tags=play_data.get("tags"),
            thediff=play_data.get("thediff"),
            player_primary_id=get_natstat_value(play_data, "players.primary.id", int),
            player_secondary_id=get_natstat_value(play_data, "players.secondary.id", int),
            player_pitcher_id=get_natstat_value(play_data, "players.pitcher.id", int),
            distance=get_natstat_value(play_data, "distance", int),
            created_at=created_at,
        )
        to_add.append(pbp)
    return to_add
