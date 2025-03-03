from itertools import groupby
from operator import attrgetter
from typing import TypeAlias
from datetime import datetime

from sqlalchemy import or_, and_, not_
from sqlalchemy.orm import Session


from app.common.logging import get_logger
from app.db.session_manager import SessionProd, provide_session
from app.db.database import (
    GameV3Data,
    League,
    Season,
    Sport,
    Game,
    GameText,
    GamePeriodScore,
    GamePlayer,
    HockeyTeamStatLine,
    HockeyPlayerStatLine,
    HockeyPlayByPlay,
    AmericanFootballPlayByPlay,
    AmericanFootballPlayerStatLine,
    AmericanFootballTeamStatLine,
    BaseballPitch,
    BaseballPlayByPlay,
    BaseballPlayerStatLine,
    BaseballScoringPlay,
    BaseballTeamStatLine,
)
from app.common.utils import get_natstat_value, transform_sequence, get_stat_value

from .types import SportTypes, GameDataWithSport


def log(msg: str):
    get_logger().info(msg)


ExistingGamePeriods: TypeAlias = dict[tuple[int, str], set[tuple[int, bool]]]


class PopulateDbClient:
    @provide_session(SessionProd)
    def get_nonbaskteball_notscheduled_games(self, session: Session) -> list[Game]:
        games = (
            session.query(Game.id, Game.sport_code)
            .join(Sport, Game.sport_code == Sport.code)
            .filter(
                and_(
                    Game.status != "Scheduled",
                    not_(Sport.sport.in_(["Basketball", "World Football"]))
                    # Sport.sport.in_(["Hockey"])
                    # Sport.sport.in_(["American Football"]),
                    # Sport.sport.in_(["Baseball"]),
                )
            )
            .all()
        )
        return games

    @provide_session(SessionProd)
    def save_v3_games(
        self, session: Session, season: str, sport_code: str, games_data: list[dict]
    ) -> None:
        to_add = []
        existing_games = session.query(GameV3Data.id, GameV3Data.sport_code).all()
        # Natstat API v3 have duplicates in responses
        existing_games_set = {(game_id, sport_code) for game_id, sport_code in existing_games}

        for game_data in games_data:
            game_id = int(game_data["id"])

            if (game_id, sport_code) in existing_games_set:
                continue

            gameno = game_data["gameno"] if not isinstance(game_data["gameno"], dict) else None
            league = (
                game_data.get("league") if not isinstance(game_data.get("league"), dict) else None
            )
            venue_name = game_data["venue"] if not isinstance(game_data["venue"], dict) else None
            venue_code = (
                game_data["venue-code"] if not isinstance(game_data["venue-code"], dict) else None
            )

            game_v3 = GameV3Data(
                id=int(game_data["id"]),
                sport_code=sport_code,
                gameno=gameno,
                league=league,
                venue_name=venue_name,
                venue_code=venue_code,
                season=season,
            )
            to_add.append(game_v3)

        session.bulk_save_objects(to_add)

    @provide_session(SessionProd)
    def get_sports(self, session: Session) -> list[Sport]:
        sports = session.query(Sport).all()
        return sports

    @provide_session(SessionProd)
    def get_leagues(self, session: Session) -> list[League]:
        leagues = session.query(League).all()
        return leagues

    @provide_session(SessionProd)
    def save_seasons_for_league(
        self,
        sport_code: str,
        session: Session,
        seasons_data: list[dict],
        league: League,
        last_season: str,
        last_season_in_play: str,
    ) -> None:
        to_add: list[Season] = []

        for season_data in seasons_data:
            first_game_str = season_data["firstgame"]
            if first_game_str.endswith("-00"):
                first_game_str = first_game_str.replace("-00", "-01")

            start_date = datetime.strptime(first_game_str, "%Y-%m-%d")
            end_date = datetime.strptime(season_data["lastgame"], "%Y-%m-%d")

            start_year = str(start_date.year)
            end_year = str(end_date.year)
            season_name = f"{start_year}-{end_year[-2:]}"

            season_obj = Season(
                season_name=season_name,
                start_year=start_year,
                end_year=end_year,
                league_id=league.id,
                league_name=league.name,
                href=f"/meta/{sport_code}/status",
                is_active=(season_data["season"] == last_season and last_season_in_play == "Y"),
                sport_code=sport_code,
                first_game=start_date,
                last_game=end_date,
            )
            to_add.append(season_obj)

        session.bulk_save_objects(to_add)
        session.commit()

    @provide_session(SessionProd)
    def save_game_details(
        self, session: Session, games: list[GameDataWithSport], sport2type: dict[str, str]
    ):
        texts_to_add = []
        periods_to_add = []
        players_to_add = []

        stats_objects_to_add = []

        existing_game_texts = self._get_existings_game_texts(session, games)
        existing_periods = self._get_existings_game_periods(session, games)

        for sport_code, game_data in games:
            game_id = int(game_data["id"])

            # save game text
            if (game_id, sport_code) not in existing_game_texts:
                texts_to_add.append(
                    GameText(
                        game_id=game_id,
                        sport_code=sport_code,
                        story=get_natstat_value(game_data, "text.story", str),
                        boxheader=get_natstat_value(game_data, "text.boxheader", str),
                        boxscore=get_natstat_value(game_data, "text.boxscore", str),
                        star=get_natstat_value(game_data, "text.star", str),
                    )
                )

            # save game periods
            periods_to_add += self._to_add_game_periods(game_data, sport_code, existing_periods)

            # save players
            players_to_add += self._to_add_game_players(session, game_data, sport_code)

            if "stats" not in game_data:
                continue

            stats = game_data["stats"]

            # save game stats
            if sport2type[sport_code] == SportTypes.hockey:
                stats_objects_to_add += self._to_add_hockey_game_stats(
                    session, sport_code, game_id, stats
                )
            elif sport2type[sport_code] == SportTypes.american_football:
                stats_objects_to_add += self._to_add_american_football_game_stats(
                    session, sport_code, game_id, stats
                )
            elif sport2type[sport_code] == SportTypes.baseball:
                stats_objects_to_add += self._to_add_baseball_game_stats(
                    session, sport_code, game_id, stats
                )
            else:
                log(f"No sport type found for {sport_code} game {game_id}")

        session.bulk_save_objects(texts_to_add)
        session.bulk_save_objects(periods_to_add)
        session.bulk_save_objects(players_to_add)

        grouped_objects = groupby(
            sorted(stats_objects_to_add, key=attrgetter("__class__.__name__")),
            key=attrgetter("__class__.__name__"),
        )
        for _, group in grouped_objects:
            session.bulk_save_objects(list(group))

        session.commit()

    def _get_existings_game_texts(
        self, session: Session, games: list[GameDataWithSport]
    ) -> set[tuple[int, str]]:
        game_ids_and_sport_codes = [
            (int(game_data["id"]), sport_code) for sport_code, game_data in games
        ]
        existing_game_texts = (
            session.query(GameText.game_id, GameText.sport_code)
            .filter(
                or_(
                    *[
                        and_(GameText.game_id == game_id, GameText.sport_code == sport_code)
                        for game_id, sport_code in game_ids_and_sport_codes
                    ]
                )
            )
            .all()
        )
        existing_game_text_set = {
            (game_id, sport_code) for game_id, sport_code in existing_game_texts
        }

        return existing_game_text_set

    def _get_existings_game_periods(
        self, session: Session, games: list[GameDataWithSport]
    ) -> ExistingGamePeriods:
        game_ids_and_sport_codes = [
            (int(game_data["id"]), sport_code) for sport_code, game_data in games
        ]
        existing_periods = (
            session.query(
                GamePeriodScore.game_id,
                GamePeriodScore.sport_code,
                GamePeriodScore.period,
                GamePeriodScore.is_visitor,
            )
            .filter(
                or_(
                    *[
                        and_(
                            GamePeriodScore.game_id == game_id,
                            GamePeriodScore.sport_code == sport_code,
                        )
                        for game_id, sport_code in game_ids_and_sport_codes
                    ]
                )
            )
            .all()
        )

        existing_periods_dict = {}
        for game_id, sport_code, period, is_visitor in existing_periods:
            if (game_id, sport_code) not in existing_periods_dict:
                existing_periods_dict[(game_id, sport_code)] = set()
            existing_periods_dict[(game_id, sport_code)].add((period, is_visitor))

        return existing_periods_dict

    def _to_add_game_periods(
        self, game_data: dict, sport_code: str, existing_periods: ExistingGamePeriods
    ) -> list[GamePeriodScore]:
        game_id = int(game_data["id"])
        to_add = []

        game_existing_periods = existing_periods.get((game_id, sport_code), set())

        for is_visitor in [True, False]:
            line_data = game_data["visitor" if is_visitor else "home"].get("line", {})
            for period, score in line_data.items():
                pnum = int(period[1:])
                if (pnum, is_visitor) in game_existing_periods:
                    continue
                if isinstance(score, dict):
                    continue
                if "Northeastern" in score:
                    continue

                if score == "x":
                    score = 0
                else:
                    try:
                        score = int(score)
                    except ValueError:
                        score = None

                to_add.append(
                    GamePeriodScore(
                        game_id=game_id,
                        sport_code=sport_code,
                        period=pnum,
                        score=score,
                        is_visitor=is_visitor,
                    )
                )

        return to_add

    def _to_add_game_players(self, session: Session, game_data: dict, sport_code: str):
        to_add = []
        game_id = int(game_data["id"])
        players = game_data["players"].values()

        # khl 25: one player is list of players..
        players = [player for player in players if isinstance(player, dict)]

        existing_player_ids = set(
            i[0]
            for i in session.query(GamePlayer.player_id)
            .filter(
                and_(
                    GamePlayer.player_id.in_([int(data["id"]) for data in players]),
                    GamePlayer.sport_code == sport_code,
                    GamePlayer.game_id == game_id,
                )
            )
            .with_entities(GamePlayer.player_id)
            .all()
        )

        for player_data in players:
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

    def _to_add_hockey_game_stats(
        self, session: Session, sport_code: str, game_id: int, stats: dict
    ) -> list:
        playerstatlines = stats.get("playerstatline", {}).values()
        teamstatlines = stats.get("teamstatline", {}).values()
        playbyplays = stats.get("playbyplay", {}).values()

        objects_to_add = []

        existing_player_statline_ids = set(
            i[0]
            for i in session.query(HockeyPlayerStatLine.id)
            .filter(
                and_(
                    HockeyPlayerStatLine.id.in_([int(data["id"]) for data in playerstatlines]),
                    HockeyPlayerStatLine.sport_code == sport_code,
                    HockeyPlayerStatLine.game_id == game_id,
                )
            )
            .with_entities(HockeyPlayerStatLine.id)
            .all()
        )

        existing_team_statline_ids = set(
            i[0]
            for i in session.query(HockeyTeamStatLine.id)
            .filter(
                and_(
                    HockeyTeamStatLine.id.in_([int(data["id"]) for data in teamstatlines]),
                    HockeyTeamStatLine.sport_code == sport_code,
                    HockeyTeamStatLine.game_id == game_id,
                )
            )
            .with_entities(HockeyTeamStatLine.id)
            .all()
        )

        existing_playbyplay_ids = set(
            i[0]
            for i in session.query(HockeyPlayByPlay.id)
            .filter(
                and_(
                    HockeyPlayByPlay.id.in_([int(data["id"]) for data in playbyplays]),
                    HockeyPlayByPlay.sport_code == sport_code,
                    HockeyPlayByPlay.game_id == game_id,
                )
            )
            .with_entities(HockeyPlayByPlay.id)
            .all()
        )

        for item in playerstatlines:
            statline_id = int(item["id"])
            if statline_id in existing_player_statline_ids:
                continue

            shifts = None
            try:
                shifts = get_natstat_value(item, "shf", int)
            except ValueError:
                pass

            goals = None
            try:
                goals = get_natstat_value(item, "goals", int)
            except ValueError:
                pass

            penalty_minutes = (
                None if item.get("pim") == "PIM" else get_natstat_value(item, "pim", int)
            )
            points = None if item.get("pts") == "PTS" else get_natstat_value(item, "pts", int)
            sshots = None if item.get("sshots") == "SOG" else get_natstat_value(item, "sshots", int)

            entry = HockeyPlayerStatLine(
                id=statline_id,
                game_id=game_id,
                sport_code=sport_code,
                player_id=get_natstat_value(item, "player_id", int),
                team_id=get_natstat_value(item, "team.id", int),
                team_code=get_natstat_value(item, "team.code", str),
                adjusted_presence_rate=get_natstat_value(item, "adjpresencerate", float),
                presence_rate=get_natstat_value(item, "presencerate", float),
                assists=get_natstat_value(item, "assists", int),
                goals=goals,
                goalie_shots=get_natstat_value(item, "gshots", int),
                goalie_time_on_ice=get_natstat_value(item, "gtoi", str),
                game_winning_goals=get_natstat_value(item, "gwg", int),
                losses=get_natstat_value(item, "losses", int),
                overtime_losses=get_natstat_value(item, "otl", int),
                performance_score=get_natstat_value(item, "perfscore", int),
                performance_score_season_avg=get_natstat_value(item, "perfscoreseasonavg", float),
                performance_score_season_avg_dev=get_natstat_value(
                    item, "perfscoreseasonavgdev", float
                ),
                penalty_minutes=penalty_minutes,
                player_number=get_natstat_value(item, "playerno", str),
                player_type=get_natstat_value(item, "playertype", str),
                plus_minus=get_natstat_value(item, "plusminus", str),
                position=get_natstat_value(item, "position", str),
                power_play_goals=get_natstat_value(item, "ppg", int),
                points=points,
                shifts=shifts,
                short_handed_goals=get_natstat_value(item, "shg", int),
                shootout_goals=get_natstat_value(item, "shootoutg", int),
                shootout_misses=get_natstat_value(item, "shootoutm", int),
                shutouts=get_natstat_value(item, "shut", int),
                sshots=sshots,
                skater_time_on_ice=get_natstat_value(item, "stoi", str),
                wins=get_natstat_value(item, "wins", int),
            )
            pen = item.get("pen")
            if isinstance(pen, list):
                entry.penalties_1 = int(pen[0])
                entry.penalties_2 = int(pen[1])
            else:
                entry.penalties_1 = get_natstat_value(item, "pen", int)

            objects_to_add.append(entry)

        for item in teamstatlines:
            statline_id = int(item["id"])
            if statline_id in existing_team_statline_ids:
                continue
            objects_to_add.append(
                HockeyTeamStatLine(
                    id=statline_id,
                    game_id=game_id,
                    sport_code=sport_code,
                    team_id=get_natstat_value(item, "team.id", int),
                    team_code=get_natstat_value(item, "team.code", str),
                    goalie_ga=get_natstat_value(item, "G.ga", int),
                    goalie_shots=get_natstat_value(item, "G.gshots", int),
                    goalie_time_on_ice=get_natstat_value(item, "G.gtoi", str),
                    goalie_losses=get_natstat_value(item, "G.losses", int),
                    goalie_shutouts=get_natstat_value(item, "G.shut", int),
                    goalie_saves=get_natstat_value(item, "G.sv", int),
                    goalie_wins=get_natstat_value(item, "G.wins", int),
                    skater_assists=get_natstat_value(item, "S.assists", int),
                    skater_goals=get_natstat_value(item, "S.goals", int),
                    skater_game_winning_goals=get_natstat_value(item, "S.gwg", int),
                    skater_penalty_minutes=get_natstat_value(item, "S.pim", int),
                    skater_plus_minus=get_natstat_value(item, "S.plusminus", int),
                    skater_power_play_goals=get_natstat_value(item, "S.ppg", int),
                    skater_points=get_natstat_value(item, "S.pts", int),
                    skater_shifts=get_natstat_value(item, "S.shf", int),
                    skater_short_handed_goals=get_natstat_value(item, "S.shg", int),
                    skater_shootout_goals=get_natstat_value(item, "S.shootoutg", int),
                    skater_shootout_misses=get_natstat_value(item, "S.shootoutm", int),
                    skater_shots=get_natstat_value(item, "S.sshots", int),
                    skater_faceoffs_won=get_natstat_value(item, "S.fow", int),
                    skater_faceoffs_lost=get_natstat_value(item, "S.fol", int),
                    skater_hits=get_natstat_value(item, "S.hits", int),
                    skater_block=get_natstat_value(item, "S.blk", int),
                    skater_giveaways=get_natstat_value(item, "S.gv", int),
                    skater_takeaways=get_natstat_value(item, "S.tk", int),
                    skater_time_on_ice=get_natstat_value(item, "S.stoi", str),
                )
            )

        for item in playbyplays:
            playbyplay_id = int(item["id"])
            if playbyplay_id in existing_playbyplay_ids:
                continue

            objects_to_add.append(
                HockeyPlayByPlay(
                    id=playbyplay_id,
                    game_id=game_id,
                    sport_code=sport_code,
                    event=get_natstat_value(item, "event", str),
                    explanation=get_natstat_value(item, "explanation", str),
                    period=get_natstat_value(item, "period", str),
                    primary_player_id=get_natstat_value(item, "players.primary.id", int),
                    secondary_player_id=get_natstat_value(item, "players.secondary.id", int),
                    tertiary_player_id=get_natstat_value(item, "players.tertiary.id", int),
                    scoring_play=get_natstat_value(item, "scoringplay", str),
                    sequence=transform_sequence(get_natstat_value(item, "sequence", str)),
                    team_code=get_natstat_value(item, "team.code", str),
                    team_id=get_natstat_value(item, "team.id", int),
                    thediff=get_natstat_value(item, "thediff", str),
                )
            )

        return objects_to_add

    def _to_add_american_football_game_stats(
        self, session: Session, sport_code: str, game_id: int, stats: dict
    ) -> list:
        playerstatlines = stats.get("playerstatline", {}).values()
        teamstatlines = stats.get("teamstatline", {}).values()
        playbyplays = stats.get("playbyplay", {}).values()

        objects_to_add = []

        existing_player_statline_ids = set(
            i[0]
            for i in session.query(AmericanFootballPlayerStatLine.id)
            .filter(
                and_(
                    AmericanFootballPlayerStatLine.id.in_(
                        [int(data["id"]) for data in playerstatlines]
                    ),
                    AmericanFootballPlayerStatLine.sport_code == sport_code,
                    AmericanFootballPlayerStatLine.game_id == game_id,
                )
            )
            .with_entities(AmericanFootballPlayerStatLine.id)
            .all()
        )

        existing_team_statline_ids = set(
            i[0]
            for i in session.query(AmericanFootballTeamStatLine.id)
            .filter(
                and_(
                    AmericanFootballTeamStatLine.id.in_(
                        [int(data["id"]) for data in teamstatlines]
                    ),
                    AmericanFootballTeamStatLine.sport_code == sport_code,
                    AmericanFootballTeamStatLine.game_id == game_id,
                )
            )
            .with_entities(AmericanFootballTeamStatLine.id)
            .all()
        )

        existing_playbyplay_ids = set(
            i[0]
            for i in session.query(AmericanFootballPlayByPlay.id)
            .filter(
                and_(
                    AmericanFootballPlayByPlay.id.in_([int(data["id"]) for data in playbyplays]),
                    AmericanFootballPlayByPlay.sport_code == sport_code,
                    AmericanFootballPlayByPlay.game_id == game_id,
                )
            )
            .with_entities(AmericanFootballPlayByPlay.id)
            .all()
        )

        for item in playerstatlines:
            statline_id = int(item["id"])
            if statline_id in existing_player_statline_ids:
                continue

            pass_rating_str = get_natstat_value(item, "passrating", str)
            pass_rating = (
                None
                if "--" == pass_rating_str or pass_rating_str is None
                else float(pass_rating_str)
            )

            kickfga = None
            try:
                kickfga = get_natstat_value(item, "kickfga", int)
            except TypeError:
                pass

            objects_to_add.append(
                AmericanFootballPlayerStatLine(
                    id=statline_id,
                    game_id=game_id,
                    sport_code=sport_code,
                    player_id=get_natstat_value(item, "player_id", int),
                    team_id=get_natstat_value(item, "team.id", int),
                    team_code=get_natstat_value(item, "team.code", str),
                    player_number=get_natstat_value(item, "playerno", str),
                    presence_rate=get_natstat_value(item, "presencerate", float),
                    adjusted_presence_rate=get_natstat_value(item, "adjpresencerate", float),
                    statline=get_natstat_value(item, "statline", str),
                    kick_field_goals_attempted=kickfga,
                    kick_field_goals_made=get_natstat_value(item, "kickfgm", int),
                    pass_attempts=get_natstat_value(item, "passatt", int),
                    pass_completions=get_natstat_value(item, "passcomp", int),
                    pass_interceptions=get_natstat_value(item, "passint", int),
                    pass_rating=pass_rating,
                    pass_sacks=get_natstat_value(item, "passsacks", int),
                    pass_sacks_yards=get_natstat_value(item, "passsacksy", int),
                    pass_touchdowns=get_natstat_value(item, "passtd", int),
                    pass_yards=get_natstat_value(item, "passyds", int),
                    pass_yards_per_attempt=get_natstat_value(item, "passypa", float),
                    performance_score=get_natstat_value(item, "perfscore", float),
                    performance_score_season_avg=get_natstat_value(
                        item, "perfscoreseasonavg", float
                    ),
                    performance_score_season_avg_dev=get_natstat_value(
                        item, "perfscoreseasonavgdev", float
                    ),
                    receptions=get_natstat_value(item, "rec", int),
                    reception_longest=get_natstat_value(item, "reclong", int),
                    reception_touchdowns=get_natstat_value(item, "rectd", int),
                    reception_yards=get_natstat_value(item, "recyds", int),
                    reception_yards_per_reception=get_natstat_value(item, "recypr", float),
                    rush_attempts=get_natstat_value(item, "rushatt", int),
                    rush_longest=get_natstat_value(item, "rushlong", int),
                    rush_touchdowns=get_natstat_value(item, "rushtd", int),
                    rush_yards=get_natstat_value(item, "rushyds", int),
                    rush_yards_per_attempt=get_natstat_value(item, "rushypa", float),
                )
            )

        for item in teamstatlines:
            statline_id = int(item["id"])
            if statline_id in existing_team_statline_ids:
                continue

            objects_to_add.append(
                AmericanFootballTeamStatLine(
                    id=statline_id,
                    game_id=game_id,
                    sport_code=sport_code,
                    team_id=get_natstat_value(item, "team.id", int),
                    team_code=get_natstat_value(item, "team.code", str),
                    fumbles=get_natstat_value(item, "stats.fumbles", int),
                    fumbles_lost=get_natstat_value(item, "stats.fumbleslost", int),
                    pass_attempts=get_natstat_value(item, "stats.passatt", int),
                    pass_completions=get_natstat_value(item, "stats.passcomp", int),
                    pass_interceptions=get_natstat_value(item, "stats.passint", int),
                    pass_rating=get_natstat_value(item, "stats.passrating", float),
                    pass_sacks=get_natstat_value(item, "stats.passsacks", int),
                    pass_sacks_yards=get_natstat_value(item, "stats.passsacksy", int),
                    pass_touchdowns=get_natstat_value(item, "stats.passtd", int),
                    pass_yards=get_natstat_value(item, "stats.passyds", int),
                    pass_yards_per_attempt=get_natstat_value(item, "stats.passypa", float),
                    receptions=get_natstat_value(item, "stats.rec", int),
                    reception_longest=get_natstat_value(item, "stats.reclong", int),
                    reception_yards=get_natstat_value(item, "stats.recyds", int),
                    rush_attempts=get_natstat_value(item, "stats.rushatt", int),
                    rush_longest=get_natstat_value(item, "stats.rushlong", int),
                    rush_touchdowns=get_natstat_value(item, "stats.rushtd", int),
                    rush_yards=get_natstat_value(item, "stats.rushyds", int),
                    rush_yards_per_attempt=get_natstat_value(item, "stats.rushypa", float),
                    sacks=get_natstat_value(item, "stats.sacks", float),
                    tackles=get_natstat_value(item, "stats.tackles", int),
                    tackles_for_loss=get_natstat_value(item, "stats.tacklesforloss", int),
                    tackles_solo=get_natstat_value(item, "stats.tacklessolo", float),
                )
            )

        for item in playbyplays:
            playbyplay_id = int(item["id"])
            if playbyplay_id in existing_playbyplay_ids:
                continue

            down = None
            try:
                down = get_natstat_value(item, "down", int)
            except ValueError:
                pass

            objects_to_add.append(
                AmericanFootballPlayByPlay(
                    id=playbyplay_id,
                    game_id=game_id,
                    sport_code=sport_code,
                    team_code=get_natstat_value(item, "team.code", str),
                    team_id=get_natstat_value(item, "team.id", int),
                    distance=get_natstat_value(item, "distance", int),
                    down=down,
                    drive_details=get_natstat_value(item, "drivedetails", str),
                    drive_result=get_natstat_value(item, "driveresult", str),
                    explanation=get_natstat_value(item, "explanation", str),
                    period=get_natstat_value(item, "period", str),
                    primary_player_id=get_natstat_value(item, "players.primary.id", int),
                    secondary_player_id=get_natstat_value(item, "players.secondary.id", int),
                    scoring_play=get_natstat_value(item, "scoringplay", str),
                    sequence=transform_sequence(get_natstat_value(item, "sequence", str)),
                    tags=get_natstat_value(item, "tags", str),
                    thediff=get_natstat_value(item, "thediff", str),
                    yard_line=get_natstat_value(item, "yardline", str),
                    yards_to_go=get_natstat_value(item, "yardstogo", str),
                )
            )

        return objects_to_add

    def _to_add_baseball_game_stats(
        self, session: Session, sport_code: str, game_id: int, stats: dict
    ) -> list:
        pitches = stats.get("pitches", {}).values()
        playerstatlines = stats.get("playerstatline", {}).values()
        teamstatlines = stats.get("teamstatline", {}).values()
        playbyplays = stats.get("playbyplay", {}).values()
        scoringplays = stats.get("scoringplays", {}).values()

        objects_to_add = []

        existing_pitch_ids = set(
            i[0]
            for i in session.query(BaseballPitch.id)
            .filter(
                and_(
                    BaseballPitch.id.in_([int(data["id"]) for data in pitches]),
                    BaseballPitch.sport_code == sport_code,
                    BaseballPitch.game_id == game_id,
                )
            )
            .with_entities(BaseballPitch.id)
            .all()
        )

        existing_player_statline_ids = set(
            i[0]
            for i in session.query(BaseballPlayerStatLine.id)
            .filter(
                and_(
                    BaseballPlayerStatLine.id.in_([int(data["id"]) for data in playerstatlines]),
                    BaseballPlayerStatLine.sport_code == sport_code,
                    BaseballPlayerStatLine.game_id == game_id,
                )
            )
            .with_entities(BaseballPlayerStatLine.id)
            .all()
        )

        existing_team_statline_ids = set(
            i[0]
            for i in session.query(BaseballTeamStatLine.id)
            .filter(
                and_(
                    BaseballTeamStatLine.id.in_([int(data["id"]) for data in teamstatlines]),
                    BaseballTeamStatLine.sport_code == sport_code,
                    BaseballTeamStatLine.game_id == game_id,
                )
            )
            .with_entities(BaseballTeamStatLine.id)
            .all()
        )

        existing_playbyplay_ids = set(
            i[0]
            for i in session.query(BaseballPlayByPlay.id)
            .filter(
                and_(
                    BaseballPlayByPlay.id.in_([int(data["id"]) for data in playbyplays]),
                    BaseballPlayByPlay.sport_code == sport_code,
                    BaseballPlayByPlay.game_id == game_id,
                )
            )
            .with_entities(BaseballPlayByPlay.id)
            .all()
        )

        existing_scoringplay_ids = set(
            i[0]
            for i in session.query(BaseballScoringPlay.id)
            .filter(
                and_(
                    BaseballScoringPlay.id.in_([int(data["id"]) for data in scoringplays]),
                    BaseballScoringPlay.sport_code == sport_code,
                    BaseballScoringPlay.game_id == game_id,
                )
            )
            .with_entities(BaseballScoringPlay.id)
            .all()
        )

        for item in pitches:
            pitch_id = int(item["id"])
            if pitch_id in existing_pitch_ids:
                continue

            objects_to_add.append(
                BaseballPitch(
                    id=pitch_id,
                    game_id=game_id,
                    sport_code=sport_code,
                    at_bat_result=get_natstat_value(item, "atbat_result", str),
                    batter_id=get_natstat_value(item, "batter_id", int),
                    batter_team_id=get_natstat_value(item, "batter_teamid", int),
                    batter_handed=get_natstat_value(item, "batterhanded", str),
                    explanation=get_natstat_value(item, "explanation", str),
                    inning_half=get_natstat_value(item, "half", str),
                    inning=get_natstat_value(item, "inning", int),
                    pitch_chart_x=get_natstat_value(item, "pitchchart_x", int),
                    pitch_chart_y=get_natstat_value(item, "pitchchart_y", int),
                    pitch_chart_zone=get_natstat_value(item, "pitchchart_zone", int),
                    pitcher_id=get_natstat_value(item, "pitcher_id", int),
                    pitcher_team_id=get_natstat_value(item, "pitcher_teamid", int),
                    pitcher_handed=get_natstat_value(item, "pitcherhanded", str),
                    pitch_number_in_at_bat=get_natstat_value(item, "pitchno_atbat", int),
                    pitch_number_in_at_bat_total=get_natstat_value(item, "pitchno_atbattotal", int),
                    pitch_number_by_pitcher=get_natstat_value(item, "pitchno_pitcher", int),
                    pitch_number_by_pitcher_total=get_natstat_value(
                        item, "pitchno_pitchertotal", int
                    ),
                    pitch_type_class=get_natstat_value(item, "pitchtype_class", str),
                    pitch_type_code=get_natstat_value(item, "pitchtype_code", str),
                    pitch_type_name=get_natstat_value(item, "pitchtype_name", str),
                    sequence=transform_sequence(get_natstat_value(item, "sequence", str)),
                    speed=get_natstat_value(item, "speed", int),
                    swung_at=get_natstat_value(item, "swungat", str),
                    tag=get_natstat_value(item, "tag", str),
                    wrong_call=get_natstat_value(item, "wrongcall", str),
                )
            )

        for item in playerstatlines:
            statline_id = int(item["id"])
            if statline_id in existing_player_statline_ids:
                continue

            pitches = None
            try:
                pitches = get_natstat_value(item, "pit", int)
            except ValueError:
                pass

            base_on_balls = None
            try:
                base_on_balls = get_natstat_value(item, "bb", int)
            except ValueError:
                pass

            batters_faced = get_stat_value(item, "bf", int)
            earned_runs = get_stat_value(item, "er", int)
            hits = get_stat_value(item, "h", int)
            home_runs = get_stat_value(item, "hr", int)
            runs = get_stat_value(item, "r", int)
            strikeouts = get_stat_value(item, "so", int)
            player_number = get_stat_value(item, "playerno", int)

            objects_to_add.append(
                BaseballPlayerStatLine(
                    id=statline_id,
                    game_id=game_id,
                    sport_code=sport_code,
                    player_id=get_natstat_value(item, "player_id", int),
                    doubles=get_natstat_value(item, '"2b"', int),
                    triples=get_natstat_value(item, '"3b"', int),
                    at_bats=get_natstat_value(item, "ab", int),
                    adjusted_presence_rate=get_natstat_value(item, "adjpresencerate", float),
                    balls=get_natstat_value(item, "balls", int),
                    base_on_balls=base_on_balls,
                    batters_faced=batters_faced,
                    caught_stealing=get_natstat_value(item, "cs", int),
                    double_plays=get_natstat_value(item, "dp", int),
                    earned_runs=earned_runs,
                    fly_outs=get_natstat_value(item, "fo", int),
                    ground_outs=get_natstat_value(item, "go", int),
                    hits=hits,
                    hit_by_pitch=get_natstat_value(item, "hbp", int),
                    home_runs=home_runs,
                    inherited_runners=get_natstat_value(item, "inhr", int),
                    inherited_runners_scored=get_natstat_value(item, "inhrs", int),
                    in_play=get_natstat_value(item, "inplay", int),
                    innings_pitched=get_natstat_value(item, "ip", float),
                    losses=get_natstat_value(item, "l", int),
                    performance_score=get_natstat_value(item, "perfscore", float),
                    performance_score_season_average=(
                        get_natstat_value(item, "perfscoreseasonavg", float)
                    ),
                    performance_score_season_average_deviation=(
                        get_natstat_value(item, "perfscoreseasonavgdev", float)
                    ),
                    pitches=pitches,
                    player_number=player_number,
                    player_type=get_natstat_value(item, "playertype", str),
                    presence_rate=get_natstat_value(item, "presencerate", float),
                    runs=runs,
                    runs_batted_in=get_natstat_value(item, "rbi", int),
                    stolen_bases=get_natstat_value(item, "sb", int),
                    sacrifice_flies=get_natstat_value(item, "sf", int),
                    sacrifice_hits=get_natstat_value(item, "sh", int),
                    strikeouts=strikeouts,
                    starter=get_natstat_value(item, "starter", bool),
                    strikes=get_natstat_value(item, "strikes", int),
                    wins=get_natstat_value(item, "w", int),
                    statline=get_natstat_value(item, "statline", str),
                )
            )

        for item in teamstatlines:
            statline_id = int(item["id"])
            if statline_id in existing_team_statline_ids:
                continue
            objects_to_add.append(
                BaseballTeamStatLine(
                    id=statline_id,
                    game_id=game_id,
                    sport_code=sport_code,
                    team_id=get_natstat_value(item, "team.id", int),
                    team_code=get_natstat_value(item, "team.code", str),
                    batting_at_bats=get_natstat_value(item, "B.ab", int),
                    batting_base_on_balls=get_natstat_value(item, "B.bb", int),
                    batting_caught_stealing=get_natstat_value(item, "B.cs", int),
                    batting_double_plays=get_natstat_value(item, "B.dp", int),
                    batting_hits=get_natstat_value(item, "B.h", int),
                    batting_hit_by_pitch=get_natstat_value(item, "B.hbp", int),
                    batting_home_runs=get_natstat_value(item, "B.hr", int),
                    batting_runs=get_natstat_value(item, "B.r", int),
                    batting_runs_batted_in=get_natstat_value(item, "B.rbi", int),
                    batting_stolen_bases=get_natstat_value(item, "B.sb", int),
                    batting_sacrifice_flies=get_natstat_value(item, "B.sf", int),
                    batting_sacrifice_hits=get_natstat_value(item, "B.sh", int),
                    batting_strikeouts=get_natstat_value(item, "B.so", int),
                    batting_triples=get_natstat_value(item, "B.threeb", int),
                    batting_doubles=get_natstat_value(item, "B.twob", int),
                    pitching_at_bats=get_natstat_value(item, "P.ab", int),
                    pitching_base_on_balls=get_natstat_value(item, "P.bb", int),
                    pitching_batters_faced=get_natstat_value(item, "P.bf", int),
                    pitching_earned_runs=get_natstat_value(item, "P.er", int),
                    pitching_fly_outs=get_natstat_value(item, "P.fo", int),
                    pitching_ground_outs=get_natstat_value(item, "P.go", int),
                    pitching_hits=get_natstat_value(item, "P.h", int),
                    pitching_home_runs=get_natstat_value(item, "P.hr", int),
                    pitching_inherited_runners=get_natstat_value(item, "P.inhr", int),
                    pitching_inherited_runners_scored=get_natstat_value(item, "P.inhrs", int),
                    pitching_innings_pitched=get_natstat_value(item, "P.ip", float),
                    pitching_losses=get_natstat_value(item, "P.l", int),
                    pitching_pitches=get_natstat_value(item, "P.pit", int),
                    pitching_runs=get_natstat_value(item, "P.r", int),
                    pitching_strikeouts=get_natstat_value(item, "P.so", int),
                    pitching_saves=get_natstat_value(item, "P.sv", int),
                    pitching_wins=get_natstat_value(item, "P.w", int),
                )
            )

        for item in playbyplays:
            pbp_id = int(item["id"])
            if pbp_id in existing_playbyplay_ids:
                continue
            objects_to_add.append(
                BaseballPlayByPlay(
                    id=pbp_id,
                    game_id=game_id,
                    sport_code=sport_code,
                    batter_handed=get_natstat_value(item, "batterhanded", str),
                    explanation=get_natstat_value(item, "explanation", str),
                    inning_half=get_natstat_value(item, "half", str),
                    inning=get_natstat_value(item, "inning", int),
                    pitcher_handed=get_natstat_value(item, "pitcherhanded", str),
                    scoring_play=get_natstat_value(item, "scoringplay", str),
                    sequence=transform_sequence(get_natstat_value(item, "sequence", str)),
                    tags=get_natstat_value(item, "tags", str),
                    team_code=get_natstat_value(item, "team.code", str),
                    team_id=get_natstat_value(item, "team.id", int),
                    count=get_natstat_value(item, "thecount", str),
                    thediff=get_natstat_value(item, "thediff", str),
                    pitcher_id=get_natstat_value(item, "players.pitcher.id", int),
                    primary_id=get_natstat_value(item, "players.primary.id", int),
                )
            )

        for item in scoringplays:
            scoringplay_id = int(item["id"])
            if scoringplay_id in existing_scoringplay_ids:
                continue
            objects_to_add.append(
                BaseballScoringPlay(
                    id=scoringplay_id,
                    game_id=game_id,
                    sport_code=sport_code,
                    description=get_natstat_value(item, "description", str),
                    inning_half=get_natstat_value(item, "half", str),
                    inning=get_natstat_value(item, "inning", int),
                    player_id=get_natstat_value(item, "player_id", int),
                    score_home=get_natstat_value(item, "scorehome", int),
                    score_visitor=get_natstat_value(item, "scorevis", int),
                    sequence=transform_sequence(get_natstat_value(item, "sequence", str)),
                    team_id=get_natstat_value(item, "team_id", int),
                    text=get_natstat_value(item, "text", str),
                    thediff=get_natstat_value(item, "thediff", str),
                )
            )

        return objects_to_add
