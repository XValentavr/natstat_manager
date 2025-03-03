import platform
import time
from urllib.parse import quote_plus

from sqlalchemy import (
    MetaData,
    Boolean,
    Column,
    DateTime,
    Integer,
    PrimaryKeyConstraint,
    String,
    create_engine,
    Float,
    Date,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker, mapped_column

from app.common.config import db_config
from app.common.logging import get_logger


class LoggedSession(Session):
    def __init__(self, *args, **kwargs):
        creation_started = time.time()
        super().__init__(*args, **kwargs)
        get_logger().info(f"DB session created, took {(time.time() - creation_started) * 1000}ms")


def create_sql_server_connection_string(host, port, username, password, database="") -> str:
    if platform.system() == "Darwin":
        driver = "ODBC Driver 18 for SQL Server"
    else:
        driver = "ODBC Driver 17 for SQL Server"

    connection_string = f"DRIVER={{{driver}}};Server={host},{port};UID={username};PWD={password};TrustServerCertificate=yes"  # noqa: 501

    if database:
        connection_string += f";Database={database}"

    connection_string = quote_plus(connection_string)
    connection_string = f"mssql+pyodbc:///?odbc_connect={connection_string}"

    return connection_string


SQLALCHEMY_DATABASE_URL = create_sql_server_connection_string(
    host=db_config.prod_db_host,
    port=db_config.prod_db_port,
    username=db_config.prod_db_username,
    password=db_config.prod_db_password,
    database=db_config.prod_db_database,
)

# assert db_config.prod_db_host == "localhost"

# 5 connections
# 10 connections

prod_engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"connect_timeout": 5, "timeout": 5, "check_same_thread": False},
    pool_size=10,
    max_overflow=15,
    fast_executemany=True,
)


ProdSessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=prod_engine, class_=LoggedSession
)


BASEBALL_SCHEMA = "baseball"
AMERICAN_FOOTBALL_SCHEMA = "american_football"
HOCKEY_SCHEMA = "hockey"

baseball_metadata = MetaData(schema=BASEBALL_SCHEMA)
football_metadata = MetaData(schema=AMERICAN_FOOTBALL_SCHEMA)
hockey_metadata = MetaData(schema=HOCKEY_SCHEMA)

BaseballBase = declarative_base(metadata=baseball_metadata)
FootballBase = declarative_base(metadata=football_metadata)
HockeyBase = declarative_base(metadata=hockey_metadata)
Base = declarative_base()


class Sport(Base):
    __tablename__ = "sports"

    code = Column(String(20), primary_key=True)
    name = Column(String)
    sport = Column(String)
    seasons = Column(Integer)
    first = Column(Integer)
    statsbegin = Column(Integer)
    last = Column(Integer)
    inplay = Column(Boolean)


class Team(Base):
    __tablename__ = "teams"

    id = Column(Integer)
    code = Column(String(20))
    sport_code = Column(String(20))
    league_code = Column(String(20))

    name = Column(String)
    nickname = Column(String)
    fullname = Column(String)
    active = Column(Boolean)

    __table_args__ = (PrimaryKeyConstraint("id", "code", "sport_code", name="pk_team"),)


class League(Base):
    __tablename__ = "leagues"

    id = Column(Integer)
    sport_code = Column(String(20))
    code = Column(String(20))

    name = Column(String)
    factor = Column(String)
    active = Column(Boolean)

    __table_args__ = (PrimaryKeyConstraint("id", "code", "sport_code", name="pk_leagues"),)


class Player(Base):
    __tablename__ = "players"

    id = Column(Integer)
    sport_code = Column(String(20))

    name = Column(String)
    position = Column(String)
    jersey = Column(String)
    experience = Column(String)

    __table_args__ = (PrimaryKeyConstraint("id", "sport_code", name="pk_player"),)


class BasketballGameStandardized(Base):
    # basketball_games_standardized
    __tablename__ = "bb_games_std"

    id = mapped_column(String(1000), primary_key=True)

    branch = mapped_column(String(1000))
    season_id = mapped_column(Integer)
    season_name = mapped_column(String(1000))
    league_name = mapped_column(String(1000))
    league_id = mapped_column(Integer)

    punteam_id = mapped_column(Integer)

    game_date_origin = mapped_column(DateTime)
    game_date_gmt = mapped_column(DateTime)
    game_date_utc = mapped_column(DateTime)
    game_date_uk = mapped_column(DateTime)
    away_team_id = mapped_column(Integer)
    away_team = mapped_column(String(1000))
    away_pts = mapped_column(Integer)
    home_team_id = mapped_column(Integer)
    home_team = mapped_column(String(1000))
    home_pts = mapped_column(Integer)
    overtimes = mapped_column(Boolean)
    attendance = mapped_column(String(1000))
    arena = mapped_column(String(1000))
    is_tournament = mapped_column(Boolean)
    is_PreSeason = mapped_column(Boolean)
    is_Nv = mapped_column(Boolean)
    subleague_name = mapped_column(String(1000))
    subleague_id = mapped_column(Integer)

    box_score_href = mapped_column(String(1000))
    update_target = mapped_column(Boolean, default=False)


class Game(Base):
    __tablename__ = "games"

    id = Column(Integer)
    sport_code = Column(String(20))

    gamedatetime = Column(DateTime)
    status = Column(String)

    visitor_id = Column(Integer)
    visitor_code = Column(String)
    home_id = Column(Integer)
    home_code = Column(String)
    winner_id = Column(Integer)
    winner_code = Column(String)
    loser_id = Column(String)
    loser_code = Column(String)

    score_visitor = Column(Integer)
    score_home = Column(Integer)
    score_overtime = Column(String)

    removed_from_natstat = Column(Boolean)

    __table_args__ = (PrimaryKeyConstraint("id", "sport_code", name="pk_game"),)


class GameText(Base):
    __tablename__ = "game_texts"

    game_id = Column(Integer)
    sport_code = Column(String(20))

    story = Column(String)
    boxheader = Column(String)
    boxscore = Column(String)
    star = Column(String)

    __table_args__ = (PrimaryKeyConstraint("game_id", "sport_code", name="pk_game_texts"),)


class GamePeriodScore(Base):
    __tablename__ = "game_periodscores"

    period = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    score = Column(Integer)
    is_visitor = Column(Boolean)

    __table_args__ = (
        PrimaryKeyConstraint(
            "period", "game_id", "sport_code", "is_visitor", name="pk_game_periodscores"
        ),
    )


class GameStatPlayByPlay(Base):
    __tablename__ = "game_stat_playbyplay"

    id = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    event = Column(String)
    period = Column(String)
    sequence = Column(String)
    explanation = Column(String)

    player_primary_id = Column(Integer)
    player_secondary_id = Column(Integer)
    player_pitcher_id = Column(Integer)

    team_id = Column(Integer)
    team_code = Column(String)
    scoringplay = Column(String)
    tags = Column(String)
    thediff = Column(String)
    distance = Column(Integer)

    created_at = Column(DateTime)

    __table_args__ = (
        PrimaryKeyConstraint("id", "game_id", "sport_code", name="pk_game_stat_playbyplay"),
    )


class GamePlayer(Base):
    __tablename__ = "game_players"

    sport_code = Column(String(20))
    player_id = Column(Integer)
    game_id = Column(Integer)
    team_id = Column(Integer)
    team_code = Column(String(20))
    position = Column(String)
    starter = Column(String)

    __table_args__ = (
        PrimaryKeyConstraint("game_id", "sport_code", "player_id", name="pk_game_players"),
    )


class GamePlayerStatline(Base):
    __tablename__ = "game_playerstatlines"

    id = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    player_id = Column(Integer)
    position = Column(String)
    starter = Column(String)

    team_id = Column(Integer)
    team_code = Column(String)

    minutes = Column(String)
    points = Column(String)
    field_goals_made = Column(String)
    field_goals_attempted = Column(String)
    three_pointers_made = Column(String)
    three_pointers_attempted = Column(String)
    free_throws_made = Column(String)
    free_throws_attempted = Column(String)
    rebounds = Column(String)
    assists = Column(String)
    steals = Column(String)
    blocks = Column(String)
    offensive_rebounds = Column(String)
    turnovers = Column(String)
    personal_fouls = Column(String)
    field_goal_percentage = Column(String)
    two_point_field_goal_percentage = Column(String)
    free_throw_percentage = Column(String)
    usage_percentage = Column(String)
    efficiency = Column(String)
    performance_score = Column(String)
    performance_score_season_avg = Column(String)
    performance_score_season_avg_deviation = Column(String)
    statline = Column(String)

    __table_args__ = (
        PrimaryKeyConstraint("id", "game_id", "sport_code", name="pk_game_playerstatlines"),
    )


class Season(Base):
    __tablename__ = "seasons"

    id = mapped_column(Integer, primary_key=True)

    season_name = mapped_column(String(300))
    start_year = mapped_column(Integer)  # could take only year
    end_year = mapped_column(Integer)  # could take only year

    league_id = mapped_column(Integer)
    league_name = mapped_column(String(300))
    champion = mapped_column(String(300))

    href = mapped_column(String)
    is_active = mapped_column(Boolean)

    sport_code = mapped_column(String)
    first_game = mapped_column(Date)
    last_game = mapped_column(Date)


class GameTeamStatline(Base):
    __tablename__ = "game_teamstatlines"

    id = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    team_id = Column(Integer)
    team_code = Column(String)

    minutes = Column(String)
    points = Column(String)
    field_goals_made = Column(String)
    field_goals_attempted = Column(String)
    three_pointers_made = Column(String)
    three_pointers_attempted = Column(String)
    free_throws_made = Column(String)
    free_throws_attempted = Column(String)
    rebounds = Column(String)
    assists = Column(String)
    steals = Column(String)
    blocks = Column(String)
    offensive_rebounds = Column(String)
    turnovers = Column(String)
    fouls = Column(String)
    team_points = Column(String)

    __table_args__ = (
        PrimaryKeyConstraint("id", "game_id", "sport_code", name="pk_game_teamstatlines"),
    )


class GameLineup(Base):
    __tablename__ = "game_lineups"

    id = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    team_id = Column(Integer)
    team_code = Column(String)
    lineup_players = Column(String)
    possessions = Column(String)
    offensive_points_per_possession = Column(String)
    defensive_points_per_possession = Column(String)
    efficiency_margin = Column(String)
    points_scored = Column(String)
    points_allowed = Column(String)
    plus_minus = Column(String)
    field_goals_made = Column(String)
    field_goals_allowed = Column(String)
    field_goals_margin = Column(String)
    field_goals_attempted = Column(String)
    field_goals_attempted_allowed = Column(String)
    field_goals_attempted_margin = Column(String)
    three_pointers_made = Column(String)
    three_pointers_allowed = Column(String)
    three_pointers_margin = Column(String)
    three_pointers_attempted = Column(String)
    three_pointers_attempted_allowed = Column(String)
    free_throws_made = Column(String)
    free_throws_allowed = Column(String)
    free_throws_attempted = Column(String)
    free_throws_attempted_allowed = Column(String)
    rebounds = Column(String)
    rebounds_allowed = Column(String)
    rebounds_margin = Column(String)
    assists = Column(String)
    assists_allowed = Column(String)
    assists_margin = Column(String)
    blocks = Column(String)
    blocks_allowed = Column(String)
    blocks_margin = Column(String)
    steals = Column(String)
    steals_allowed = Column(String)
    steals_margin = Column(String)
    turnovers = Column(String)
    turnovers_allowed = Column(String)
    turnovers_margin = Column(String)
    personal_fouls = Column(String)
    personal_fouls_drawn = Column(String)
    second_chance_points = Column(String)
    second_chance_points_allowed = Column(String)
    second_chance_points_margin = Column(String)
    fast_break_points = Column(String)
    fast_break_points_allowed = Column(String)
    fast_break_points_margin = Column(String)
    points_off_turnovers = Column(String)
    points_off_turnovers_allowed = Column(String)
    points_off_turnovers_margin = Column(String)
    points_in_the_paint = Column(String)
    points_in_the_paint_allowed = Column(String)
    points_in_the_paint_margin = Column(String)

    player_1_id = Column(Integer)
    player_2_id = Column(Integer)
    player_3_id = Column(Integer)
    player_4_id = Column(Integer)
    player_5_id = Column(Integer)

    __table_args__ = (PrimaryKeyConstraint("id", "game_id", "sport_code", name="pk_game_lineups"),)


class GamesToMonitor(Base):
    __tablename__ = "games_to_monitor"
    game_id = Column(Integer)
    sport_code = Column(String(20))

    created = Column(DateTime)
    startdatetime = Column(DateTime)
    status = Column(String)
    last_checked = Column(DateTime)
    stop_monitor_at = Column(DateTime)
    is_stuck_scheduled = Column(Boolean)

    __table_args__ = (PrimaryKeyConstraint("game_id", "sport_code", name="pk_games_to_monitor"),)


class GameUpdates(Base):
    __tablename__ = "game_updates"
    id = Column(Integer, primary_key=True)

    game_id = Column(Integer)
    sport_code = Column(String(20))

    status = Column(String)
    update_time = Column(DateTime)
    score_visitor = Column(Integer)
    score_home = Column(Integer)
    score_overtime = Column(String)

    play_by_play_hash = Column(String)
    play_by_play_json = Column(String)

    is_final = Column(Boolean)


class GameUpdatesSSOT(Base):
    __tablename__ = "game_updates_ssot"
    id = Column(Integer, primary_key=True)
    game_id = Column(Integer)
    sport_code = Column(String(20))
    created = Column(DateTime)
    data_text = Column(String)


class AmericanFootballPlayerStatLine(FootballBase):
    __tablename__ = "game_playerstatlines"
    __table_args__ = {"schema": AMERICAN_FOOTBALL_SCHEMA}

    id = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    player_id = Column(Integer)
    team_id = Column(Integer)
    team_code = Column(String)
    player_number = Column(String)  # Original: playerno

    presence_rate = Column(Float)  # Original: presencerate
    adjusted_presence_rate = Column(Float)  # Original: adjpresencerate
    statline = Column(String)  # Original: adjpresencerate

    kick_field_goals_attempted = Column(Integer)  # Original: kickfga
    kick_field_goals_made = Column(Integer)  # Original: kickfgm
    pass_attempts = Column(Integer)  # Original: passatt
    pass_completions = Column(Integer)  # Original: passcomp
    pass_interceptions = Column(Integer)  # Original: passint
    pass_rating = Column(Float)  # Original: passrating
    pass_sacks = Column(Integer)  # Original: passsacks
    pass_sacks_yards = Column(Integer)  # Original: passsacksy
    pass_touchdowns = Column(Integer)  # Original: passtd
    pass_yards = Column(Integer)  # Original: passyds
    pass_yards_per_attempt = Column(Float)  # Original: passypa
    performance_score = Column(Float)  # Original: perfscore
    performance_score_season_avg = Column(Float)  # Original: perfscoreseasonavg
    performance_score_season_avg_dev = Column(Float)  # Original: perfscoreseasonavgdev
    receptions = Column(Integer)  # Original: rec
    reception_longest = Column(Integer)  # Original: reclong
    reception_touchdowns = Column(Integer)  # Original: rectd
    reception_yards = Column(Integer)  # Original: recyds
    reception_yards_per_reception = Column(Float)  # Original: recypr
    rush_attempts = Column(Integer)  # Original: rushatt
    rush_longest = Column(Integer)  # Original: rushlong
    rush_touchdowns = Column(Integer)  # Original: rushtd
    rush_yards = Column(Integer)  # Original: rushyds
    rush_yards_per_attempt = Column(Float)  # Original: rushypa

    __table_args__ = (
        PrimaryKeyConstraint("id", "game_id", "sport_code", name="pk_game_playerstatlines"),
    )


class AmericanFootballTeamStatLine(FootballBase):
    __tablename__ = "game_teamstatlines"
    __table_args__ = {"schema": AMERICAN_FOOTBALL_SCHEMA}

    id = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    team_id = Column(Integer)
    team_code = Column(String)

    fumbles = Column(Integer)
    fumbles_lost = Column(Integer)

    pass_attempts = Column(Integer)  # Original: passatt
    pass_completions = Column(Integer)  # Original: passcomp
    pass_interceptions = Column(Integer)  # Original: passint
    pass_rating = Column(Float)  # Original: passrating
    pass_sacks = Column(Integer)  # Original: passsacks
    pass_sacks_yards = Column(Integer)  # Original: passsacksy
    pass_touchdowns = Column(Integer)  # Original: passtd
    pass_yards = Column(Integer)  # Original: passyds
    pass_yards_per_attempt = Column(Float)  # Original: passypa
    receptions = Column(Integer)  # Original: rec
    reception_longest = Column(Integer)  # Original: reclong
    reception_yards = Column(Integer)  # Original: recyds
    rush_attempts = Column(Integer)  # Original: rushatt
    rush_longest = Column(Integer)  # Original: rushlong
    rush_touchdowns = Column(Integer)  # Original: rushtd
    rush_yards = Column(Integer)  # Original: rushyds
    rush_yards_per_attempt = Column(Float)  # Original: rushypa

    sacks = Column(Float)
    tackles = Column(Integer)
    tackles_for_loss = Column(Integer)  # Original: tacklesforloss
    tackles_solo = Column(Float)  # Original: tacklessolo

    __table_args__ = (
        PrimaryKeyConstraint("id", "game_id", "sport_code", name="pk_game_teamstatlines"),
    )


class AmericanFootballPlayByPlay(FootballBase):
    __tablename__ = "game_playbyplays"
    __table_args__ = {"schema": AMERICAN_FOOTBALL_SCHEMA}

    id = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    team_code = Column(String)
    team_id = Column(Integer)

    distance = Column(Integer)
    down = Column(Integer)
    drive_details = Column(String)  # Original: drivedetails
    drive_result = Column(String)  # Original: driveresult
    explanation = Column(String)
    period = Column(String)
    primary_player_id = Column(Integer)
    secondary_player_id = Column(Integer)
    scoring_play = Column(String)  # Original: scoringplay
    sequence = Column(String)
    tags = Column(String)
    thediff = Column(String)  # Original: thediff
    yard_line = Column(String)  # Original: yardline
    yards_to_go = Column(String)  # Original: yardstogo

    __table_args__ = (
        PrimaryKeyConstraint("id", "game_id", "sport_code", name="pk_game_playbyplays"),
    )


class HockeyTeamStatLine(HockeyBase):
    __tablename__ = "game_teamstatlines"
    __table_args__ = {"schema": HOCKEY_SCHEMA}

    id = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    team_id = Column(Integer)
    team_code = Column(String)

    # Goalie stats (G)
    goalie_ga = Column(Integer)  # Original: G.ga
    goalie_shots = Column(Integer)  # Original: G.gshots
    goalie_time_on_ice = Column(String)  # Original: G.gtoi (in seconds)
    goalie_losses = Column(Integer)  # Original: G.losses
    goalie_shutouts = Column(Integer)  # Original: G.shut
    goalie_saves = Column(Integer)  # Original: G.sv
    goalie_wins = Column(Integer)  # Original: G.wins

    # Skater stats (S)
    skater_assists = Column(Integer)  # Original: S.assists
    skater_goals = Column(Integer)  # Original: S.goals
    skater_game_winning_goals = Column(Integer)  # Original: S.gwg
    skater_penalty_minutes = Column(Integer)  # Original: S.pim
    skater_plus_minus = Column(Integer)  # Original: S.plusminus
    skater_power_play_goals = Column(Integer)  # Original: S.ppg
    skater_points = Column(Integer)  # Original: S.pts
    skater_shifts = Column(Integer)  # Original: S.shf
    skater_short_handed_goals = Column(Integer)  # Original: S.shg
    skater_shootout_goals = Column(Integer)  # Original: S.shootoutg
    skater_shootout_misses = Column(Integer)  # Original: S.shootoutm
    skater_shots = Column(Integer)  # Original: S.sshots
    skater_faceoffs_won = Column(Integer)  # Original: S.fow
    skater_faceoffs_lost = Column(Integer)  # Original: S.fol
    skater_hits = Column(Integer)  # Original: S.hits
    skater_block = Column(Integer)  # Original: S.blk
    skater_giveaways = Column(Integer)  # Original: S.gv
    skater_takeaways = Column(Integer)  # Original: S.tk
    skater_time_on_ice = Column(String)

    __table_args__ = (
        PrimaryKeyConstraint("id", "game_id", "sport_code", name="pk_game_teamstatlines"),
    )


class HockeyPlayerStatLine(HockeyBase):
    __tablename__ = "game_playerstatlines"
    __table_args__ = {"schema": HOCKEY_SCHEMA}

    id = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    player_id = Column(Integer)
    team_id = Column(Integer)
    team_code = Column(String)

    adjusted_presence_rate = Column(Float)  # Original: adjpresencerate
    presence_rate = Column(Float)

    assists = Column(Integer)
    goals = Column(Integer)
    goalie_shots = Column(Integer)  # Original: gshots
    goalie_time_on_ice = Column(String)  # Original: gtoi
    game_winning_goals = Column(Integer)  # Original: gwg
    losses = Column(Integer)
    overtime_losses = Column(Integer)  # Original: otl
    penalties_1 = Column(Integer)  # Original: pen
    penalties_2 = Column(Integer)  # Original: pen
    performance_score = Column(Integer)  # Original: perfscore
    performance_score_season_avg = Column(Float)  # Original: perfscoreseasonavg
    performance_score_season_avg_dev = Column(Float)  # Original: perfscoreseasonavgdev
    penalty_minutes = Column(Integer)  # Original: pim
    player_number = Column(String)  # Original: playerno
    player_type = Column(String)
    plus_minus = Column(String)
    position = Column(String)
    power_play_goals = Column(Integer)  # Original: ppg
    points = Column(Integer)  # Original: pts
    shifts = Column(Integer)  # Original: shf
    short_handed_goals = Column(Integer)  # Original: shg
    shootout_goals = Column(Integer)  # Original: shootoutg
    shootout_misses = Column(Integer)  # Original: shootoutm
    shutouts = Column(Integer)  # Original: shut
    sshots = Column(Integer)  # Original: sshots
    skater_time_on_ice = Column(String)  # Original: stoi
    wins = Column(Integer)

    __table_args__ = (
        PrimaryKeyConstraint("id", "game_id", "sport_code", name="pk_game_playerstatlines"),
    )


class HockeyPlayByPlay(HockeyBase):
    __tablename__ = "game_playbyplays"
    __table_args__ = {"schema": HOCKEY_SCHEMA}

    id = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    event = Column(String)
    explanation = Column(String)
    period = Column(String)
    primary_player_id = Column(Integer)
    secondary_player_id = Column(Integer)
    tertiary_player_id = Column(Integer)
    scoring_play = Column(String)
    sequence = Column(String)
    team_code = Column(String)
    team_id = Column(Integer)
    thediff = Column(String)

    __table_args__ = (
        PrimaryKeyConstraint("id", "game_id", "sport_code", name="pk_game_playbyplays"),
    )


class BaseballPitch(BaseballBase):
    __tablename__ = "game_pitches"
    __table_args__ = {"schema": BASEBALL_SCHEMA}

    id = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    at_bat_result = Column(String)  # Original: atbat_result
    batter_id = Column(Integer)
    batter_team_id = Column(Integer)
    batter_handed = Column(String)  # Original: batterhanded
    explanation = Column(String)
    inning_half = Column(String)  # Original: half
    inning = Column(Integer)
    pitch_chart_x = Column(Integer)  # Original: pitchchart_x
    pitch_chart_y = Column(Integer)  # Original: pitchchart_y
    pitch_chart_zone = Column(Integer)  # Original: pitchchart_zone
    pitcher_id = Column(Integer)
    pitcher_team_id = Column(Integer)
    pitcher_handed = Column(String)  # Original: pitcherhanded
    pitch_number_in_at_bat = Column(Integer)  # Original: pitchno_atbat
    pitch_number_in_at_bat_total = Column(Integer)  # Original: pitchno_atbattotal
    pitch_number_by_pitcher = Column(Integer)  # Original: pitchno_pitcher
    pitch_number_by_pitcher_total = Column(Integer)  # Original: pitchno_pitchertotal
    pitch_type_class = Column(String)
    pitch_type_code = Column(String)
    pitch_type_name = Column(String)
    sequence = Column(String)
    speed = Column(Integer)
    swung_at = Column(String)  # Original: swungat
    tag = Column(String)
    wrong_call = Column(String)  # Original: wrongcall

    __table_args__ = (PrimaryKeyConstraint("id", "game_id", "sport_code", name="pk_game_pitches"),)


class BaseballPlayByPlay(BaseballBase):
    __tablename__ = "game_playbyplays"
    __table_args__ = {"schema": BASEBALL_SCHEMA}

    id = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    batter_handed = Column(String)  # Original: batterhanded
    explanation = Column(String)
    inning_half = Column(String)  # Original: half
    inning = Column(Integer)
    pitcher_handed = Column(String)  # Original: pitcherhanded
    scoring_play = Column(String)  # Original: scoringplay
    sequence = Column(String)
    tags = Column(String)
    team_code = Column(String)
    team_id = Column(Integer)
    count = Column(String)  # Original: thecount
    thediff = Column(String)  # Original: thediff
    pitcher_id = Column(Integer)
    primary_id = Column(Integer)

    __table_args__ = (
        PrimaryKeyConstraint("id", "game_id", "sport_code", name="pk_game_playbyplays"),
    )


class BaseballPlayerStatLine(BaseballBase):
    __tablename__ = "game_playerstatlines"
    __table_args__ = {"schema": BASEBALL_SCHEMA}

    id = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    player_id = Column(Integer)
    doubles = Column(Integer)  # Original: 2b
    triples = Column(Integer)  # Original: 3b
    at_bats = Column(Integer)  # Original: ab
    adjusted_presence_rate = Column(Float)  # Original: adjpresencerate
    balls = Column(Integer)
    base_on_balls = Column(Integer)  # Original: bb
    batters_faced = Column(Integer)  # Original: bf
    caught_stealing = Column(Integer)  # Original: cs
    double_plays = Column(Integer)  # Original: dp
    earned_runs = Column(Integer)  # Original: er
    fly_outs = Column(Integer)  # Original: fo
    ground_outs = Column(Integer)  # Original: go
    hits = Column(Integer)  # Original: h
    hit_by_pitch = Column(Integer)  # Original: hbp
    home_runs = Column(Integer)  # Original: hr
    inherited_runners = Column(Integer)  # Original: inhr
    inherited_runners_scored = Column(Integer)  # Original: inhrs
    in_play = Column(Integer)  # Original: inplay
    innings_pitched = Column(Float)  # Original: ip
    losses = Column(Integer)  # Original: l
    performance_score = Column(Float)  # Original: perfscore
    performance_score_season_average = Column(Float)  # Original: perfscoreseasonavg
    performance_score_season_average_deviation = Column(Float)  # Original: perfscoreseasonavgdev
    pitches = Column(Integer)  # Original: pit
    player_number = Column(Integer)  # Original: playerno
    player_type = Column(String)
    presence_rate = Column(Float)
    runs = Column(Integer)  # Original: r
    runs_batted_in = Column(Integer)  # Original: rbi
    stolen_bases = Column(Integer)  # Original: sb
    sacrifice_flies = Column(Integer)  # Original: sf
    sacrifice_hits = Column(Integer)  # Original: sh
    strikeouts = Column(Integer)  # Original: so
    starter = Column(Boolean)
    strikes = Column(Integer)
    wins = Column(Integer)  # Original: w
    statline = Column(String)

    __table_args__ = (
        PrimaryKeyConstraint("id", "game_id", "sport_code", name="pk_game_playerstatlines"),
    )


class BaseballScoringPlay(BaseballBase):
    __tablename__ = "game_scoringplays"
    __table_args__ = {"schema": BASEBALL_SCHEMA}

    id = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    description = Column(String)
    inning_half = Column(String)  # Original: half
    inning = Column(Integer)
    player_id = Column(Integer)
    score_home = Column(Integer)
    score_visitor = Column(Integer)  # Original: scorevis
    sequence = Column(String)
    team_id = Column(Integer)
    text = Column(String)
    thediff = Column(Integer)  # Original: thediff

    __table_args__ = (
        PrimaryKeyConstraint("id", "game_id", "sport_code", name="pk_game_scoringplays"),
    )


class BaseballTeamStatLine(BaseballBase):
    __tablename__ = "game_teamstatlines"
    __table_args__ = {"schema": BASEBALL_SCHEMA}

    id = Column(Integer)
    game_id = Column(Integer)
    sport_code = Column(String(20))

    team_id = Column(Integer)
    team_code = Column(String)
    # Batting stats
    batting_at_bats = Column(Integer)  # Original: B.ab
    batting_base_on_balls = Column(Integer)  # Original: B.bb
    batting_caught_stealing = Column(Integer)  # Original: B.cs
    batting_double_plays = Column(Integer)  # Original: B.dp
    batting_hits = Column(Integer)  # Original: B.h
    batting_hit_by_pitch = Column(Integer)  # Original: B.hbp
    batting_home_runs = Column(Integer)  # Original: B.hr
    batting_runs = Column(Integer)  # Original: B.r
    batting_runs_batted_in = Column(Integer)  # Original: B.rbi
    batting_stolen_bases = Column(Integer)  # Original: B.sb
    batting_sacrifice_flies = Column(Integer)  # Original: B.sf
    batting_sacrifice_hits = Column(Integer)  # Original: B.sh
    batting_strikeouts = Column(Integer)  # Original: B.so
    batting_triples = Column(Integer)  # Original: B.threeb
    batting_doubles = Column(Integer)  # Original: B.twob
    # Pitching stats
    pitching_at_bats = Column(Integer)  # Original: P.ab
    pitching_base_on_balls = Column(Integer)  # Original: P.bb
    pitching_batters_faced = Column(Integer)  # Original: P.bf
    pitching_earned_runs = Column(Integer)  # Original: P.er
    pitching_fly_outs = Column(Integer)  # Original: P.fo
    pitching_ground_outs = Column(Integer)  # Original: P.go
    pitching_hits = Column(Integer)  # Original: P.h
    pitching_home_runs = Column(Integer)  # Original: P.hr
    pitching_inherited_runners = Column(Integer)  # Original: P.inhr
    pitching_inherited_runners_scored = Column(Integer)  # Original: P.inhrs
    pitching_innings_pitched = Column(Float)  # Original: P.ip
    pitching_losses = Column(Integer)  # Original: P.l
    pitching_pitches = Column(Integer)  # Original: P.pit
    pitching_runs = Column(Integer)  # Original: P.r
    pitching_strikeouts = Column(Integer)  # Original: P.so
    pitching_saves = Column(Integer)  # Original: P.sv
    pitching_wins = Column(Integer)  # Original: P.w

    __table_args__ = (
        PrimaryKeyConstraint("id", "game_id", "sport_code", name="pk_game_teamstatlines"),
    )


class GameV3Data(Base):
    """
    Extra game data fetch via v3 API
    Main reason to get league code for each game
    """

    __tablename__ = "games_v3"

    id = Column(Integer)
    sport_code = Column(String(20))

    gameno = Column(String)
    league = Column(String)
    venue_name = Column(String)
    venue_code = Column(String)
    season = Column(String)

    __table_args__ = (PrimaryKeyConstraint("id", "sport_code", name="pk_games_v3"),)


class BasketballBoxScores(Base):
    __tablename__ = "bb_box_scores"

    id = mapped_column(Integer, primary_key=True)

    season_id = mapped_column(Integer)
    event_id = mapped_column(String)
    game_date_et = mapped_column(DateTime)

    # Home
    home_Tm = mapped_column(String)
    home_team_id = mapped_column(String)
    home_MP = mapped_column(Integer)
    home_FG = mapped_column(Integer)
    home_FGA = mapped_column(Integer)
    home_3P = mapped_column(Integer)
    home_3PA = mapped_column(Integer)
    home_FT = mapped_column(Integer)
    home_FTA = mapped_column(Integer)
    home_ORB = mapped_column(Integer)
    home_DRB = mapped_column(Integer)
    home_TRB = mapped_column(Integer)
    home_AST = mapped_column(Integer)
    home_STL = mapped_column(Integer)
    home_BLK = mapped_column(Integer)
    home_TOV = mapped_column(Integer)
    home_PF = mapped_column(Integer)
    home_PTS = mapped_column(Integer)

    # Away
    away_Tm = mapped_column(String)
    away_team_id = mapped_column(String)
    away_MP = mapped_column(Integer)
    away_FG = mapped_column(Integer)
    away_FGA = mapped_column(Integer)
    away_3P = mapped_column(Integer)
    away_3PA = mapped_column(Integer)
    away_FT = mapped_column(Integer)
    away_FTA = mapped_column(Integer)
    away_ORB = mapped_column(Integer)
    away_DRB = mapped_column(Integer)
    away_TRB = mapped_column(Integer)
    away_AST = mapped_column(Integer)
    away_STL = mapped_column(Integer)
    away_BLK = mapped_column(Integer)
    away_TOV = mapped_column(Integer)
    away_PF = mapped_column(Integer)
    away_PTS = mapped_column(Integer)

    avg_poss = mapped_column(Float)
    home_poss = mapped_column(Float)
    away_poss = mapped_column(Float)
    home_2PA = mapped_column(Float)
    away_2PA = mapped_column(Float)
    home_2P = mapped_column(Float)
    away_2P = mapped_column(Float)


# from sqlalchemy import event
# from sqlalchemy.schema import CreateSchema


# @event.listens_for(Base.metadata, "before_create")
# def create_schemas(target, connection, **kw):
#     schemas = [AMERICAN_FOOTBALL_SCHEMA, BASEBALL_SCHEMA, HOCKEY_SCHEMA]
#     for schema in schemas:
#         if not connection.dialect.has_schema(connection, schema):
#             connection.execute(CreateSchema(schema))


# # Base.metadata.create_all(prod_engine)
# FootballBase.metadata.create_all(prod_engine)
# HockeyBase.metadata.create_all(prod_engine)
# BaseballBase.metadata.create_all(prod_engine)
# Base.metadata.create_all(prod_engine)
