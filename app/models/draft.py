from __future__ import annotations

import csv
import re
from pathlib import Path

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    text,
)
from sqlalchemy.engine import URL, make_url


metadata = MetaData()

tbl_teams = Table(
    "tbl_teams",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(120), nullable=False, unique=True),
    Column("region", String(40), nullable=True),
    Column("seed", Integer, nullable=True),
)

tbl_players = Table(
    "tbl_players",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("team_id", Integer, ForeignKey("tbl_teams.id"), nullable=False),
    Column("first_name", String(80), nullable=False),
    Column("last_name", String(80), nullable=False),
    Column("position", String(30), nullable=True),
    Column("ppg", Float, nullable=True),
    Column("jersey_number", Integer, nullable=True),
    Column("is_eliminated", Boolean, nullable=False, default=False),
)

tbl_player_points = Table(
    "tbl_player_points",
    metadata,
    Column("player_id", Integer, ForeignKey("tbl_players.id"), primary_key=True),
    Column("tournament_round", Integer, primary_key=True),
    Column("points", Integer, nullable=False, default=0),
)

tbl_fantasy_teams = Table(
    "tbl_fantasy_teams",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(120), nullable=False, unique=True),
    Column("is_active", Boolean, nullable=False, default=True),
    Column("draft_position", Integer, nullable=True),
)

tbl_draft_settings = Table(
    "tbl_draft_settings",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("draft_order_locked", Boolean, nullable=False, default=False),
)

tbl_owners = Table(
    "tbl_owners",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(120), nullable=False),
    Column("email", String(180), nullable=True),
)

tbl_owner_fantasy_teams = Table(
    "tbl_owner_fantasy_teams",
    metadata,
    Column("owner_id", Integer, ForeignKey("tbl_owners.id"), primary_key=True),
    Column(
        "fantasy_team_id",
        Integer,
        ForeignKey("tbl_fantasy_teams.id"),
        primary_key=True,
    ),
)

tbl_stg_games = Table(
    "tbl_stg_games",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("home_team_id", Integer, ForeignKey("tbl_teams.id"), nullable=False),
    Column("away_team_id", Integer, ForeignKey("tbl_teams.id"), nullable=False),
    Column("round_name", String(50), nullable=False),
    Column("winner_team_id", Integer, ForeignKey("tbl_teams.id"), nullable=True),
    Column("game_time", DateTime, nullable=True),
)

tbl_stg_box_scores = Table(
    "tbl_stg_box_scores",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("game_id", Integer, ForeignKey("tbl_stg_games.id"), nullable=False),
    Column("player_id", Integer, ForeignKey("tbl_players.id"), nullable=False),
    Column("points", Integer, nullable=False, default=0),
    Column("rebounds", Integer, nullable=False, default=0),
    Column("assists", Integer, nullable=False, default=0),
    Column("steals", Integer, nullable=False, default=0),
    Column("blocks", Integer, nullable=False, default=0),
    Column("turnovers", Integer, nullable=False, default=0),
    Column("minutes", Integer, nullable=False, default=0),
)

tbl_player_draft_event = Table(
    "tbl_player_draft_event",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("fantasy_team_id", Integer, ForeignKey("tbl_fantasy_teams.id"), nullable=False),
    Column("player_id", Integer, ForeignKey("tbl_players.id"), nullable=False),
    Column("draft_round", Integer, nullable=False),
    Column("pick_number", Integer, nullable=False),
    Column("drafted_at", DateTime, nullable=True),
)

tbl_bracket = Table(
    "tbl_bracket",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("game_id", Integer, ForeignKey("tbl_stg_games.id"), nullable=False),
    Column("round_name", String(50), nullable=False),
    Column("region", String(40), nullable=True),
    Column("winner_team_id", Integer, ForeignKey("tbl_teams.id"), nullable=True),
)


tbl_sportsref_school_index = Table(
    "tbl_sportsref_school_index",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("school_name", String(200), nullable=False),
    Column("url", String(500), nullable=True),
    Column("season_year", Integer, nullable=False),
    Column("scraped_at", DateTime, nullable=False),
)

tbl_sportsref_school_roster = Table(
    "tbl_sportsref_school_roster",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("school_name", String(200), nullable=False),
    Column("season_year", Integer, nullable=False),
    Column("jersey_number", String(10), nullable=True),
    Column("first_name", String(100), nullable=True),
    Column("last_name", String(100), nullable=True),
    Column("position", String(30), nullable=True),
    Column("class_year", String(20), nullable=True),
    Column("height", String(20), nullable=True),
    Column("weight", String(20), nullable=True),
    Column("hometown", Text, nullable=True),
    Column("nation", String(100), nullable=True),
    Column("ppg", Float, nullable=True),
    Column("rpg", Float, nullable=True),
    Column("apg", Float, nullable=True),
    Column("scraped_at", DateTime, nullable=False),
)


def _sanitize_database_name(database_name: str) -> str:
    if not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_]{1,62}", database_name):
        raise ValueError("Invalid database name format")
    return database_name.lower()


def _admin_url(base_url: str | URL) -> URL:
    url = make_url(base_url)
    return url.set(database="postgres")


def build_draft_database_url(main_db_url: str, database_name: str) -> str:
    safe_name = _sanitize_database_name(database_name)
    url = make_url(main_db_url).set(database=safe_name)
    return url.render_as_string(hide_password=False)


def create_draft_database(main_db_url: str, database_name: str) -> None:
    safe_name = _sanitize_database_name(database_name)
    engine = create_engine(_admin_url(main_db_url), isolation_level="AUTOCOMMIT")

    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
            {"db_name": safe_name},
        ).scalar()
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{safe_name}"'))

    engine.dispose()


def create_draft_schema(main_db_url: str, database_name: str) -> None:
    draft_url = build_draft_database_url(main_db_url, database_name)
    draft_engine = create_engine(draft_url)
    metadata.create_all(draft_engine)

    draft_engine.dispose()


def seed_teams_from_csv(main_db_url: str, database_name: str, year: int) -> dict:
    """Load tbl_teams from app/models/seeds/{year}_seeds.csv.

    Only inserts if tbl_teams is empty.
    Returns {"inserted": int, "team_names": list[str]}.
    Silently skips if the CSV file does not exist.
    """
    csv_path = Path(__file__).parent / "seeds" / f"{year}_seeds.csv"
    if not csv_path.exists():
        return {"inserted": 0, "team_names": []}

    draft_url = build_draft_database_url(main_db_url, database_name)
    engine = create_engine(draft_url)
    inserted = 0

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = [
            {
                "name": row["name"].strip(),
                "seed": int(row["seed"]),
                "region": row["region"].strip(),
            }
            for row in reader
            if row.get("name", "").strip()
        ]

    with engine.begin() as conn:
        team_count = conn.execute(text("SELECT COUNT(*) FROM tbl_teams")).scalar_one()
        if team_count == 0:
            for row in rows:
                conn.execute(
                    text(
                        "INSERT INTO tbl_teams (name, seed, region) "
                        "VALUES (:name, :seed, :region)"
                    ),
                    row,
                )
                inserted += 1

    engine.dispose()
    team_names = [r["name"] for r in rows] if inserted > 0 else []
    return {"inserted": inserted, "team_names": team_names}
