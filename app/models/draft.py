from __future__ import annotations

import json
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


def seed_dummy_teams_and_players(main_db_url: str, database_name: str) -> dict[str, int]:
    draft_url = build_draft_database_url(main_db_url, database_name)
    engine = create_engine(draft_url)

    seed_rosters_path = Path(__file__).with_name("seeds_team_rosters.json")
    with seed_rosters_path.open("r", encoding="utf-8") as fh:
        seed_rosters_payload = json.load(fh)
    seeded_teams: list[dict[str, object]] = seed_rosters_payload.get("teams", [])

    inserted_teams = 0
    inserted_players = 0

    with engine.begin() as conn:
        team_count = conn.execute(text("SELECT COUNT(*) FROM tbl_teams")).scalar_one()
        player_count = conn.execute(text("SELECT COUNT(*) FROM tbl_players")).scalar_one()

        if team_count == 0:
            for team in seeded_teams:
                conn.execute(
                    text(
                        """
                        INSERT INTO tbl_teams (name, region, seed)
                        VALUES (:name, :region, :seed)
                        """
                    ),
                    {
                        "name": str(team["name"]),
                        "region": str(team["region"]),
                        "seed": int(team["seed"]),
                    },
                )
                inserted_teams += 1

        if player_count == 0 and seeded_teams:
            team_rows = conn.execute(text("SELECT id, name FROM tbl_teams")).mappings().all()
            team_id_by_name = {str(row["name"]): int(row["id"]) for row in team_rows}

            for team in seeded_teams:
                team_name = str(team["name"])
                team_id = team_id_by_name.get(team_name)
                if not team_id:
                    continue

                players = team.get("players", [])
                for idx, player in enumerate(players):
                    conn.execute(
                        text(
                            """
                            INSERT INTO tbl_players (
                                team_id,
                                first_name,
                                last_name,
                                position,
                                ppg,
                                jersey_number,
                                is_eliminated
                            )
                            VALUES (
                                :team_id,
                                :first_name,
                                :last_name,
                                :position,
                                :ppg,
                                :jersey_number,
                                false
                            )
                            """
                        ),
                        {
                            "team_id": team_id,
                            "first_name": str(player["first_name"]),
                            "last_name": str(player["last_name"]),
                            "position": str(player["position"]),
                            "ppg": float(player["ppg"]),
                            "jersey_number": idx + 1,
                        },
                    )
                    inserted_players += 1

    engine.dispose()
    return {"teams": inserted_teams, "players": inserted_players}
