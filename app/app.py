from __future__ import annotations

import random
from typing import Any

from sqlalchemy import create_engine, text

from app.models.draft import build_draft_database_url


def _draft_engine(main_db_url: str, database_name: str):
    return create_engine(build_draft_database_url(main_db_url, database_name))


FANTASY_ROUND_COLUMNS: list[dict[str, Any]] = [
    {"key": "P", "label": "P", "round_value": 0},
    {"key": "1", "label": "1", "round_value": 1},
    {"key": "2", "label": "2", "round_value": 2},
    {"key": "3", "label": "3", "round_value": 3},
    {"key": "4", "label": "4", "round_value": 4},
    {"key": "5", "label": "5", "round_value": 5},
    {"key": "6", "label": "6", "round_value": 6},
]


def get_admin_view_data(main_db_url: str, database_name: str) -> dict[str, Any]:
    engine = _draft_engine(main_db_url, database_name)
    payload: dict[str, Any] = {"fantasy_teams": [], "owners": [], "draft_order_locked": False}

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO tbl_draft_settings (id, draft_order_locked)
                VALUES (1, false)
                ON CONFLICT (id) DO NOTHING
                """
            )
        )

        team_rows = conn.execute(
            text("SELECT id, name, is_active, draft_position FROM tbl_fantasy_teams ORDER BY id")
        ).mappings()
        owner_rows = conn.execute(
            text("SELECT id, name, email FROM tbl_owners ORDER BY id")
        ).mappings()
        assignment_rows = conn.execute(
            text(
                """
                SELECT oft.owner_id, oft.fantasy_team_id, ft.name AS fantasy_team_name
                FROM tbl_owner_fantasy_teams oft
                INNER JOIN tbl_fantasy_teams ft ON ft.id = oft.fantasy_team_id
                ORDER BY oft.owner_id, ft.name
                """
            )
        ).mappings()
        locked = conn.execute(
            text("SELECT draft_order_locked FROM tbl_draft_settings WHERE id = 1")
        ).scalar_one()

        payload["fantasy_teams"] = [dict(row) for row in team_rows]
        owners = [dict(row) for row in owner_rows]
        payload["draft_order_locked"] = bool(locked)

        assignments_by_owner: dict[int, list[dict[str, Any]]] = {}
        for row in assignment_rows:
            owner_id = row["owner_id"]
            assignments_by_owner.setdefault(owner_id, []).append(
                {
                    "fantasy_team_id": row["fantasy_team_id"],
                    "fantasy_team_name": row["fantasy_team_name"],
                }
            )

        for owner in owners:
            owner["fantasy_teams"] = assignments_by_owner.get(owner["id"], [])

        payload["owners"] = owners

    engine.dispose()
    return payload


def get_leaderboard_payload(main_db_url: str, database_name: str) -> dict[str, Any]:
    engine = _draft_engine(main_db_url, database_name)

    with engine.connect() as conn:
        teams = conn.execute(
            text(
                """
                SELECT id, name, draft_position
                FROM tbl_fantasy_teams
                ORDER BY draft_position NULLS LAST, id
                """
            )
        ).mappings().all()

        point_rows = conn.execute(
            text(
                """
                SELECT de.fantasy_team_id,
                       pp.tournament_round,
                       SUM(pp.points)::INT AS total_points
                FROM tbl_player_draft_event de
                INNER JOIN tbl_player_points pp ON pp.player_id = de.player_id
                WHERE pp.tournament_round BETWEEN 0 AND 6
                GROUP BY de.fantasy_team_id, pp.tournament_round
                """
            )
        ).mappings().all()

    engine.dispose()

    round_columns = [{"key": col["key"], "label": col["label"]} for col in FANTASY_ROUND_COLUMNS]
    round_key_by_value = {col["round_value"]: col["key"] for col in FANTASY_ROUND_COLUMNS}

    team_round_points: dict[int, dict[str, int]] = {}
    for row in point_rows:
        team_id = int(row["fantasy_team_id"])
        round_key = round_key_by_value.get(int(row["tournament_round"]))
        if not round_key:
            continue
        team_round_points.setdefault(team_id, {})[round_key] = int(row["total_points"] or 0)

    rows: list[dict[str, Any]] = []
    for team in teams:
        team_id = int(team["id"])
        rounds = {col["key"]: 0 for col in FANTASY_ROUND_COLUMNS}
        rounds.update(team_round_points.get(team_id, {}))
        total = sum(rounds.values())
        rows.append(
            {
                "team_id": team_id,
                "team_name": team["name"],
                "draft_position": team["draft_position"],
                "rounds": rounds,
                "total": total,
            }
        )

    rows.sort(key=lambda row: (-row["total"], row["draft_position"] or 999, row["team_name"]))

    return {
        "round_columns": round_columns,
        "rows": rows,
    }


def get_fantasy_teams_payload(main_db_url: str, database_name: str) -> dict[str, Any]:
    engine = _draft_engine(main_db_url, database_name)

    with engine.connect() as conn:
        fantasy_teams = conn.execute(
            text(
                """
                SELECT id, name, draft_position
                FROM tbl_fantasy_teams
                ORDER BY draft_position NULLS LAST, id
                """
            )
        ).mappings().all()

        drafted_players = conn.execute(
            text(
                """
                SELECT de.fantasy_team_id,
                       de.player_id,
                       de.pick_number,
                       p.first_name,
                       p.last_name,
                       p.is_eliminated,
                       ct.name AS college_team_name
                FROM tbl_player_draft_event de
                INNER JOIN tbl_players p ON p.id = de.player_id
                INNER JOIN tbl_teams ct ON ct.id = p.team_id
                ORDER BY de.fantasy_team_id, de.pick_number
                """
            )
        ).mappings().all()

        points_rows = conn.execute(
            text(
                """
                SELECT player_id, tournament_round, SUM(points)::INT AS total_points
                FROM tbl_player_points
                WHERE tournament_round BETWEEN 0 AND 6
                GROUP BY player_id, tournament_round
                """
            )
        ).mappings().all()

        owner_assignment_rows = conn.execute(
            text(
                """
                SELECT oft.fantasy_team_id, o.name AS owner_name
                FROM tbl_owner_fantasy_teams oft
                INNER JOIN tbl_owners o ON o.id = oft.owner_id
                ORDER BY oft.fantasy_team_id, o.name
                """
            )
        ).mappings().all()

    engine.dispose()

    round_columns = [{"key": col["key"], "label": col["label"]} for col in FANTASY_ROUND_COLUMNS]
    round_key_by_value = {col["round_value"]: col["key"] for col in FANTASY_ROUND_COLUMNS}

    points_by_player: dict[int, dict[str, int | None]] = {}
    for row in points_rows:
        player_id = int(row["player_id"])
        round_key = round_key_by_value.get(int(row["tournament_round"]))
        if not round_key:
            continue

        points_by_player.setdefault(player_id, {})[round_key] = int(row["total_points"] or 0)

    owners_by_team: dict[int, list[str]] = {}
    for row in owner_assignment_rows:
        team_id = int(row["fantasy_team_id"])
        owner_name = str(row["owner_name"] or "").strip()
        if not owner_name:
            continue
        owners_by_team.setdefault(team_id, []).append(owner_name)

    players_by_team: dict[int, list[dict[str, Any]]] = {}
    for row in drafted_players:
        player_id = int(row["player_id"])
        points_map: dict[str, int | None] = {col["key"]: None for col in FANTASY_ROUND_COLUMNS}
        points_map.update(points_by_player.get(player_id, {}))
        player_total = sum(value for value in points_map.values() if value is not None)

        players_by_team.setdefault(int(row["fantasy_team_id"]), []).append(
            {
                "player_id": player_id,
                "player_name": f"{row['first_name']} {row['last_name']}",
                "college_team_name": row["college_team_name"],
                "pick_number": int(row["pick_number"]),
                "is_eliminated": bool(row["is_eliminated"]),
                "points": points_map,
                "total": player_total,
            }
        )

    teams_payload: list[dict[str, Any]] = []
    for team in fantasy_teams:
        team_players = players_by_team.get(int(team["id"]), [])
        round_totals = {col["key"]: 0 for col in FANTASY_ROUND_COLUMNS}
        for player in team_players:
            for round_key, value in player["points"].items():
                if value is not None:
                    round_totals[round_key] += value

        team_total = sum(round_totals.values())
        teams_payload.append(
            {
                "id": int(team["id"]),
                "name": team["name"],
                "draft_position": team["draft_position"],
                "owner_names": owners_by_team.get(int(team["id"]), []),
                "players": team_players,
                "round_totals": round_totals,
                "team_total": team_total,
            }
        )

    return {
        "round_columns": round_columns,
        "teams": teams_payload,
    }


def update_player_round_points(
    main_db_url: str,
    database_name: str,
    player_id: int,
    round_value: int,
    points: int | None,
    actor_role: str = "unknown",
) -> None:
    engine = _draft_engine(main_db_url, database_name)

    with engine.begin() as conn:
        existing_points_row = conn.execute(
            text(
                """
                SELECT points
                FROM tbl_player_points
                WHERE player_id = :player_id
                  AND tournament_round = :tournament_round
                """
            ),
            {
                "player_id": player_id,
                "tournament_round": round_value,
            },
        ).mappings().one_or_none()
        old_points = None if not existing_points_row else existing_points_row["points"]

        player_row = conn.execute(
            text("SELECT is_eliminated FROM tbl_players WHERE id = :player_id"),
            {"player_id": player_id},
        ).mappings().one_or_none()
        if not player_row:
            raise ValueError("Player not found.")

        is_eliminated = bool(player_row["is_eliminated"])
        if is_eliminated:
            latest_scored_round = conn.execute(
                text(
                    """
                    SELECT MAX(tournament_round)
                    FROM tbl_player_points
                    WHERE player_id = :player_id
                    """
                ),
                {"player_id": player_id},
            ).scalar_one()

            if latest_scored_round is None or round_value > int(latest_scored_round):
                raise ValueError(
                    "Player is eliminated. You can only edit rounds that already have recorded points."
                )

        if points is None:
            conn.execute(
                text(
                    """
                    DELETE FROM tbl_player_points
                    WHERE player_id = :player_id
                      AND tournament_round = :tournament_round
                    """
                ),
                {
                    "player_id": player_id,
                    "tournament_round": round_value,
                },
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO tbl_player_points (player_id, tournament_round, points)
                    VALUES (:player_id, :tournament_round, :points)
                    ON CONFLICT (player_id, tournament_round)
                    DO UPDATE SET points = EXCLUDED.points
                    """
                ),
                {
                    "player_id": player_id,
                    "tournament_round": round_value,
                    "points": points,
                },
            )

        if old_points != points:
            fantasy_team_row = conn.execute(
                text(
                    """
                    SELECT fantasy_team_id
                    FROM tbl_player_draft_event
                    WHERE player_id = :player_id
                    ORDER BY pick_number
                    LIMIT 1
                    """
                ),
                {"player_id": player_id},
            ).mappings().one_or_none()

            conn.execute(
                text(
                    """
                    INSERT INTO tbl_score_change_log
                    (changed_at, actor_role, player_id, fantasy_team_id, tournament_round, old_points, new_points)
                    VALUES (NOW(), :actor_role, :player_id, :fantasy_team_id, :tournament_round, :old_points, :new_points)
                    """
                ),
                {
                    "actor_role": (actor_role or "unknown").lower(),
                    "player_id": player_id,
                    "fantasy_team_id": None if not fantasy_team_row else fantasy_team_row["fantasy_team_id"],
                    "tournament_round": round_value,
                    "old_points": old_points,
                    "new_points": points,
                },
            )

    engine.dispose()


def get_draft_events_log(main_db_url: str, database_name: str, limit: int = 500) -> list[dict[str, Any]]:
    engine = _draft_engine(main_db_url, database_name)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT de.drafted_at,
                       ft.name AS fantasy_team_name,
                       de.pick_number,
                       t.name AS team_name,
                       p.first_name,
                       p.last_name
                FROM tbl_player_draft_event de
                INNER JOIN tbl_fantasy_teams ft ON ft.id = de.fantasy_team_id
                INNER JOIN tbl_players p ON p.id = de.player_id
                INNER JOIN tbl_teams t ON t.id = p.team_id
                ORDER BY de.drafted_at DESC NULLS LAST, de.pick_number DESC
                LIMIT :limit
                """
            ),
            {"limit": max(1, int(limit))},
        ).mappings().all()

    engine.dispose()

    return [
        {
            "executed_at": row["drafted_at"],
            "fantasy_team_name": row["fantasy_team_name"],
            "draft_pick": row["pick_number"],
            "team_name": row["team_name"],
            "player_name": f"{row['first_name']} {row['last_name']}",
        }
        for row in rows
    ]


def get_score_changes_log(main_db_url: str, database_name: str, limit: int = 500) -> list[dict[str, Any]]:
    engine = _draft_engine(main_db_url, database_name)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT scl.changed_at,
                       scl.actor_role,
                       ft.name AS fantasy_team_name,
                       t.name AS team_name,
                       p.first_name,
                       p.last_name,
                       scl.tournament_round,
                       scl.old_points,
                       scl.new_points
                FROM tbl_score_change_log scl
                INNER JOIN tbl_players p ON p.id = scl.player_id
                INNER JOIN tbl_teams t ON t.id = p.team_id
                LEFT JOIN tbl_fantasy_teams ft ON ft.id = scl.fantasy_team_id
                ORDER BY scl.changed_at DESC, scl.id DESC
                LIMIT :limit
                """
            ),
            {"limit": max(1, int(limit))},
        ).mappings().all()

    engine.dispose()

    return [
        {
            "changed_at": row["changed_at"],
            "actor_role": row["actor_role"],
            "fantasy_team_name": row["fantasy_team_name"],
            "team_name": row["team_name"],
            "player_name": f"{row['first_name']} {row['last_name']}",
            "round_label": "P" if int(row["tournament_round"]) == 0 else str(int(row["tournament_round"])),
            "old_points": row["old_points"],
            "new_points": row["new_points"],
        }
        for row in rows
    ]


def set_player_elimination_status(
    main_db_url: str,
    database_name: str,
    player_id: int,
    is_eliminated: bool,
) -> bool:
    engine = _draft_engine(main_db_url, database_name)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                UPDATE tbl_players
                SET is_eliminated = :is_eliminated
                WHERE id = :player_id
                RETURNING is_eliminated
                """
            ),
            {"player_id": player_id, "is_eliminated": is_eliminated},
        ).mappings().one_or_none()

        if not row:
            raise ValueError("Player not found.")

    engine.dispose()
    return bool(row["is_eliminated"])


def set_player_injured_status(
    main_db_url: str,
    database_name: str,
    player_id: int,
    is_injured: bool,
) -> bool:
    engine = _draft_engine(main_db_url, database_name)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                UPDATE tbl_players
                SET is_injured = :is_injured
                WHERE id = :player_id
                RETURNING is_injured
                """
            ),
            {"player_id": player_id, "is_injured": is_injured},
        ).mappings().one_or_none()

        if not row:
            raise ValueError("Player not found.")

    engine.dispose()
    return bool(row["is_injured"])


def get_rosters_payload(main_db_url: str, database_name: str) -> dict[str, Any]:
    engine = _draft_engine(main_db_url, database_name)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT p.id, p.first_name, p.last_name, p.position, p.jersey_number,
                       p.ppg, p.rpg, p.apg, p.class_year, p.height, p.weight,
                       p.hometown, p.is_eliminated, p.is_injured,
                     t.id AS team_id, t.name AS team_name, t.seed, t.region
                FROM tbl_players p
                INNER JOIN tbl_teams t ON t.id = p.team_id
                ORDER BY t.region, t.seed, p.last_name, p.first_name
                """
            )
        ).mappings().all()

    engine.dispose()

    teams: dict[int, dict[str, Any]] = {}
    for row in rows:
        team_id = int(row["team_id"])
        if team_id not in teams:
            teams[team_id] = {
                "team_id": team_id,
                "team_name": row["team_name"],
                "seed": row["seed"],
                "region": row["region"],
                "players": [],
            }
        teams[team_id]["players"].append(
            {
                "id": int(row["id"]),
                "first_name": row["first_name"],
                "last_name": row["last_name"],
                "position": row["position"],
                "jersey_number": row["jersey_number"],
                "ppg": row["ppg"],
                "rpg": row["rpg"],
                "apg": row["apg"],
                "class_year": row["class_year"],
                "height": row["height"],
                "weight": row["weight"],
                "hometown": row["hometown"],
                "is_eliminated": bool(row["is_eliminated"]),
                "is_injured": bool(row["is_injured"]),
            }
        )

    for team_payload in teams.values():
        team_payload["players"].sort(
            key=lambda player: (
                player["ppg"] is not None,
                float(player["ppg"] or 0),
            ),
            reverse=True,
        )

    return {"teams": list(teams.values())}


def get_player_detail(main_db_url: str, database_name: str, player_id: int) -> dict[str, Any] | None:
    engine = _draft_engine(main_db_url, database_name)

    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT p.id, p.first_name, p.last_name, p.position, p.jersey_number,
                       p.ppg, p.rpg, p.apg, p.class_year, p.height, p.weight,
                       p.hometown, p.is_eliminated, p.is_injured,
                       t.name AS team_name, t.seed, t.region
                FROM tbl_players p
                INNER JOIN tbl_teams t ON t.id = p.team_id
                WHERE p.id = :player_id
                """
            ),
            {"player_id": player_id},
        ).mappings().one_or_none()

    engine.dispose()

    if not row:
        return None

    return {
        "id": int(row["id"]),
        "first_name": row["first_name"],
        "last_name": row["last_name"],
        "position": row["position"],
        "jersey_number": row["jersey_number"],
        "ppg": row["ppg"],
        "rpg": row["rpg"],
        "apg": row["apg"],
        "class_year": row["class_year"],
        "height": row["height"],
        "weight": row["weight"],
        "hometown": row["hometown"],
        "is_eliminated": bool(row["is_eliminated"]),
        "is_injured": bool(row["is_injured"]),
        "team_name": row["team_name"],
        "seed": row["seed"],
        "region": row["region"],
    }


def get_team_detail_payload(main_db_url: str, database_name: str, team_id: int) -> dict[str, Any] | None:
    engine = _draft_engine(main_db_url, database_name)

    with engine.connect() as conn:
        team_row = conn.execute(
            text(
                """
                SELECT id, name, seed, region
                FROM tbl_teams
                WHERE id = :team_id
                """
            ),
            {"team_id": team_id},
        ).mappings().one_or_none()

        if not team_row:
            engine.dispose()
            return None

        player_rows = conn.execute(
            text(
                """
                SELECT id, first_name, last_name, position, jersey_number,
                       ppg, rpg, apg, class_year, height, weight, hometown,
                       is_eliminated, is_injured
                FROM tbl_players
                WHERE team_id = :team_id
                ORDER BY ppg DESC NULLS LAST, last_name, first_name
                """
            ),
            {"team_id": team_id},
        ).mappings().all()

    engine.dispose()

    players = [
        {
            "id": int(row["id"]),
            "first_name": row["first_name"],
            "last_name": row["last_name"],
            "position": row["position"],
            "jersey_number": row["jersey_number"],
            "ppg": row["ppg"],
            "rpg": row["rpg"],
            "apg": row["apg"],
            "class_year": row["class_year"],
            "height": row["height"],
            "weight": row["weight"],
            "hometown": row["hometown"],
            "is_eliminated": bool(row["is_eliminated"]),
            "is_injured": bool(row["is_injured"]),
        }
        for row in player_rows
    ]

    return {
        "team_id": int(team_row["id"]),
        "team_name": team_row["name"],
        "seed": team_row["seed"],
        "region": team_row["region"],
        "players": players,
    }


def add_fantasy_team(main_db_url: str, database_name: str, name: str) -> None:
    engine = _draft_engine(main_db_url, database_name)
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO tbl_fantasy_teams (name, is_active) VALUES (:name, true)"),
            {"name": name.strip()},
        )
    engine.dispose()


def remove_fantasy_team(main_db_url: str, database_name: str, team_id: int) -> None:
    engine = _draft_engine(main_db_url, database_name)
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM tbl_owner_fantasy_teams WHERE fantasy_team_id = :team_id"),
            {"team_id": team_id},
        )
        conn.execute(
            text("DELETE FROM tbl_player_draft_event WHERE fantasy_team_id = :team_id"),
            {"team_id": team_id},
        )
        conn.execute(
            text("DELETE FROM tbl_fantasy_teams WHERE id = :team_id"),
            {"team_id": team_id},
        )
    engine.dispose()


def add_owner(main_db_url: str, database_name: str, name: str, email: str | None) -> None:
    engine = _draft_engine(main_db_url, database_name)
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO tbl_owners (name, email) VALUES (:name, :email)"),
            {"name": name.strip(), "email": (email or "").strip() or None},
        )
    engine.dispose()


def remove_owner(main_db_url: str, database_name: str, owner_id: int) -> None:
    engine = _draft_engine(main_db_url, database_name)
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM tbl_owner_fantasy_teams WHERE owner_id = :owner_id"),
            {"owner_id": owner_id},
        )
        conn.execute(text("DELETE FROM tbl_owners WHERE id = :owner_id"), {"owner_id": owner_id})
    engine.dispose()


def assign_owner_to_fantasy_team(
    main_db_url: str,
    database_name: str,
    owner_id: int,
    fantasy_team_id: int,
) -> None:
    engine = _draft_engine(main_db_url, database_name)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO tbl_owner_fantasy_teams (owner_id, fantasy_team_id)
                VALUES (:owner_id, :fantasy_team_id)
                ON CONFLICT DO NOTHING
                """
            ),
            {"owner_id": owner_id, "fantasy_team_id": fantasy_team_id},
        )
    engine.dispose()


def unassign_owner_from_fantasy_team(
    main_db_url: str,
    database_name: str,
    owner_id: int,
    fantasy_team_id: int,
) -> None:
    engine = _draft_engine(main_db_url, database_name)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM tbl_owner_fantasy_teams
                WHERE owner_id = :owner_id AND fantasy_team_id = :fantasy_team_id
                """
            ),
            {"owner_id": owner_id, "fantasy_team_id": fantasy_team_id},
        )
    engine.dispose()


def randomize_draft_order(main_db_url: str, database_name: str) -> None:
    engine = _draft_engine(main_db_url, database_name)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO tbl_draft_settings (id, draft_order_locked)
                VALUES (1, false)
                ON CONFLICT (id) DO NOTHING
                """
            )
        )
        is_locked = conn.execute(
            text("SELECT draft_order_locked FROM tbl_draft_settings WHERE id = 1")
        ).scalar_one()
        if is_locked:
            raise ValueError("Draft order is locked and cannot be randomized.")

        team_ids = conn.execute(
            text("SELECT id FROM tbl_fantasy_teams ORDER BY id")
        ).scalars().all()
        if len(team_ids) < 2:
            raise ValueError("At least two fantasy teams are required to randomize draft order.")

        random.shuffle(team_ids)
        for idx, team_id in enumerate(team_ids, start=1):
            conn.execute(
                text("UPDATE tbl_fantasy_teams SET draft_position = :draft_position WHERE id = :team_id"),
                {"draft_position": idx, "team_id": team_id},
            )
    engine.dispose()


def lock_draft_order(main_db_url: str, database_name: str) -> None:
    engine = _draft_engine(main_db_url, database_name)
    with engine.begin() as conn:
        team_rows = conn.execute(
            text("SELECT draft_position FROM tbl_fantasy_teams ORDER BY id")
        ).scalars().all()

        if not team_rows or any(position is None for position in team_rows):
            raise ValueError("Randomize draft order before locking it in.")

        if len(set(team_rows)) != len(team_rows):
            raise ValueError("Draft order contains duplicate positions.")

        conn.execute(
            text(
                """
                INSERT INTO tbl_draft_settings (id, draft_order_locked)
                VALUES (1, true)
                ON CONFLICT (id) DO UPDATE SET draft_order_locked = EXCLUDED.draft_order_locked
                """
            )
        )
    engine.dispose()


def search_available_players(main_db_url: str, database_name: str, query: str, limit: int = 20) -> list[dict[str, Any]]:
    engine = _draft_engine(main_db_url, database_name)
    like_value = f"%{query.strip()}%"
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT p.id, p.first_name, p.last_name, t.name AS team_name
                FROM tbl_players p
                INNER JOIN tbl_teams t ON t.id = p.team_id
                LEFT JOIN tbl_player_draft_event de ON de.player_id = p.id
                WHERE de.id IS NULL
                    AND p.is_injured = false
                    AND (
                        p.first_name ILIKE :q
                        OR p.last_name ILIKE :q
                        OR (p.first_name || ' ' || p.last_name) ILIKE :q
                    )
                ORDER BY p.last_name, p.first_name
                LIMIT :limit
                """
            ),
            {"q": like_value, "limit": limit},
        ).mappings()
        payload = [dict(row) for row in rows]
    engine.dispose()
    return payload


def _snake_pick_map(team_ids: list[int], rounds: int) -> list[dict[str, int]]:
    picks: list[dict[str, int]] = []
    pick_number = 1
    for round_number in range(1, rounds + 1):
        order = team_ids if round_number % 2 == 1 else list(reversed(team_ids))
        for team_id in order:
            picks.append(
                {
                    "pick_number": pick_number,
                    "draft_round": round_number,
                    "fantasy_team_id": team_id,
                }
            )
            pick_number += 1
    return picks


def get_draft_night_payload(main_db_url: str, database_name: str, rounds: int) -> dict[str, Any]:
    engine = _draft_engine(main_db_url, database_name)
    payload: dict[str, Any] = {
        "teams": [],
        "rounds": list(range(1, rounds + 1)),
        "board": {},
        "next_pick": None,
        "draft_order_locked": False,
        "elapsed_since_last_pick_seconds": 0,
    }

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO tbl_draft_settings (id, draft_order_locked)
                VALUES (1, false)
                ON CONFLICT (id) DO NOTHING
                """
            )
        )
        teams = conn.execute(
            text(
                """
                SELECT id, name, draft_position
                FROM tbl_fantasy_teams
                ORDER BY draft_position NULLS LAST, id
                """
            )
        ).mappings().all()

        draft_events = conn.execute(
            text(
                """
                SELECT de.id,
                       de.fantasy_team_id,
                       de.player_id,
                       de.draft_round,
                       de.pick_number,
                       p.first_name,
                       p.last_name,
                       p.is_injured,
                       t.id AS team_id,
                       t.name AS team_name
                FROM tbl_player_draft_event de
                INNER JOIN tbl_players p ON p.id = de.player_id
                INNER JOIN tbl_teams t ON t.id = p.team_id
                ORDER BY de.pick_number
                """
            )
        ).mappings().all()

        is_locked = conn.execute(
            text("SELECT draft_order_locked FROM tbl_draft_settings WHERE id = 1")
        ).scalar_one()
        elapsed_since_last_pick_seconds = conn.execute(
            text(
                """
                SELECT COALESCE(FLOOR(EXTRACT(EPOCH FROM (NOW() - MAX(drafted_at)))), 0)::INT
                FROM tbl_player_draft_event
                """
            )
        ).scalar_one()

    engine.dispose()

    payload["teams"] = [dict(row) for row in teams]
    payload["draft_order_locked"] = bool(is_locked)
    payload["elapsed_since_last_pick_seconds"] = int(elapsed_since_last_pick_seconds or 0)

    team_ids_in_order = [row["id"] for row in teams if row["draft_position"] is not None]
    if not team_ids_in_order or len(team_ids_in_order) != len(teams):
        return payload

    pick_map = _snake_pick_map(team_ids_in_order, rounds)
    board: dict[str, dict[str, Any]] = {}
    for slot in pick_map:
        board[f"{slot['fantasy_team_id']}:{slot['draft_round']}"] = {
            "pick_number": slot["pick_number"],
            "player_name": None,
            "player_team_name": None,
            "player_team_id": None,
            "player_is_injured": False,
            "player_id": None,
        }

    drafted_count = 0
    for event in draft_events:
        key = f"{event['fantasy_team_id']}:{event['draft_round']}"
        if key in board:
            board[key]["player_name"] = f"{event['first_name']} {event['last_name']}"
            board[key]["player_team_name"] = event["team_name"]
            board[key]["player_team_id"] = event["team_id"]
            board[key]["player_is_injured"] = bool(event["is_injured"])
            board[key]["player_id"] = event["player_id"]
            drafted_count += 1

    payload["board"] = board

    if drafted_count < len(pick_map):
        payload["next_pick"] = pick_map[drafted_count]

    return payload


def draft_player_pick(
    main_db_url: str,
    database_name: str,
    rounds: int,
    fantasy_team_id: int,
    player_id: int,
) -> None:
    engine = _draft_engine(main_db_url, database_name)

    with engine.begin() as conn:
        is_locked = conn.execute(
            text("SELECT draft_order_locked FROM tbl_draft_settings WHERE id = 1")
        ).scalar_one_or_none()
        if not is_locked:
            raise ValueError("Draft order must be locked before drafting starts.")

        teams = conn.execute(
            text(
                """
                SELECT id, draft_position
                FROM tbl_fantasy_teams
                WHERE draft_position IS NOT NULL
                ORDER BY draft_position, id
                """
            )
        ).mappings().all()

        if not teams:
            raise ValueError("No draft order set. Randomize and lock the order first.")

        team_ids_in_order = [row["id"] for row in teams]
        pick_map = _snake_pick_map(team_ids_in_order, rounds)

        drafted_count = conn.execute(text("SELECT COUNT(*) FROM tbl_player_draft_event")).scalar_one()
        if drafted_count >= len(pick_map):
            raise ValueError("Draft is complete.")

        expected_pick = pick_map[drafted_count]
        if expected_pick["fantasy_team_id"] != fantasy_team_id:
            raise ValueError("This is not the next pick in snake order.")

        player_row = conn.execute(
            text("SELECT is_injured FROM tbl_players WHERE id = :player_id"),
            {"player_id": player_id},
        ).mappings().one_or_none()
        if not player_row:
            raise ValueError("Selected player was not found.")
        if bool(player_row["is_injured"]):
            raise ValueError("Selected player is injured and cannot be drafted.")

        already_drafted = conn.execute(
            text("SELECT 1 FROM tbl_player_draft_event WHERE player_id = :player_id"),
            {"player_id": player_id},
        ).scalar_one_or_none()
        if already_drafted:
            raise ValueError("Selected player has already been drafted.")

        conn.execute(
            text(
                """
                INSERT INTO tbl_player_draft_event (fantasy_team_id, player_id, draft_round, pick_number, drafted_at)
                VALUES (:fantasy_team_id, :player_id, :draft_round, :pick_number, NOW())
                """
            ),
            {
                "fantasy_team_id": fantasy_team_id,
                "player_id": player_id,
                "draft_round": expected_pick["draft_round"],
                "pick_number": expected_pick["pick_number"],
            },
        )

    engine.dispose()


def get_team_roster_payload(
    main_db_url: str,
    database_name: str,
    min_ppg: float,
    only_available: bool = False,
) -> dict[str, Any]:
    engine = _draft_engine(main_db_url, database_name)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT t.region,
                       t.seed,
                      t.id AS team_id,
                       t.name AS team_name,
                       p.id AS player_id,
                       p.first_name,
                       p.last_name,
                       p.position,
                       COALESCE(p.ppg, 0) AS ppg,
                      p.is_injured,
                       CASE WHEN de.id IS NOT NULL THEN TRUE ELSE FALSE END AS is_drafted
                FROM tbl_teams t
                LEFT JOIN tbl_players p
                    ON p.team_id = t.id
                    AND COALESCE(p.ppg, 0) >= :min_ppg
                LEFT JOIN tbl_player_draft_event de
                    ON de.player_id = p.id
                WHERE t.region IS NOT NULL
                ORDER BY t.seed, t.name, p.last_name, p.first_name
                """
            ),
            {"min_ppg": max(0.0, min_ppg)},
        ).mappings().all()

    engine.dispose()

    preferred_region_order = ["East", "West", "South", "Midwest"]
    regions_in_data = [row["region"] for row in rows if row["region"]]
    regions = [region for region in preferred_region_order if region in regions_in_data]
    for region in sorted(set(regions_in_data)):
        if region not in regions:
            regions.append(region)

    teams_by_region_seed: dict[tuple[str, int], dict[str, Any]] = {}
    seeds: set[int] = set()

    for row in rows:
        region = row["region"]
        seed = row["seed"]
        if not region or seed is None:
            continue

        seeds.add(seed)
        key = (region, seed)
        if key not in teams_by_region_seed:
            teams_by_region_seed[key] = {
                "team_id": row["team_id"],
                "team_name": row["team_name"],
                "players": [],
            }

        if row["first_name"]:
            is_injured = bool(row["is_injured"])
            is_drafted = bool(row["is_drafted"])
            if only_available and (is_injured or is_drafted):
                continue

            teams_by_region_seed[key]["players"].append(
                {
                    "player_id": row["player_id"],
                    "name": f"{row['first_name']} {row['last_name']}",
                    "position": row["position"] or "-",
                    "ppg": float(row["ppg"] or 0),
                    "is_injured": is_injured,
                    "is_drafted": is_drafted,
                }
            )

    for team_slot in teams_by_region_seed.values():
        team_slot["players"].sort(key=lambda player: player["ppg"], reverse=True)

    roster_rows: list[dict[str, Any]] = []
    for seed in sorted(seeds):
        row_payload = {"seed": seed, "regions": {}}
        for region in regions:
            row_payload["regions"][region] = teams_by_region_seed.get((region, seed))
        roster_rows.append(row_payload)

    return {
        "regions": regions,
        "rows": roster_rows,
        "min_ppg": max(0.0, min_ppg),
        "only_available": bool(only_available),
    }
