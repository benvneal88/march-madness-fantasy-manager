from __future__ import annotations

from functools import wraps

from flask import (
    Blueprint,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from slugify import slugify

from app.app import (
    add_fantasy_team,
    add_owner,
    assign_owner_to_fantasy_team,
    draft_player_pick,
    get_admin_view_data,
    get_draft_night_payload,
    get_draft_events_log,
    get_fantasy_teams_payload,
    get_leaderboard_payload,
    get_player_detail,
    get_rosters_payload,
    get_score_changes_log,
    get_team_detail_payload,
    get_team_roster_payload,
    lock_draft_order,
    randomize_draft_order,
    remove_fantasy_team,
    remove_owner,
    search_available_players,
    set_player_elimination_status,
    set_player_injured_status,
    update_player_round_points,
    unassign_owner_from_fantasy_team,
)
from app.extensions import db
from app.models import Draft, UserLoginEvent
from app.models.draft import (
    build_draft_database_url,
    create_draft_database,
    create_draft_schema,
    reload_teams_from_csv,
    seed_teams_from_csv,
)
from app.roster_jobs import get_roster_fetch_job_status, start_roster_fetch_job


main_bp = Blueprint("main", __name__)


def _request_ip_address() -> str | None:
    # Prefer the left-most forwarded IP when behind a reverse proxy.
    forwarded_for = request.headers.get("X-Forwarded-For", "").strip()
    if forwarded_for:
        client_ip = forwarded_for.split(",", 1)[0].strip()
        if client_ip:
            return client_ip[:64]

    real_ip = request.headers.get("X-Real-IP", "").strip()
    if real_ip:
        return real_ip[:64]

    remote_addr = (request.remote_addr or "").strip()
    return remote_addr[:64] or None


def _role_password_map() -> dict[str, str]:
    return {
        "viewer": current_app.config["VIEWER_PASSWORD"],
        "editor": current_app.config["EDITOR_PASSWORD"],
        "admin": current_app.config["ADMIN_PASSWORD"],
    }


def login_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if "role" not in session:
            return redirect(url_for("main.login"))
        return view(*args, **kwargs)

    return wrapped_view


def role_required(required_role: str):
    def decorator(view):
        @wraps(view)
        def wrapped_view(*args, **kwargs):
            if session.get("role") != required_role:
                flash("You do not have permission to access this page.", "danger")
                return redirect(url_for("main.index"))
            return view(*args, **kwargs)

        return wrapped_view

    return decorator


def draft_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if not _selected_draft():
            flash("Please select a draft first.", "warning")
            return redirect(url_for("main.select_draft", next=request.path))
        return view(*args, **kwargs)

    return wrapped_view


def _selected_draft() -> Draft | None:
    draft_id = session.get("selected_draft_id")
    if not draft_id:
        return None
    return db.session.get(Draft, draft_id)


def _draft_from_form_or_session() -> Draft | None:
    draft_id = request.form.get("draft_id", type=int)
    if draft_id:
        return db.session.get(Draft, draft_id)
    return _selected_draft()


def _next_path(default_endpoint: str = "main.index") -> str:
    target = request.form.get("next") or request.args.get("next")
    if target and target.startswith("/"):
        return target
    return url_for(default_endpoint)


def _theme_value() -> str:
    theme = session.get("theme", "light")
    return theme if theme in {"light", "dark"} else "light"


@main_bp.app_context_processor
def inject_nav_state() -> dict[str, object]:
    role = session.get("role")
    nav_drafts = []
    selected_draft = None

    if role:
        nav_drafts = Draft.query.order_by(Draft.year.desc(), Draft.id.desc()).all()
        selected_draft = _selected_draft()

    return {
        "nav_drafts": nav_drafts,
        "selected_draft": selected_draft,
        "current_role": role,
        "current_theme": _theme_value(),
        "app_version": current_app.config.get("VERSION", "1.0.0"),
    }


@main_bp.route("/")
def index():
    if "role" not in session:
        return redirect(url_for("main.login"))
    if "selected_draft_id" not in session:
        return redirect(url_for("main.select_draft"))
    return redirect(url_for("main.admin" if session.get("role") == "admin" else "main.leaderboard"))


@main_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        role = request.form.get("role", "").strip().lower()
        password = request.form.get("password", "")
        role_passwords = _role_password_map()

        if role in role_passwords and password == role_passwords[role]:
            session.clear()
            session["role"] = role

            login_event = UserLoginEvent(
                role=role,
                ip_address=_request_ip_address(),
            )
            try:
                db.session.add(login_event)
                db.session.commit()
            except Exception:
                db.session.rollback()

            flash("Logged in successfully.", "success")
            return redirect(url_for("main.select_draft"))

        flash("Invalid role or password.", "danger")

    return render_template("login.html")


@main_bp.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "info")
    return redirect(url_for("main.login"))


@main_bp.route("/select-draft", methods=["GET", "POST"])
@login_required
def select_draft():
    drafts = Draft.query.order_by(Draft.year.desc(), Draft.id.desc()).all()

    if request.method == "POST":
        draft_id = request.form.get("draft_id", type=int)
        draft = db.session.get(Draft, draft_id)
        if not draft:
            flash("Draft selection is invalid.", "danger")
        else:
            create_draft_schema(current_app.config["SQLALCHEMY_DATABASE_URI"], draft.database_name)
            session["selected_draft_id"] = draft.id
            flash(f"Selected draft: {draft.name}", "success")
            return redirect(_next_path())

    return render_template(
        "select_draft.html",
        drafts=drafts,
        selected_draft_id=session.get("selected_draft_id"),
        next_path=_next_path(),
    )


@main_bp.post("/set-draft")
@login_required
def set_draft():
    draft_id = request.form.get("draft_id", type=int)
    draft = db.session.get(Draft, draft_id)
    if not draft:
        flash("Draft selection is invalid.", "danger")
        return redirect(_next_path())

    create_draft_schema(current_app.config["SQLALCHEMY_DATABASE_URI"], draft.database_name)
    session["selected_draft_id"] = draft.id
    flash(f"Selected draft: {draft.name}", "success")
    return redirect(_next_path())


@main_bp.post("/set-theme")
@login_required
def set_theme():
    action = request.form.get("theme", "toggle")
    current_theme = _theme_value()

    if action == "toggle":
        new_theme = "dark" if current_theme == "light" else "light"
    else:
        new_theme = action if action in {"light", "dark"} else current_theme

    session["theme"] = new_theme
    flash(f"Theme set to {new_theme} mode.", "info")
    return redirect(_next_path())


@main_bp.route("/leaderboard")
@login_required
@draft_required
def leaderboard():
    selected_draft = _selected_draft()
    if not selected_draft:
        flash("Select a draft first.", "danger")
        return redirect(url_for("main.select_draft"))

    create_draft_schema(current_app.config["SQLALCHEMY_DATABASE_URI"], selected_draft.database_name)
    payload = get_leaderboard_payload(
        current_app.config["SQLALCHEMY_DATABASE_URI"],
        selected_draft.database_name,
    )
    return render_template(
        "leaderboard.html",
        page_title="Leaderboard",
        selected_draft=selected_draft,
        leaderboard_payload=payload,
    )


@main_bp.route("/fantasy-teams")
@login_required
@draft_required
def fantasy_teams():
    selected_draft = _selected_draft()
    if not selected_draft:
        flash("Select a draft first.", "danger")
        return redirect(url_for("main.select_draft"))

    create_draft_schema(current_app.config["SQLALCHEMY_DATABASE_URI"], selected_draft.database_name)
    payload = get_fantasy_teams_payload(
        current_app.config["SQLALCHEMY_DATABASE_URI"],
        selected_draft.database_name,
    )
    return render_template(
        "fantasy_teams.html",
        page_title="Fantasy Teams",
        selected_draft=selected_draft,
        fantasy_payload=payload,
    )


@main_bp.post("/fantasy-teams/update-points")
@login_required
@draft_required
def fantasy_teams_update_points():
    if session.get("role") not in {"admin", "editor"}:
        return jsonify({"ok": False, "error": "You do not have permission to edit points."}), 403

    selected_draft = _selected_draft()
    if not selected_draft:
        return jsonify({"ok": False, "error": "Select a draft first."}), 400

    data = request.get_json(silent=True) or {}
    player_id = data.get("player_id")
    round_value = data.get("round_value")
    points_raw = data.get("points")

    try:
        player_id = int(player_id)
        round_value = int(round_value)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Invalid payload."}), 400

    if points_raw in {None, ""}:
        points: int | None = None
    else:
        try:
            points = int(points_raw)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "Points must be a whole number or blank."}), 400

    if round_value not in {0, 1, 2, 3, 4, 5, 6}:
        return jsonify({"ok": False, "error": "Round must be one of P,1,2,3,4,5,6."}), 400
    if points is not None and points < 0:
        return jsonify({"ok": False, "error": "Points must be zero or greater."}), 400

    try:
        update_player_round_points(
            current_app.config["SQLALCHEMY_DATABASE_URI"],
            selected_draft.database_name,
            player_id,
            round_value,
            points,
            actor_role=session.get("role", "unknown"),
        )
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    return jsonify({"ok": True})


@main_bp.post("/fantasy-teams/toggle-eliminated")
@login_required
@draft_required
def fantasy_teams_toggle_eliminated():
    if session.get("role") not in {"admin", "editor"}:
        flash("You do not have permission to update elimination status.", "danger")
        return redirect(url_for("main.fantasy_teams"))

    selected_draft = _selected_draft()
    if not selected_draft:
        flash("Select a draft first.", "danger")
        return redirect(url_for("main.select_draft"))

    player_id = request.form.get("player_id", type=int)
    target_state = request.form.get("is_eliminated", "").strip().lower() == "true"

    if not player_id:
        flash("Invalid player selection.", "danger")
        return redirect(url_for("main.fantasy_teams"))

    try:
        new_state = set_player_elimination_status(
            current_app.config["SQLALCHEMY_DATABASE_URI"],
            selected_draft.database_name,
            player_id,
            target_state,
        )
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("main.fantasy_teams"))

    if new_state:
        flash("Player marked as eliminated.", "warning")
    else:
        flash("Player marked as active.", "success")

    return redirect(url_for("main.fantasy_teams"))


@main_bp.route("/team-rosters")
@login_required
@draft_required
def team_rosters():
    selected_draft = _selected_draft()
    payload = get_rosters_payload(
        current_app.config["SQLALCHEMY_DATABASE_URI"],
        selected_draft.database_name,
    )
    return render_template(
        "rosters.html",
        page_title="Rosters",
        selected_draft=selected_draft,
        rosters_payload=payload,
    )


@main_bp.route("/player/<int:player_id>")
@login_required
@draft_required
def player_detail(player_id: int):
    selected_draft = _selected_draft()
    player = get_player_detail(
        current_app.config["SQLALCHEMY_DATABASE_URI"],
        selected_draft.database_name,
        player_id,
    )
    if not player:
        flash("Player not found.", "danger")
        return redirect(url_for("main.team_rosters"))
    return render_template(
        "player.html",
        page_title=f"{player['first_name']} {player['last_name']}",
        selected_draft=selected_draft,
        player=player,
    )


@main_bp.route("/teams/<int:team_id>")
@login_required
@draft_required
def team_detail(team_id: int):
    selected_draft = _selected_draft()
    team = get_team_detail_payload(
        current_app.config["SQLALCHEMY_DATABASE_URI"],
        selected_draft.database_name,
        team_id,
    )
    if not team:
        flash("Team not found.", "danger")
        return redirect(url_for("main.team_rosters"))

    return render_template(
        "team.html",
        page_title=team["team_name"],
        selected_draft=selected_draft,
        team=team,
    )


@main_bp.post("/player/<int:player_id>/toggle-injured")
@login_required
@draft_required
def player_toggle_injured(player_id: int):
    if session.get("role") not in {"admin", "editor"}:
        flash("You do not have permission to update injury status.", "danger")
        return redirect(url_for("main.player_detail", player_id=player_id))

    selected_draft = _selected_draft()
    target_state = request.form.get("is_injured", "").strip().lower() == "true"

    try:
        new_state = set_player_injured_status(
            current_app.config["SQLALCHEMY_DATABASE_URI"],
            selected_draft.database_name,
            player_id,
            target_state,
        )
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("main.player_detail", player_id=player_id))

    flash("Player marked as injured." if new_state else "Player marked as healthy.", "info" if new_state else "success")
    return redirect(url_for("main.player_detail", player_id=player_id))


@main_bp.post("/player/<int:player_id>/toggle-eliminated")
@login_required
@draft_required
def player_toggle_eliminated(player_id: int):
    if session.get("role") not in {"admin", "editor"}:
        flash("You do not have permission to update elimination status.", "danger")
        return redirect(url_for("main.player_detail", player_id=player_id))

    selected_draft = _selected_draft()
    target_state = request.form.get("is_eliminated", "").strip().lower() == "true"

    try:
        new_state = set_player_elimination_status(
            current_app.config["SQLALCHEMY_DATABASE_URI"],
            selected_draft.database_name,
            player_id,
            target_state,
        )
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("main.player_detail", player_id=player_id))

    flash("Player marked as eliminated." if new_state else "Player marked as active.", "warning" if new_state else "success")
    return redirect(url_for("main.player_detail", player_id=player_id))


@main_bp.route("/bracket")
@login_required
@draft_required
def bracket():
    return render_template("placeholder.html", page_title="Bracket")


@main_bp.route("/logs")
@login_required
@draft_required
def logs():
    selected_draft = _selected_draft()
    if not selected_draft:
        flash("Select a draft first.", "danger")
        return redirect(url_for("main.select_draft"))

    create_draft_schema(current_app.config["SQLALCHEMY_DATABASE_URI"], selected_draft.database_name)

    active_log_view = request.args.get("view", "draft-events").strip().lower()
    if active_log_view not in {"draft-events", "score-changes", "user-logins"}:
        active_log_view = "draft-events"

    draft_events = get_draft_events_log(
        current_app.config["SQLALCHEMY_DATABASE_URI"],
        selected_draft.database_name,
    ) if active_log_view == "draft-events" else []

    score_changes = get_score_changes_log(
        current_app.config["SQLALCHEMY_DATABASE_URI"],
        selected_draft.database_name,
    ) if active_log_view == "score-changes" else []

    user_logins = []
    if active_log_view == "user-logins":
        rows = UserLoginEvent.query.order_by(UserLoginEvent.logged_in_at.desc(), UserLoginEvent.id.desc()).limit(500).all()
        user_logins = [
            {
                "logged_in_at": row.logged_in_at,
                "role": row.role,
                "ip_address": row.ip_address,
            }
            for row in rows
        ]

    return render_template(
        "logs.html",
        page_title="Logs",
        selected_draft=selected_draft,
        active_log_view=active_log_view,
        draft_events=draft_events,
        score_changes=score_changes,
        user_logins=user_logins,
    )


@main_bp.route("/draft-night")
@login_required
@draft_required
def draft_night():
    selected_draft = _selected_draft()
    if not selected_draft:
        flash("Select a draft first.", "danger")
        return redirect(url_for("main.select_draft"))

    create_draft_schema(current_app.config["SQLALCHEMY_DATABASE_URI"], selected_draft.database_name)
    min_ppg = request.args.get("min_ppg", default=5.0, type=float)
    if min_ppg is None:
        min_ppg = 5.0
    only_available = request.args.get("only_available", "").strip().lower() in {
        "1",
        "true",
        "on",
        "yes",
    }

    payload = get_draft_night_payload(
        current_app.config["SQLALCHEMY_DATABASE_URI"],
        selected_draft.database_name,
        selected_draft.num_draft_rounds,
    )
    roster_payload = get_team_roster_payload(
        current_app.config["SQLALCHEMY_DATABASE_URI"],
        selected_draft.database_name,
        min_ppg,
        only_available=only_available,
    )
    return render_template(
        "draft_night.html",
        page_title="Draft Night",
        selected_draft=selected_draft,
        draft_payload=payload,
        roster_payload=roster_payload,
    )


@main_bp.get("/draft-night/player-search")
@login_required
@draft_required
def draft_night_player_search():
    selected_draft = _selected_draft()
    if not selected_draft:
        return jsonify([])

    query = request.args.get("q", "").strip()
    if len(query) < 2:
        return jsonify([])

    players = search_available_players(
        current_app.config["SQLALCHEMY_DATABASE_URI"],
        selected_draft.database_name,
        query,
    )
    return jsonify(players)


@main_bp.post("/draft-night/draft-player")
@login_required
@draft_required
def draft_night_draft_player():
    selected_draft = _selected_draft()
    if not selected_draft:
        flash("Select a draft first.", "danger")
        return redirect(url_for("main.select_draft"))

    fantasy_team_id = request.form.get("fantasy_team_id", type=int)
    player_id = request.form.get("player_id", type=int)
    if not fantasy_team_id or not player_id:
        flash("Please select a valid player and draft cell.", "danger")
        return redirect(url_for("main.draft_night"))

    try:
        draft_player_pick(
            current_app.config["SQLALCHEMY_DATABASE_URI"],
            selected_draft.database_name,
            selected_draft.num_draft_rounds,
            fantasy_team_id,
            player_id,
        )
        flash("Player drafted successfully.", "success")
    except ValueError as exc:
        flash(str(exc), "danger")

    return redirect(url_for("main.draft_night"))


@main_bp.route("/admin", methods=["GET", "POST"])
@login_required
@role_required("admin")
def admin():
    return redirect(url_for("main.admin_drafts"))


@main_bp.route("/admin/drafts")
@login_required
@role_required("admin")
def admin_drafts():
    all_drafts = Draft.query.order_by(Draft.year.desc(), Draft.id.desc()).all()
    return render_template(
        "admin_drafts.html",
        drafts=all_drafts,
        active_admin_tab="drafts",
    )


@main_bp.route("/admin/drafts/<int:draft_id>")
@login_required
@role_required("admin")
def admin_draft_detail(draft_id: int):
    selected_draft = db.session.get(Draft, draft_id)
    if not selected_draft:
        flash("Draft not found.", "danger")
        return redirect(url_for("main.admin_drafts"))

    session["selected_draft_id"] = selected_draft.id
    create_draft_schema(current_app.config["SQLALCHEMY_DATABASE_URI"], selected_draft.database_name)

    draft_payload = get_admin_view_data(
        current_app.config["SQLALCHEMY_DATABASE_URI"],
        selected_draft.database_name,
    )

    return render_template(
        "admin_draft_detail.html",
        selected_draft=selected_draft,
        fantasy_teams=draft_payload["fantasy_teams"],
        owners=draft_payload["owners"],
        draft_order_locked=draft_payload["draft_order_locked"],
        roster_fetch_status=get_roster_fetch_job_status(selected_draft.id),
        active_admin_tab="drafts",
    )


@main_bp.post("/admin/randomize-draft-order")
@login_required
@role_required("admin")
def admin_randomize_draft_order():
    selected_draft = _draft_from_form_or_session()
    if not selected_draft:
        flash("Select a draft first.", "danger")
        return redirect(url_for("main.select_draft"))

    try:
        randomize_draft_order(
            current_app.config["SQLALCHEMY_DATABASE_URI"],
            selected_draft.database_name,
        )
        flash("Draft order randomized.", "success")
    except ValueError as exc:
        flash(str(exc), "danger")

    return redirect(url_for("main.admin_draft_detail", draft_id=selected_draft.id))


@main_bp.post("/admin/lock-draft-order")
@login_required
@role_required("admin")
def admin_lock_draft_order():
    selected_draft = _draft_from_form_or_session()
    if not selected_draft:
        flash("Select a draft first.", "danger")
        return redirect(url_for("main.select_draft"))

    try:
        lock_draft_order(
            current_app.config["SQLALCHEMY_DATABASE_URI"],
            selected_draft.database_name,
        )
        flash("Draft order locked in.", "success")
    except ValueError as exc:
        flash(str(exc), "danger")

    return redirect(url_for("main.admin_draft_detail", draft_id=selected_draft.id))


@main_bp.route("/admin/create-draft", methods=["GET", "POST"])
@login_required
@role_required("admin")
def admin_create_draft():
    if request.method == "GET":
        return render_template("admin_create_draft.html", active_admin_tab="create-draft")

    name = request.form.get("name", "").strip()
    year = request.form.get("year", type=int)
    num_rounds = request.form.get("num_draft_rounds", type=int) or 12

    if not name or not year:
        flash("Draft name and year are required.", "danger")
        return redirect(url_for("main.admin_create_draft"))

    slug = slugify(f"{name}-{year}")
    database_name = f"draft_{slug.replace('-', '_')}"

    existing = Draft.query.filter((Draft.slug == slug) | (Draft.database_name == database_name)).first()
    if existing:
        flash("A draft with this name/year already exists.", "danger")
        return redirect(url_for("main.admin_create_draft"))

    draft = Draft(
        name=name,
        slug=slug,
        year=year,
        is_active=False,
        database_name=database_name,
        num_draft_rounds=max(1, num_rounds),
    )
    db.session.add(draft)
    db.session.commit()

    create_draft_database(current_app.config["SQLALCHEMY_DATABASE_URI"], database_name)
    create_draft_schema(current_app.config["SQLALCHEMY_DATABASE_URI"], database_name)
    seed_teams_from_csv(current_app.config["SQLALCHEMY_DATABASE_URI"], database_name, year)

    session["selected_draft_id"] = draft.id
    flash(f"Draft '{draft.name}' created and selected.", "success")
    return redirect(url_for("main.admin_draft_detail", draft_id=draft.id))


@main_bp.post("/admin/fetch-rosters/<int:draft_id>")
@login_required
@role_required("admin")
def admin_fetch_rosters(draft_id: int):
    draft = db.session.get(Draft, draft_id)
    if not draft:
        flash("Draft not found.", "danger")
        return redirect(url_for("main.admin_drafts"))

    draft_db_url = build_draft_database_url(
        current_app.config["SQLALCHEMY_DATABASE_URI"], draft.database_name
    )

    from sqlalchemy import create_engine, text as sa_text
    engine = create_engine(draft_db_url)
    with engine.connect() as conn:
        team_names = [
            row[0] for row in conn.execute(sa_text("SELECT name FROM tbl_teams ORDER BY name"))
        ]
    engine.dispose()

    if not team_names:
        flash("No teams found in this draft — seed teams first.", "warning")
        return redirect(url_for("main.admin_draft_detail", draft_id=draft.id))

    _, started = start_roster_fetch_job(
        draft_id=draft.id,
        main_db_url=draft_db_url,
        database_name=draft.database_name,
        season_year=draft.year,
        team_names=team_names,
    )

    if started:
        flash("Roster fetch started in the background. This page will show progress.", "info")
    else:
        flash("Roster fetch is already running for this draft.", "warning")

    return redirect(url_for("main.admin_draft_detail", draft_id=draft.id))


@main_bp.post("/admin/reload-teams")
@login_required
@role_required("admin")
def admin_reload_teams():
    selected_draft = _draft_from_form_or_session()
    if not selected_draft:
        flash("Select a draft first.", "danger")
        return redirect(url_for("main.select_draft"))

    try:
        create_draft_schema(current_app.config["SQLALCHEMY_DATABASE_URI"], selected_draft.database_name)
        result = reload_teams_from_csv(
            current_app.config["SQLALCHEMY_DATABASE_URI"],
            selected_draft.database_name,
            selected_draft.year,
        )

        if result.get("inserted", 0) > 0:
            flash(f"Reloaded teams from seed file. Inserted {result['inserted']} teams.", "warning")
        else:
            flash("Teams were cleared, but no seed file data was inserted.", "warning")
    except Exception:
        flash("Failed to reload teams.", "danger")

    return redirect(url_for("main.admin_draft_detail", draft_id=selected_draft.id))


@main_bp.get("/admin/fetch-rosters/<int:draft_id>/status")
@login_required
@role_required("admin")
def admin_fetch_rosters_status(draft_id: int):
    status = get_roster_fetch_job_status(draft_id)
    if not status:
        return jsonify({"status": "idle"})
    return jsonify(status)


@main_bp.post("/admin/update-draft")
@login_required
@role_required("admin")
def admin_update_draft():
    draft_id = request.form.get("draft_id", type=int)
    is_active = request.form.get("is_active") == "on"

    draft = db.session.get(Draft, draft_id)
    if not draft:
        flash("Draft not found.", "danger")
        return redirect(url_for("main.admin_drafts"))

    if is_active:
        Draft.query.update({Draft.is_active: False})
        draft.is_active = True
    else:
        draft.is_active = False

    db.session.commit()
    flash("Draft status updated.", "success")
    return redirect(url_for("main.admin_draft_detail", draft_id=draft.id))


@main_bp.post("/admin/add-fantasy-team")
@login_required
@role_required("admin")
def admin_add_fantasy_team():
    selected_draft = _draft_from_form_or_session()
    if not selected_draft:
        flash("Select a draft first.", "danger")
        return redirect(url_for("main.select_draft"))

    name = request.form.get("name", "").strip()
    if not name:
        flash("Fantasy team name is required.", "danger")
        return redirect(url_for("main.admin_draft_detail", draft_id=selected_draft.id))

    try:
        add_fantasy_team(
            current_app.config["SQLALCHEMY_DATABASE_URI"],
            selected_draft.database_name,
            name,
        )
        flash("Fantasy team added.", "success")
    except Exception:
        flash("Unable to add fantasy team. Team name may already exist.", "danger")
    return redirect(url_for("main.admin_draft_detail", draft_id=selected_draft.id))


@main_bp.post("/admin/delete-fantasy-team/<int:team_id>")
@login_required
@role_required("admin")
def admin_delete_fantasy_team(team_id: int):
    selected_draft = _draft_from_form_or_session()
    if not selected_draft:
        flash("Select a draft first.", "danger")
        return redirect(url_for("main.select_draft"))

    remove_fantasy_team(
        current_app.config["SQLALCHEMY_DATABASE_URI"],
        selected_draft.database_name,
        team_id,
    )
    flash("Fantasy team removed.", "info")
    return redirect(url_for("main.admin_draft_detail", draft_id=selected_draft.id))


@main_bp.post("/admin/add-owner")
@login_required
@role_required("admin")
def admin_add_owner():
    selected_draft = _draft_from_form_or_session()
    if not selected_draft:
        flash("Select a draft first.", "danger")
        return redirect(url_for("main.select_draft"))

    name = request.form.get("name", "").strip()
    email = request.form.get("email", "").strip()
    if not name:
        flash("Owner name is required.", "danger")
        return redirect(url_for("main.admin_draft_detail", draft_id=selected_draft.id))

    try:
        add_owner(
            current_app.config["SQLALCHEMY_DATABASE_URI"],
            selected_draft.database_name,
            name,
            email,
        )
        flash("Owner added.", "success")
    except Exception:
        flash("Unable to add owner.", "danger")
    return redirect(url_for("main.admin_draft_detail", draft_id=selected_draft.id))


@main_bp.post("/admin/delete-owner/<int:owner_id>")
@login_required
@role_required("admin")
def admin_delete_owner(owner_id: int):
    selected_draft = _draft_from_form_or_session()
    if not selected_draft:
        flash("Select a draft first.", "danger")
        return redirect(url_for("main.select_draft"))

    remove_owner(
        current_app.config["SQLALCHEMY_DATABASE_URI"],
        selected_draft.database_name,
        owner_id,
    )
    flash("Owner removed.", "info")
    return redirect(url_for("main.admin_draft_detail", draft_id=selected_draft.id))


@main_bp.post("/admin/assign-owner-team")
@login_required
@role_required("admin")
def admin_assign_owner_team():
    selected_draft = _draft_from_form_or_session()
    if not selected_draft:
        flash("Select a draft first.", "danger")
        return redirect(url_for("main.select_draft"))

    owner_id = request.form.get("owner_id", type=int)
    fantasy_team_id = request.form.get("fantasy_team_id", type=int)
    if not owner_id or not fantasy_team_id:
        flash("Owner and fantasy team are required for assignment.", "danger")
        return redirect(url_for("main.admin_draft_detail", draft_id=selected_draft.id))

    assign_owner_to_fantasy_team(
        current_app.config["SQLALCHEMY_DATABASE_URI"],
        selected_draft.database_name,
        owner_id,
        fantasy_team_id,
    )
    flash("Owner assigned to fantasy team.", "success")
    return redirect(url_for("main.admin_draft_detail", draft_id=selected_draft.id))


@main_bp.post("/admin/unassign-owner-team")
@login_required
@role_required("admin")
def admin_unassign_owner_team():
    selected_draft = _draft_from_form_or_session()
    if not selected_draft:
        flash("Select a draft first.", "danger")
        return redirect(url_for("main.select_draft"))

    owner_id = request.form.get("owner_id", type=int)
    fantasy_team_id = request.form.get("fantasy_team_id", type=int)
    if not owner_id or not fantasy_team_id:
        flash("Owner and fantasy team are required.", "danger")
        return redirect(url_for("main.admin_draft_detail", draft_id=selected_draft.id))

    unassign_owner_from_fantasy_team(
        current_app.config["SQLALCHEMY_DATABASE_URI"],
        selected_draft.database_name,
        owner_id,
        fantasy_team_id,
    )
    flash("Owner-team assignment removed.", "info")
    return redirect(url_for("main.admin_draft_detail", draft_id=selected_draft.id))