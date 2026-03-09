# March Madness Fantasy Manager

Initial MVP scaffold for a Flask-based fantasy manager app with Docker Compose services for:

- `nginx` reverse proxy
- `web` Flask application
- `db` PostgreSQL

## Tech Stack

- Python + Flask
- SQLAlchemy ORM
- PostgreSQL
- Nginx
- Docker Compose

## Quick Start

1. Create local environment file:

   ```bash
   cp .env.example .env
   ```

2. Start the app:

   ```bash
   docker compose up --build
   ```

3. Open:

   - http://localhost:8080

## Auth Roles

Login page supports shared role accounts:

- `viewer`
- `editor`
- `admin`

Passwords come from `.env`:

- `VIEWER_PASSWORD`
- `EDITOR_PASSWORD`
- `ADMIN_PASSWORD`

## Current MVP Features

- Role/password login
- Draft selection
- Admin page to:
  - Create new draft (creates a dedicated PostgreSQL database for that draft)
  - Update draft config (`num_draft_rounds`, active flag)
  - Add/remove fantasy teams
  - Add/remove owners
- Placeholder pages for other tabs:
  - Leaderboard
  - Fantasy Teams
  - Team Rosters
  - Bracket
  - Draft Night

## Data Model

Main app database:

- `drafts`: stores each draft instance and target `database_name`

Per-draft database tables are provisioned automatically:

- `tbl_teams`
- `tbl_players`
- `tbl_player_points`
- `tbl_fantasy_teams`
- `tbl_player_draft_event`
- `tbl_owners`
- `tbl_owner_fantasy_teams`
- `tbl_bracket`
- `tbl_stg_games`
- `tbl_stg_box_scores`