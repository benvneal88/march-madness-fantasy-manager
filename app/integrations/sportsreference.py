"""
Sports-Reference.com web scraping integration.

Rate limit: 10 requests per minute enforced by _rate_limited_get().
"""
from __future__ import annotations

import collections
import logging
import re
import threading
import time
from datetime import datetime

import pandas
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text

from app.models.draft import tbl_sportsref_school_index, tbl_sportsref_school_roster

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rate limiter: 10 requests per 100 seconds (sliding window) for sports-reference.com
# ---------------------------------------------------------------------------
_RATE_LIMIT_REQUESTS = 10
_RATE_LIMIT_WINDOW = 100  # seconds

_rate_lock = threading.Lock()
_request_timestamps: collections.deque[float] = collections.deque()


def _available_request_slots() -> int:
    """Return currently available rate-limit slots in the active window."""
    with _rate_lock:
        now = time.monotonic()
        while _request_timestamps and now - _request_timestamps[0] >= _RATE_LIMIT_WINDOW:
            _request_timestamps.popleft()
        return max(0, _RATE_LIMIT_REQUESTS - len(_request_timestamps))


def _rate_limited_get(url: str, **kwargs) -> requests.Response:
    """Perform a GET request respecting the 10 requests/minute rate limit.

    Uses a sliding-window algorithm. The lock is released during any sleep so
    that other threads can also check and wait concurrently.
    """
    while True:
        with _rate_lock:
            now = time.monotonic()
            # Evict timestamps that have aged out of the window
            while _request_timestamps and now - _request_timestamps[0] >= _RATE_LIMIT_WINDOW:
                _request_timestamps.popleft()

            if len(_request_timestamps) < _RATE_LIMIT_REQUESTS:
                # Slot available — reserve it and break out of the retry loop
                _request_timestamps.append(now)
                sleep_for = 0.0
                break

            # No slot yet — compute how long to wait before the oldest slot frees up
            sleep_for = _RATE_LIMIT_WINDOW - (now - _request_timestamps[0])

        # Sleep outside the lock so other threads are not blocked
        logger.info("Rate limit reached for sports-reference.com, sleeping %.1fs", sleep_for)
        time.sleep(max(sleep_for, 0.0))

    headers = kwargs.pop("headers", {})
    headers.setdefault(
        "User-Agent",
        "Mozilla/5.0 (compatible; mmfm-scraper/1.0; +https://github.com/mmfm)",
    )
    response = requests.get(url, headers=headers, timeout=30, **kwargs)
    response.raise_for_status()
    return response


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _transform_school_list_raw(data_str: str) -> pandas.DataFrame:
    """Parse the sports-reference schools index HTML and return a DataFrame.

    Columns: all columns from the #schools table plus 'url' (relative href).
    """
    soup = BeautifulSoup(data_str, "html.parser")
    school_html = soup.find(id="basic_school_stats")

    dfs = pandas.read_html(str(school_html), flavor="bs4")
    schools_df = dfs[0]

    # Flatten multi-level column headers if present
    if isinstance(schools_df.columns, pandas.MultiIndex):
        schools_df.columns = [
            " ".join(str(c) for c in col).strip() if isinstance(col, tuple) else col
            for col in schools_df.columns
        ]
    # Rename the school name column to a predictable label
    school_col = next(
        (c for c in schools_df.columns if "school" in str(c).lower()),
        schools_df.columns[0],
    )
    schools_df = schools_df.rename(columns={school_col: "School"})

    # Extract href links from the tbody
    link_dict: dict[str, str] = {}
    table = school_html.find("tbody")
    for tr in table.find_all("tr"):
        for td in tr.find_all("td"):
            try:
                link = td.find("a")["href"]
                school_name = td.find("a").get_text(strip=True)
                link_dict[school_name] = link
            except (TypeError, KeyError):
                pass

    schools_df["url"] = schools_df["School"].map(link_dict)
    # Some cells have a suffix appended with a non-breaking space, e.g.
    # "Alabama\xa0NCAA" — strip everything from the first \xa0 onward so the
    # school name matches the link_dict key (anchor text only).
    schools_df["School"] = (
        schools_df["School"].astype(str).str.split("\xa0").str[0].str.strip()
    )
    # Re-apply the map with the normalised names
    schools_df["url"] = schools_df["School"].map(link_dict)
    # Drop header rows that sport-reference.com repeats mid-table
    schools_df = schools_df[schools_df["School"] != "School"]
    schools_df = schools_df.dropna(subset=["School"])

    return schools_df.reset_index(drop=True)


_SUMMARY_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?).*?([0-9]+(?:\.[0-9]+)?).*?([0-9]+(?:\.[0-9]+)?)")
_NAME_RE = re.compile(r"([A-Za-z'\.\-]+)\s+([A-Za-z'\.\- ]+)")


def _transform_roster_raw(data_str: str, school_name: str) -> pandas.DataFrame:
    """Parse a school roster page and return a flattened DataFrame.

    Columns after transformation:
        School, #, First Name, Last Name, Pos, Class, Ht, Wt, Hometown,
        PPG, RPG, APG
    """

    def _parse_summary(summary_str: str) -> tuple[str | None, str | None, str | None]:
        if not isinstance(summary_str, str):
            return None, None, None
        m = re.search(_SUMMARY_RE, summary_str)
        if not m:
            return None, None, None
        return m.group(1), m.group(2), m.group(3)

    def _parse_name(name_str: str) -> tuple[str, str]:
        if not isinstance(name_str, str):
            return "error", "error"
        m = re.match(_NAME_RE, name_str.strip())
        if not m:
            logger.warning("Unable to parse first/last name from %r", name_str)
            return "error", "error"
        return m.group(1), m.group(2).strip()

    soup = BeautifulSoup(data_str, "html.parser")
    roster_html = soup.find(id="roster")

    dfs = pandas.read_html(str(roster_html), flavor="bs4")
    roster_df = dfs[0]

    # Flatten multi-level column headers if present
    if isinstance(roster_df.columns, pandas.MultiIndex):
        roster_df.columns = [
            " ".join(str(c) for c in col).strip() if isinstance(col, tuple) else col
            for col in roster_df.columns
        ]

    roster_df["School"] = school_name

    if "Summary" in roster_df.columns:
        roster_df["PPG"], roster_df["RPG"], roster_df["APG"] = zip(
            *roster_df["Summary"].apply(_parse_summary)
        )
        roster_df = roster_df.drop(columns=["Summary"])
    else:
        roster_df["PPG"] = None
        roster_df["RPG"] = None
        roster_df["APG"] = None

    if "Player" in roster_df.columns:
        roster_df["First Name"], roster_df["Last Name"] = zip(
            *roster_df["Player"].apply(_parse_name)
        )
        roster_df = roster_df.drop(columns=["Player"])

    return roster_df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Public scraping functions
# ---------------------------------------------------------------------------

_BASE_URL = "https://www.sports-reference.com"
_SCHOOL_INDEX_URL_TEMPLATE = (
    "https://www.sports-reference.com/cbb/seasons/men/{year}-school-stats.html"
)


def scrape_school_index(db_url: str, season_year: int) -> int:
    """Download and parse the sports-reference schools index for *season_year*.

    Replaces any existing rows for that season in tbl_sportsref_school_index
    and returns the number of rows inserted.

    Args:
        db_url: SQLAlchemy connection string for the target database.
        season_year: NCAA season year (e.g. 2023).
    """
    url = _SCHOOL_INDEX_URL_TEMPLATE.format(year=season_year)
    logger.info("Fetching school index for %d from %s", season_year, url)

    response = _rate_limited_get(url)
    df = _transform_school_list_raw(response.text)

    now = datetime.utcnow()
    records = [
        {
            "school_name": row["School"],
            "url": row.get("url"),
            "season_year": season_year,
            "scraped_at": now,
        }
        for _, row in df.iterrows()
        if pandas.notna(row.get("School"))
    ]

    engine = create_engine(db_url)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM tbl_sportsref_school_index WHERE season_year = :year"
                ),
                {"year": season_year},
            )
            if records:
                conn.execute(tbl_sportsref_school_index.insert(), records)
    finally:
        engine.dispose()

    logger.info("Loaded %d schools into tbl_sportsref_school_index", len(records))
    return len(records)


def scrape_school_roster(
    school_url: str,
    school_name: str,
    db_url: str,
    season_year: int,
) -> int:
    """Download and parse a school's roster page from sports-reference.com.

    Replaces any existing roster rows for *school_name* / *season_year* in
    tbl_sportsref_school_roster and returns the number of rows inserted.

    Args:
        school_url: Relative or absolute URL to the school roster page,
            e.g. ``/cbb/schools/air-force/men/2023.html``.
        school_name: Human-readable school name used as the key in the DB.
        db_url: SQLAlchemy connection string for the target database.
        season_year: NCAA season year (e.g. 2023).
    """
    full_url = school_url if school_url.startswith("http") else _BASE_URL + school_url
    logger.info("Fetching roster for %s from %s", school_name, full_url)

    response = _rate_limited_get(full_url)
    df = _transform_roster_raw(response.text, school_name)

    def _safe_float(val) -> float | None:
        try:
            f = float(val)
            return None if pandas.isna(f) else f
        except (TypeError, ValueError):
            return None

    def _safe_str(val) -> str | None:
        if val is None or (isinstance(val, float) and pandas.isna(val)):
            return None
        return str(val).strip() or None

    now = datetime.utcnow()
    records = []
    for _, row in df.iterrows():
        records.append(
            {
                "school_name": school_name,
                "season_year": season_year,
                "jersey_number": _safe_str(row.get("#")),
                "first_name": _safe_str(row.get("First Name")),
                "last_name": _safe_str(row.get("Last Name")),
                "position": _safe_str(row.get("Pos") or row.get("Position")),
                "class_year": _safe_str(row.get("Class")),
                "height": _safe_str(row.get("Ht") or row.get("Height")),
                "weight": _safe_str(row.get("Wt") or row.get("Weight")),
                "hometown": _safe_str(row.get("Hometown")),
                "ppg": _safe_float(row.get("PPG")),
                "rpg": _safe_float(row.get("RPG")),
                "apg": _safe_float(row.get("APG")),
                "scraped_at": now,
            }
        )

    engine = create_engine(db_url)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM tbl_sportsref_school_roster"
                    " WHERE school_name = :name AND season_year = :year"
                ),
                {"name": school_name, "year": season_year},
            )
            if records:
                conn.execute(tbl_sportsref_school_roster.insert(), records)
    finally:
        engine.dispose()

    logger.info(
        "Loaded %d players for %s into tbl_sportsref_school_roster",
        len(records),
        school_name,
    )
    return len(records)


def fetch_rosters_for_teams(
    db_url: str,
    team_names: list[str],
    season_year: int,
    max_rosters_per_run: int = 5,
) -> dict[str, list[str]]:
    """Scrape the school index then fetch a roster for every team in *team_names*.

    Steps:
    1. Ensure tbl_sportsref_school_index is present for *season_year*.
    2. Build a case-insensitive lookup from school name → relative URL.
    3. For each team name, match against the index and call scrape_school_roster.

    Returns a result object with:
    - unmatched: team names that could not be matched or failed to scrape.
    - skipped_existing: team names skipped because roster rows already exist.
    - fetched: team names successfully fetched in this run.
    - deferred: team names matched but deferred due to per-run/rate-limit budget.

    Args:
        db_url: SQLAlchemy connection string for the draft database.
        team_names: School names to fetch rosters for (typically from tbl_teams).
        season_year: NCAA season year (e.g. 2025).
        max_rosters_per_run: Max number of rosters to fetch in one request.
    """
    max_rosters_per_run = max(1, int(max_rosters_per_run))

    # 1. Ensure we have school index rows for this season; avoid refreshing every
    # request to reduce API calls and prevent long blocking requests.
    engine = create_engine(db_url)
    try:
        with engine.connect() as conn:
            index_count = conn.execute(
                text(
                    "SELECT COUNT(*) FROM tbl_sportsref_school_index WHERE season_year = :year"
                ),
                {"year": season_year},
            ).scalar_one()
    finally:
        engine.dispose()

    if int(index_count) == 0:
        scrape_school_index(db_url, season_year)

    # 2. Read the index back and build a normalised lookup
    engine = create_engine(db_url)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT school_name, url FROM tbl_sportsref_school_index"
                    " WHERE season_year = :year"
                ),
                {"year": season_year},
            ).mappings().all()
    finally:
        engine.dispose()

    index_by_name: dict[str, str] = {
        row["school_name"].strip().lower(): row["url"]
        for row in rows
        if row["url"]
    }

    # 3. Match and scrape
    # Build a set of teams already downloaded for this season so we can skip
    # redundant network requests.
    engine = create_engine(db_url)
    try:
        with engine.connect() as conn:
            existing_rows = conn.execute(
                text(
                    "SELECT DISTINCT school_name FROM tbl_sportsref_school_roster"
                    " WHERE season_year = :year"
                ),
                {"year": season_year},
            ).mappings().all()
    finally:
        engine.dispose()

    existing_rosters = {
        str(row["school_name"]).strip().lower()
        for row in existing_rows
        if row.get("school_name")
    }

    unmatched: list[str] = []
    skipped_existing: list[str] = []
    fetched: list[str] = []
    deferred: list[str] = []

    # Budget fetches so a single web request does not block long enough to hit
    # gunicorn worker timeout.
    available_slots = _available_request_slots()
    fetch_budget = min(max_rosters_per_run, available_slots)

    for team_name in team_names:
        team_key = team_name.strip().lower()
        if team_key in existing_rosters:
            logger.info("Skipping roster fetch for %r (already downloaded)", team_name)
            skipped_existing.append(team_name)
            continue

        url = index_by_name.get(team_key)
        if not url:
            logger.warning("No sports-reference match for team %r", team_name)
            unmatched.append(team_name)
            continue

        if fetch_budget <= 0:
            deferred.append(team_name)
            continue

        try:
            scrape_school_roster(url, team_name, db_url, season_year)
            fetched.append(team_name)
            fetch_budget -= 1
        except Exception as exc:
            logger.error("Failed to scrape roster for %r: %s", team_name, exc)
            unmatched.append(team_name)

    return {
        "unmatched": unmatched,
        "skipped_existing": skipped_existing,
        "fetched": fetched,
        "deferred": deferred,
    }
