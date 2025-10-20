from flask import (
    Flask,
    Response,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    stream_with_context,
    url_for,
)
from flask_sqlalchemy import SQLAlchemy
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
import os
import json
import datetime as dt
from datetime import datetime, date
import time
from google.oauth2.credentials import Credentials
from functions.help_functions import *
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from typing import Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from werkzeug.utils import secure_filename
import pyotp
from google.cloud import storage
from google.oauth2 import service_account
import calendar
import uuid
from functions import PROJECT_ROOT
from redis.exceptions import RedisError
from rq import Queue
from rq.job import Job
from rq.exceptions import NoSuchJobError
import requests
from bs4 import BeautifulSoup

from jobs.analytical_report_job import run_analytical_report_job
from services.analytical_jobs import AnalyticalJobStore, get_redis_connection, utc_now_iso

app = Flask(__name__)
app.secret_key = 'teamheretics'

BUCKET_NAME = "bucket-reports1"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "api_keys/reports.json"
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

app.config.setdefault(
    "ANALYTICAL_REPORT_SHARE_EMAIL",
    os.getenv("ANALYTICAL_REPORT_SHARE_EMAIL", "pablolopezarauzo@gmail.com"),
)
app.config.setdefault(
    "ANALYTICAL_REPORT_CREDENTIALS",
    os.getenv("ANALYTICAL_REPORT_CREDENTIALS", "api_keys/valorant-sheets-credentials.json"),
)
app.config.setdefault("REDIS_URL", os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))

redis_connection = None
analytical_queue = None
analytical_job_store = None

try:
    redis_connection = get_redis_connection(app.config["REDIS_URL"])
    # Force a connection attempt so we fail fast if Redis is offline.
    redis_connection.ping()
except RedisError as exc:
    app.logger.warning("Redis unavailable for analytical reports: %s", exc)
    redis_connection = None
    analytical_queue = None
    analytical_job_store = None
else:
    analytical_queue = Queue("analytical-reports", connection=redis_connection, default_timeout=1200)
    analytical_job_store = AnalyticalJobStore(redis_connection)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# User Model
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(150), unique=True, nullable=False)
    password = db.Column(db.String(150), nullable=False)
    role = db.Column(db.String(50), nullable=False)

# Database Model
class Event(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100), nullable=False)
    start = db.Column(db.String(100), nullable=False)
    end = db.Column(db.String(100), nullable=True)
    description = db.Column(db.Text, nullable=True)

def upload_to_gcs(file, destination_blob_name):
    """Uploads a file to GCS and returns its public URL."""
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_file(file, content_type='application/pdf')

    # Generate a signed URL that expires in 1 hour
    expiration = dt.timedelta(hours=1)
    signed_url = blob.generate_signed_url(expiration=expiration, method="GET")
    
    return signed_url


with app.app_context():
    db.create_all()    

SERVICE_ACCOUNT_FILE = "api_keys/keys_api_scrims.json"
SCOPES = ["https://www.googleapis.com/auth/calendar"]
#calendar_id = "c_e7db020ac3b822514bd2fda49f5974cf6c61d22187dafead645ab7348cb9fd14@group.calendar.google.com"  # Replace with the actual calendar ID
calendar_id = "pablolopezarauzo@gmail.com"

def get_calendar_service():
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    # Delegate access to the coach's calendar
    service = build('calendar', 'v3', credentials=credentials)
    return service

def get_scrim_data():
    SPREADSHEET_ID = "1YlYRJ3ZTc6f425nX0X3h1xQXzPCG7yRzG6S1iQjqv3I"
    RANGE_NAME = 'Scrim Day Insights!A3:K1100'
    # Path to your service account credentials
    
    # Define the required scope
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    
    # Create credentials and build the service
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    
    # Call the Sheets API to get the data
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    values = result.get('values', [])
    return values

MAP_RANKING_PLAYERS = [
    {"id": "miniboo", "label": "MiniBoo"},
    {"id": "woot", "label": "Wo0t"},
    {"id": "riens", "label": "RieNs"},
    {"id": "benjy", "label": "Benjy"},
    {"id": "boo", "label": "Boo"},
]

DATA_DIR = PROJECT_ROOT / "data"
STATIC_DIR = PROJECT_ROOT / "static"
DATA_DIR.mkdir(parents=True, exist_ok=True)
PUUID_LIST_PATH = STATIC_DIR / "puuid_list.json"
OPPONENTS_PATH = STATIC_DIR / "opponents.json"

with OPPONENTS_PATH.open("r", encoding="utf-8") as f:
    opponents_data = json.load(f)

ANALYTICAL_LIBRARY_FILE = DATA_DIR / "analytical_reports.json"
ANALYTICAL_STATIC_REPORTS = [
    {
        "team_name": "MIBR",
        "created_label": "Sep 23, 2025",
        "spreadsheet_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vSm8s3rrv6EX8K8GuHoYXdjWkNjwD5amepYKHjy3v8JpVYW4xQw7xnkQ78FBTM3KeG4ygApIkFD8jNG/pubhtml",
        "source": "legacy",
    },
    {
        "team_name": "T1",
        "created_label": "Sep 16, 2025",
        "spreadsheet_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vQWl_UeYQ7q-dQx1qli8lTQw1yQSJUr9xYPVBwM2JU6JWxfdaxDMY6erp6YP6PJ95Af1a3HRkjbrpUy/pubhtml",
        "source": "legacy",
    },
    {
        "team_name": "G2 Esports",
        "created_label": "Sep 4, 2025",
        "spreadsheet_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vQYwi5MJ-e1y-UfVacDqZIagt0jEVQG0p71Gq-KdGw-1lT3TVEewAy5b5MOwh326qEhrzynhyhKjsuO/pubhtml",
        "source": "legacy",
    },
    {
        "team_name": "Team Heretics",
        "created_label": "Sep 3, 2025",
        "spreadsheet_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vQFz5eUhn8LiYr_70QgqvSW4IYP-hAkwXFmhzdYlDAX82WFbYj04wDyVQR9T2l8uI1glxZAKeircUo1/pubhtml",
        "source": "legacy",
    },
    {
        "team_name": "Paper Rex (only EWC)",
        "created_label": "May 26, 2025",
        "spreadsheet_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vTPdAniu0_3AZZxJ_BlnWfsHlpthgC5yRuarM0vJMOe5Wfk8ZVTzldZnT3j05BEKan7Ry-4vcB_p5u4/pubhtml",
        "source": "legacy",
    },
    {
        "team_name": "Team Heretics (self-report)",
        "created_label": "May 1, 2025",
        "spreadsheet_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vRQ0TQlFVcvLbtDOoRJ2afqMc3fT622xjIojg1CfH7Mpuhcpu5LYyYq7-yOS2uHfO2snX6CamwOpoCd/pubhtml",
        "source": "legacy",
    },
    {
        "team_name": "Team Vitality",
        "created_label": "January 30, 2025",
        "spreadsheet_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vT4u_2qPpXqvQ7hemqCcqWkbAcwX79znF6AcVGUN4Gu3koLvzYZe3mh0KxPUTrp4T1Nma75zDdGKv5u/pubhtml",
        "source": "legacy",
    },
    {
        "team_name": "BBL Esports",
        "created_label": "January 24, 2025",
        "spreadsheet_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vSaY9mAspet0vgszrkmLnh2zxvkNSwd-0C2XDXlm2R45_eGRSIO4goWGVaCLI5fYoafK77mGHq8fJfL/pubhtml",
        "source": "legacy",
    },
    {
        "team_name": "Gentle Mates",
        "created_label": "January 17, 2025",
        "spreadsheet_url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vTTqlYT6B9dryMktHIWL88he6N25xoBfnrPv30J7T6lhbsK41vflMcPE8Bp85rwmWsB-YFf0J6u-J86/pubhtml",
        "source": "legacy",
    },
]

def _clean_url(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if isinstance(value, str):
        value = value.strip()
    return value or None


def _derive_csv_url(csv_url: Optional[str], spreadsheet_url: Optional[str]) -> Optional[str]:
    """Return a publish CSV URL for a Google Sheet when possible."""
    if csv_url:
        return csv_url
    if not spreadsheet_url:
        return None
    try:
        parsed = urlparse(spreadsheet_url)
    except Exception:
        return None

    if "docs.google.com" not in parsed.netloc:
        return None

    query_params = parse_qs(parsed.query or "")
    gid = query_params.get("gid", ["0"])[0]

    path = parsed.path or ""
    if path.endswith("/pubhtml"):
        base_path = path[: -len("/pubhtml")]
        return f"{parsed.scheme}://{parsed.netloc}{base_path}/pub?gid={gid}&output=csv"

    if path.endswith("/pub"):
        flattened = {key: values[-1] for key, values in query_params.items() if values}
        flattened.setdefault("gid", gid)
        flattened["output"] = "csv"
        return f"{parsed.scheme}://{parsed.netloc}{path}?{urlencode(flattened)}"

    return None


SHEET_META_CACHE: dict[str, dict] = {}
SHEET_META_CACHE_TTL = 300  # seconds


def _extract_gid(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    parsed = urlparse(value)
    candidates = [parsed.fragment, parsed.query]
    for candidate in candidates:
        if not candidate:
            continue
        params = parse_qs(candidate)
        gid_values = params.get("gid")
        if gid_values:
            return gid_values[-1]
    return None


def _normalise_published_html_url(spreadsheet_url: str) -> str:
    parsed = urlparse(spreadsheet_url)
    if parsed.scheme not in ("http", "https") or "docs.google.com" not in parsed.netloc:
        raise ValueError("Only Google Sheets publish URLs are supported.")

    query_params = parse_qs(parsed.query)
    normalised_query_items: list[tuple[str, str]] = []
    if "gid" in query_params:
        normalised_query_items.append(("gid", query_params["gid"][-1]))
    if "single" in query_params:
        normalised_query_items.append(("single", query_params["single"][-1]))

    normalised_query = urlencode(normalised_query_items)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", normalised_query, ""))


def _parse_published_sheet_metadata(html_text: str) -> tuple[list[dict], Optional[str]]:
    soup = BeautifulSoup(html_text, "html.parser")
    menu = soup.find("ul", id="sheet-menu")

    sheets: list[dict] = []
    if menu:
        for link in menu.find_all("a"):
            gid = _extract_gid(link.get("href") or link.get("data-sheets-gid"))
            if not gid:
                continue
            title = link.get_text(separator=" ", strip=True) or f"Sheet {len(sheets) + 1}"
            sheets.append({"gid": gid, "title": title})

    doc_title = None
    title_el = soup.find(id="doc-title")
    if title_el:
        doc_title = title_el.get_text(separator=" ", strip=True)

    return sheets, doc_title


def get_published_sheet_metadata(spreadsheet_url: str) -> dict:
    if not spreadsheet_url:
        raise ValueError("Missing spreadsheet URL.")

    normalised_url = _normalise_published_html_url(spreadsheet_url)
    default_gid = _extract_gid(spreadsheet_url) or "0"

    cached = SHEET_META_CACHE.get(normalised_url)
    now_monotonic = time.monotonic()
    if cached and now_monotonic - cached["timestamp"] < SHEET_META_CACHE_TTL:
        return cached["data"]

    response = requests.get(normalised_url, timeout=10)
    response.raise_for_status()

    sheets, doc_title = _parse_published_sheet_metadata(response.text)
    if not sheets:
        sheets = [{"gid": default_gid, "title": "Overview"}]
    else:
        deduped: list[dict] = []
        seen: set[str] = set()
        for sheet in sheets:
            gid = sheet.get("gid")
            if not gid or gid in seen:
                continue
            deduped.append(sheet)
            seen.add(gid)
        sheets = deduped
        if default_gid and all(sheet["gid"] != default_gid for sheet in sheets):
            sheets.insert(0, {"gid": default_gid, "title": "Overview"})

    metadata = {
        "title": doc_title,
        "default_gid": default_gid,
        "sheets": sheets,
    }
    SHEET_META_CACHE[normalised_url] = {"timestamp": now_monotonic, "data": metadata}
    return metadata


def _resolve_spreadsheet_open_url(
    edit_url: Optional[str], view_url: Optional[str], default_url: Optional[str]
) -> Optional[str]:
    for candidate in (edit_url, view_url, default_url):
        if candidate:
            return candidate
    return None


def load_analytical_library() -> list[dict]:
    ANALYTICAL_LIBRARY_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not ANALYTICAL_LIBRARY_FILE.exists():
        ANALYTICAL_LIBRARY_FILE.write_text("[]", encoding="utf-8")
        return []
    try:
        with ANALYTICAL_LIBRARY_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def save_analytical_library(entries: list[dict]) -> None:
    ANALYTICAL_LIBRARY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with ANALYTICAL_LIBRARY_FILE.open("w", encoding="utf-8") as f:
        json.dump(entries, f, indent=2)


def format_created_label(iso_timestamp: str) -> str:
    if not iso_timestamp:
        return ""
    raw = str(iso_timestamp).strip()
    if not raw:
        return ""
    normalised = raw.replace("Z", "+00:00")
    try:
        dt_obj = datetime.fromisoformat(normalised)
    except Exception:
        try:
            dt_obj = datetime.fromisoformat(normalised.split(".")[0])
        except Exception:
            return raw
    if dt_obj.tzinfo is not None:
        dt_obj = dt_obj.astimezone()
    return dt_obj.strftime("%b %d, %Y")


def build_team_indexes(teams: list[dict]) -> Tuple[dict, dict]:
    """Return helper maps keyed by tag and name."""
    by_tag = {team["tag"].upper(): team for team in teams}
    by_name = {team["name"].strip().lower(): team for team in teams}
    return by_tag, by_name


def resolve_team_choice(raw_input: str, teams_by_tag: dict, teams_by_name: dict) -> Optional[dict]:
    """Map user input to a team entry with a few parsing heuristics."""
    if not raw_input:
        return None
    candidate = raw_input.strip()
    if not candidate:
        return None

    lookup = teams_by_tag.get(candidate.upper())
    if lookup:
        return lookup

    lookup = teams_by_name.get(candidate.lower())
    if lookup:
        return lookup

    if "(" in candidate and ")" in candidate:
        inner = candidate.split("(", 1)[1].split(")", 1)[0].strip()
        lookup = teams_by_tag.get(inner.upper())
        if lookup:
            return lookup

    tokens = candidate.replace("(", " ").replace(")", " ").split()
    for token in reversed(tokens):
        lookup = teams_by_tag.get(token.upper())
        if lookup:
            return lookup
    return None

# Decorator to check user role
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash("You need to log in first.", 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def login_required_with_2fa(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash("You need to log in first.", 'warning')
            return redirect(url_for('login'))

        if 'authenticated' not in session:
            return redirect(url_for('two_factor_auth'))  # Redirect to 2FA page if not authenticated via 2FA

        return f(*args, **kwargs)
    return decorated_function

@app.before_request
def restrict_pages():
    open_routes = [
        'index',
        'login',
        'logout',
        'signup',
        'pricing',
        'pick_bans',
        'static',
        'two_factor_auth',
    ]  # endpoints that stay open
    if request.endpoint in open_routes or request.endpoint is None:
        return

    # Require login
    if 'user_id' not in session:
        flash("You need to log in first.", 'warning')
        return redirect(url_for('login'))

    # Require role admin or user
    if session.get('role') not in ['admin', 'member']:
        flash("You do not have permission to access this page.", 'danger')
        return redirect(url_for('login'))



# Home route (public landing)
@app.route('/')
def index():
    return render_template('home.html', active_page="home")

@app.route('/stats')
@login_required  # Protect this page
def stats():
    return render_template('stats.html', active_page="stats")


@app.route('/map-rankings', methods=['GET', 'POST'])
@login_required
def map_rankings():
    stored_rankings = load_map_rankings()
    form_values = {player["id"]: "" for player in MAP_RANKING_PLAYERS}
    errors = {}

    if request.method == 'POST':
        action = request.form.get('action', 'compute')

        if action == 'delete':
            ranking_id = request.form.get('ranking_id')
            if session.get('role') != 'admin':
                flash('You do not have permission to delete rankings.', 'danger')
                return redirect(url_for('map_rankings'))

            if not ranking_id:
                flash('Unable to delete the ranking. Please try again.', 'danger')
                return redirect(url_for('map_rankings'))

            new_rankings = [item for item in stored_rankings if item.get('id') != ranking_id]
            if len(new_rankings) == len(stored_rankings):
                flash('Ranking not found. It may have already been removed.', 'warning')
            else:
                save_map_rankings(new_rankings)
                flash('Ranking removed.', 'success')
            return redirect(url_for('map_rankings'))

        parsed_orders = {}
        for player in MAP_RANKING_PLAYERS:
            field_id = player["id"]
            raw_text = request.form.get(field_id, '')
            form_values[field_id] = raw_text

            if not raw_text.strip():
                errors[field_id] = "Please paste this player's ranking."
                continue

            order, found = parse_map_ranking(raw_text)
            if not found:
                errors[field_id] = "No recognizable maps found. Double-check the spellings."
                continue

            parsed_orders[player["label"]] = order

        if len(parsed_orders) != len(MAP_RANKING_PLAYERS):
            flash("We couldn't compute the ranking yet. Fix the highlighted players and try again.", 'danger')
        else:
            averages, ordered_maps = compute_average_map_ranking(parsed_orders)
            timestamp = dt.datetime.utcnow()
            entry = {
                "id": str(uuid.uuid4()),
                "created_at": timestamp.isoformat(timespec='seconds'),
                "date": timestamp.date().isoformat(),
                "average_order": ordered_maps,
                "average_scores": averages,
                "players": {},
            }

            for player in MAP_RANKING_PLAYERS:
                label = player["label"]
                order = parsed_orders[label]
                entry["players"][label] = {
                    "input": form_values[player["id"]],
                    "order": order,
                    "positions": order_to_positions(order),
                }

            stored_rankings.append(entry)
            save_map_rankings(stored_rankings)
            flash('Map ranking saved successfully.', 'success')
            return redirect(url_for('map_rankings', open=entry["id"]))

    stored_rankings = load_map_rankings()
    rankings_sorted = sorted(
        stored_rankings,
        key=lambda item: item.get('created_at', ''),
        reverse=True
    )

    display_rankings = []
    for item in rankings_sorted:
        created_iso = item.get('created_at')
        created_display = created_iso
        if created_iso:
            try:
                created_dt = dt.datetime.fromisoformat(created_iso)
                created_display = created_dt.strftime('%B %d, %Y • %H:%M')
            except ValueError:
                pass

        average_scores = item.get('average_scores') or item.get('average') or {}
        average_order = item.get('average_order') or []
        if not average_order:
            average_order = sorted(
                MAP_POOL,
                key=lambda name: (
                    average_scores.get(name, float('inf')) if average_scores.get(name) is not None else float('inf'),
                    name
                )
            )

        average_rows = []
        for map_name in average_order:
            score = average_scores.get(map_name)
            average_rows.append({
                'map': map_name,
                'score': score,
                'display': f"{score:.2f}" if isinstance(score, (int, float)) else '–'
            })

        players_block = []
        players_data = item.get('players', {})
        for label, details in players_data.items():
            order = details.get('order', [])
            positions = details.get('positions') or order_to_positions(order)
            players_block.append({
                'name': label,
                'order': order,
                'positions': positions,
                'raw_input': details.get('input', ''),
            })

        players_block.sort(key=lambda entry: entry['name'])

        display_rankings.append({
            'id': item.get('id'),
            'created_at_iso': created_iso,
            'created_at_label': created_display,
            'date': item.get('date'),
            'average_rows': average_rows,
            'players': players_block,
        })

    open_ranking_id = request.args.get('open')

    return render_template(
        'map_rankings.html',
        active_page="map_rankings",
        players=MAP_RANKING_PLAYERS,
        form_values=form_values,
        errors=errors,
        rankings=display_rankings,
        map_pool=MAP_POOL,
        can_delete=session.get('role') == 'admin',
        open_ranking_id=open_ranking_id,
    )

@app.route('/scrims')
@login_required
def scrims():
    mode = request.args.get('mode', 'preset')
    scrim_type = (request.args.get('scrim_type', 'all') or 'all').strip().lower()
    
    if mode == 'preset':
        # Use the 'month' parameter to compute the date interval.
        month_str = request.args.get('month')
        year = 2025  # fixed year as per requirements
        if month_str:
            month = int(month_str)
        else:
            # Default to current month if month is missing
            today = datetime.date.today()
            month = today.month
        start_date = datetime.date(year, month, 1)
        last_day = calendar.monthrange(year, month)[1]
        end_date = datetime.date(year, month, last_day)
        start_date_str = start_date.isoformat()
        end_date_str = end_date.isoformat()
        title_text = f"{start_date.strftime('%B')} Winrates"
    elif mode == 'custom':
        # Use provided start_date and end_date parameters
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        if not start_date_str or not end_date_str:
            today = datetime.date.today()
            start_date = today.replace(day=1)
            last_day = calendar.monthrange(today.year, today.month)[1]
            end_date = today.replace(day=last_day)
            start_date_str = start_date.isoformat()
            end_date_str = end_date.isoformat()
        else:
            start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()
        # Human-friendly title, e.g., "From 1st August 2025 to 17th August 2025"
        def _ordinal(n: int) -> str:
            return "th" if 11 <= (n % 100) <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")

        def _human(d):
            return f"{d.day}{_ordinal(d.day)} {d.strftime('%B %Y')}"

        title_text = f"From {_human(start_date)} to {_human(end_date)}"
    else:
        # fallback if no mode specified
        today = datetime.date.today()
        start_date = today.replace(day=1)
        last_day = calendar.monthrange(today.year, today.month)[1]
        end_date = today.replace(day=last_day)
        start_date_str = start_date.isoformat()
        end_date_str = end_date.isoformat()
        title_text = ""

    # Fetch all scrim data from Google Sheets
    scrim_data = get_scrim_data()  # Replace with your function to fetch data
    scrim_data = process_scrim_data(scrim_data)

    # Determine the latest scrim date present in the sheet (for header context)
    latest_scrim_date = None
    for row in scrim_data:
        if not row or row[0] in ['VOD LINK', 'DATE']:
            continue
        try:
            d = datetime.datetime.strptime(row[0], '%B %d, %Y').date()
        except ValueError:
            continue
        if latest_scrim_date is None or d > latest_scrim_date:
            latest_scrim_date = d

    # Filter scrim data based on the selected date range
    filtered_data = []
    for row in scrim_data:
        if not row or row[0] == 'VOD LINK' or row[0] == 'DATE':
            continue
        try:
            scrim_date = datetime.datetime.strptime(row[0], '%B %d, %Y').date()
        except ValueError:
            continue
        if start_date <= scrim_date <= end_date:
            filtered_data.append(row)

    # Optional filter by scrim type (column index 1)
    def _matches_scrim_type(value: str) -> bool:
        if scrim_type in (None, '', 'all'):
            return True
        v = (value or '').strip().lower()
        if scrim_type in ('grey', 'gray'):
            return ('grey' in v) or ('gray' in v)
        return scrim_type in v  # substring match to be resilient (e.g., "green scrim")

    filtered_data = [r for r in filtered_data if len(r) > 1 and _matches_scrim_type(r[1] if len(r) > 1 else '')]
    win_stats = compute_win_stats(filtered_data)
    # Baseline: year average metrics for comparison (same year as selection)
    # Determine baseline year
    baseline_year = None
    if mode == 'preset':
        baseline_year = 2025
    else:
        try:
            baseline_year = start_date.year
        except Exception:
            baseline_year = datetime.date.today().year

    # Filter to full calendar year for baseline
    baseline_start = datetime.date(baseline_year, 1, 1)
    baseline_end = datetime.date(baseline_year, 12, 31)
    baseline_filtered = []
    for row in scrim_data:
        if not row or row[0] in ['VOD LINK', 'DATE']:
            continue
        try:
            bd = datetime.datetime.strptime(row[0], '%B %d, %Y').date()
        except ValueError:
            continue
        if baseline_start <= bd <= baseline_end:
            baseline_filtered.append(row)

    # Apply same scrim-type filter to baseline for fair comparison
    baseline_filtered = [r for r in baseline_filtered if len(r) > 1 and _matches_scrim_type(r[1] if len(r) > 1 else '')]

    baseline_win_stats = compute_win_stats(baseline_filtered)

    def _aggregate_wr(stats_list):
        if not stats_list:
            return {
                'overall': 0,
                'def_wr': 0,
                'atk_wr': 0,
                'pistol_wr': 0,
            }
        t_games = sum(s.get('games', 0) for s in stats_list)
        wins = sum(s.get('wins', 0) for s in stats_list)
        overall = round((wins / t_games) * 100) if t_games > 0 else 0
        n_maps = len(stats_list)
        def_wr = round(sum(s.get('def_winrate', 0) for s in stats_list) / n_maps) if n_maps > 0 else 0
        atk_wr = round(sum(s.get('atk_winrate', 0) for s in stats_list) / n_maps) if n_maps > 0 else 0
        pistol_wr = 0
        if n_maps > 0:
            pistol_wr = round((sum(s.get('def_pistol', 0) for s in stats_list) + sum(s.get('atk_pistol', 0) for s in stats_list)) / (2 * n_maps))
        return {
            'overall': overall,
            'def_wr': def_wr,
            'atk_wr': atk_wr,
            'pistol_wr': pistol_wr,
        }

    baseline_wr = _aggregate_wr(baseline_win_stats)
    agent_winrates = get_agent_winrates(scrim_data)
    # Head-to-head (computed on filtered data to reflect current selection)
    head_to_head = compute_head_to_head_summary(filtered_data)
    opp_comps = get_scrim_teams(scrim_data)
    
    return render_template(
        'scrims.html', 
        active_page="scrims", 
        win_stats=win_stats, 
        start_date_default=start_date_str, 
        end_date_default=end_date_str,
        agent_winrates=agent_winrates,
        mode=mode,
        title_text=title_text,
        teams=opp_comps,
        last_scrim_date=latest_scrim_date.strftime('%B %d, %Y') if latest_scrim_date else None,
        baseline_overall_wr=baseline_wr['overall'],
        baseline_def_wr=baseline_wr['def_wr'],
        baseline_atk_wr=baseline_wr['atk_wr'],
        baseline_pistol_wr=baseline_wr['pistol_wr'],
        scrim_type=scrim_type,
        head_to_head=head_to_head,
    )




@app.route('/pick&bans', methods=['GET', 'POST'])
@login_required  # Protect this page
def pick_bans():
    # On a normal GET, just render the page (the JS will attach to the one blank form)
    if request.method == 'GET':
        return render_template('pick&bans.html',
        active_page="pick&bans")

    # For POST, always run help_scrape on the submitted form
    team = request.form.get('team')
    events = request.form.getlist('events')
    table, overview = help_scrape(team, events)
    recency_weight = request.form.get('recency_weight', 1)

    if request.form.get('analysis'):
        resp = client.responses.create(
            model="o4-mini",
            input=[{"role":"system","content":"You are a helpful analyst."},
                   {"role":"user","content": ai_prompt(overview, team, recency_weight)}],
            reasoning={"effort":"low"},
            max_output_tokens=4000,
            stream=True
        )
        def generate():
            try:
                yield 'event: start\ndata: {"type":"start"}\n\n'
                for event in resp:
                    if event.type == "response.output_text.done":
                        analysis_data = json.loads(event.text)
                        html = render_template(
                            '_pickbans_analysis.html',
                            team=team,
                            analysis=analysis_data
                        )
                        payload = json.dumps({"html": html})
                        yield f"data: {payload}\n\n"
                        return
            except Exception as stream_err:
                err_payload = json.dumps({"error": str(stream_err)})
                yield f"data: {err_payload}\n\n"


        # tell Flask to stream as SSE (you can also use chunked JSON)
        return Response(
            stream_with_context(generate()),
            headers={
                "Content-Type":        "text/event-stream",
                "Cache-Control":       "no-cache",
                "X-Accel-Buffering":   "no",      # for Nginx
            }
        )

    # If coming via AJAX, return just the rendered partial
    if request.form.get('ajax'):
        html = render_template(
            '_pickbans_results.html',
            selected_team=team,
            selected_events=events,
            table=table,
            overview=overview,
            active_page="pick&bans"
        )
        return jsonify({'html': html})

    # (Optional) If you still want to support non-JS fallback:
    return render_template(
        'pick&bans.html',
        # you could prefill the first container with these values...
        initial_team=team,
        initial_events=events,
        initial_table=table,
        initial_overview=overview,
        active_page="pick&bans"
    )

@app.route('/performance')
@login_required  # Protect this page
def performance():
    return render_template('performance.html', active_page="performance")

# valoplant route updated to load and display saved strategies
@app.route('/valoplant')
@login_required  # Protect this page
def valoplant():
    strategies = load_strategies()  # Load saved strategies
    return render_template('valoplant.html', active_page="valoplant", strategies=strategies)

"""
@app.route('/strategic_reports')
@login_required
def strategic():
    reports_dir_25 = app.config.get('REPORTS_DIR', 'static/reports/2025')
    teams_25 = {}
    for file_name in os.listdir(reports_dir_25):
        if file_name.endswith(".pdf"):  # We are interested only in .pdf files
            # Assuming the format is "TEAMNAME_MapName.pdf"
            team_name, map_name, date = file_name.split('_')
            date = date.replace(".pdf", "")  # Clean the map name from the extension
            date = convert_number_to_date(date) + ", 2025"
            print(team_name, map_name, date)
            # If the team doesn't exist in the dictionary, create a new list
            if team_name not in teams_25:
                teams_25[team_name] = []

            # Add map information to the team's list
            teams_25[team_name].append({
                "map": map_name,
                "file": file_name,
                "date": date
            })

    reports_dir_24 = app.config.get('REPORTS_DIR', 'static/reports/2024')
    teams_24 = {}
    for file_name in os.listdir(reports_dir_24):
        if file_name.endswith(".pdf"):  # We are interested only in .pdf files
            # Assuming the format is "TEAMNAME_MapName.pdf"
            team_name, map_name, date = file_name.split('_')
            date = date.replace(".pdf", "")  # Clean the map name from the extension
            date = convert_number_to_date(date) + ", 2024"
            # If the team doesn't exist in the dictionary, create a new list
            if team_name not in teams_24:
                teams_24[team_name] = []

            # Add map information to the team's list
            teams_24[team_name].append({
                "map": map_name,
                "file": file_name,
                "date": date
            })
    def parse_date(team):
        try:
            # Parse the date string into a datetime object
            date_str = team[1][0]["date"]
            return datetime.datetime.strptime(date_str, "%B %dth, %Y")  # Adjust format to match the input date
        except ValueError:
            # Handle cases where "th", "st", "nd", etc., are part of the day
            date_str = team[1][0]["date"].replace("th", "").replace("st", "").replace("nd", "").replace("rd", "")
            return datetime.datetime.strptime(date_str, "%B %d, %Y")
    teams_25 = dict(sorted(teams_25.items(), key=parse_date, reverse=True))
    teams_24 = dict(sorted(teams_24.items(), key=parse_date, reverse=True))
    print(teams_24)
    return render_template('strat_reports.html', teams_24=teams_24, teams_25=teams_25, active_page="strategic")
"""

def list_reports_from_gcs(prefix):
    """Lists reports in GCS bucket under the given prefix (e.g., '2025/' or '2024/')."""
    blobs = bucket.list_blobs(prefix=prefix)
    reports = {}

    for blob in blobs:
        filename = blob.name.split("/")[-1]  # Extract the filename
        if filename.endswith(".pdf"):
            try:
                parts = filename.replace(".pdf", "").split("_")
                # Team Name = First part
                team_name = parts[0]

                # Map Name = Second part
                map_name = parts[1]

                # Date = Remaining parts joined together
                date_str = " ".join(parts[2:])
                formatted_date = convert_number_to_date(date_str)

                if team_name not in reports:
                    reports[team_name] = []

                # Generate a signed URL for the report file
                signed_url = blob.generate_signed_url(expiration=dt.timedelta(hours=1), method="GET")
                
                reports[team_name].append({
                    "map": map_name,
                    "file_url": signed_url,  # Use the signed URL instead of the public URL
                    "date": formatted_date
                })
            except ValueError:
                print(f"Skipping file with unexpected format: {filename}")
    
    # Sort teams by the most recent date
    def parse_date(team):
        try:
            date_str = team[1][0]["date"]
            # Use dateutil parser which is more flexible than strptime
            return parser.parse(date_str)
        except Exception as e:
            print(f"Failed to parse date: {date_str} ({e})")
            return datetime.datetime.min  # fallback so sorting still works

    return dict(sorted(reports.items(), key=parse_date, reverse=True))


@app.route("/strategic_reports")
def strategic():
    teams_25 = list_reports_from_gcs("2025/")
    teams_24 = list_reports_from_gcs("2024/")
    return render_template("strat_reports.html", teams_24=teams_24, teams_25=teams_25, active_page="strategic")



def build_analytical_generator_context() -> dict:
    error = None
    share_email_default = app.config.get("ANALYTICAL_REPORT_SHARE_EMAIL", "pablolopezarauzo@gmail.com")
    saved_reports_entries = load_analytical_library()
    teams: list[dict] = []

    try:
        teams_raw = get_teams()
    except Exception as exc:
        error = f"Could not load teams list: {exc}"
    else:
        teams = [
            {"tag": t["tag"], "name": t.get("name", t["tag"])}
            for t in (teams_raw.values() if isinstance(teams_raw, dict) else teams_raw)
        ]
        teams.sort(key=lambda item: item["name"])

    redis_available = analytical_job_store is not None and analytical_queue is not None and redis_connection is not None

    active_job_meta: dict = {}
    initial_events: list[dict] = []
    active_job_id = session.get("analytical_active_job")
    if not redis_available:
        active_job_id = None
    elif active_job_id:
        active_job_meta = analytical_job_store.get_meta(active_job_id)
        if active_job_meta:
            initial_events = list(analytical_job_store.log_lines(active_job_id))
        else:
            active_job_id = None
            session.pop("analytical_active_job", None)
            session.modified = True

    last_form = session.get("analytical_last_form", {})
    selected_team_input = last_form.get("team", "")
    match_count_value = last_form.get("match_count", "")
    share_email_value = last_form.get("share_email") or share_email_default

    saved_reports_display = []
    for entry in saved_reports_entries:
        spreadsheet_url = _clean_url(entry.get("spreadsheet_url"))
        spreadsheet_edit_url = _clean_url(entry.get("spreadsheet_edit_url"))
        spreadsheet_view_url = _clean_url(entry.get("spreadsheet_view_url"))
        spreadsheet_csv_url_raw = _clean_url(entry.get("spreadsheet_csv_url"))
        spreadsheet_csv_url = _derive_csv_url(spreadsheet_csv_url_raw, spreadsheet_url)
        open_url = _resolve_spreadsheet_open_url(spreadsheet_edit_url, spreadsheet_view_url, spreadsheet_url)
        report_payload = entry.get("report_payload")

        saved_reports_display.append(
            {
                "team_name": entry.get("team_name") or entry.get("team_tag") or "Unknown team",
                "team_tag": entry.get("team_tag"),
                "created_label": format_created_label(entry.get("created_at", "")),
                "spreadsheet_url": spreadsheet_url,
                "spreadsheet_edit_url": spreadsheet_edit_url,
                "spreadsheet_view_url": spreadsheet_view_url,
                "spreadsheet_csv_url": spreadsheet_csv_url,
                "match_count": entry.get("match_count"),
                "entry_id": entry.get("entry_id") or entry.get("id") or entry.get("spreadsheet_url"),
                "source": "saved",
                "open_url": open_url,
                "report_payload": report_payload,
                "has_payload": bool(report_payload),
            }
        )

    legacy_reports_display = []
    for raw_entry in ANALYTICAL_STATIC_REPORTS:
        entry = dict(raw_entry)
        spreadsheet_url = _clean_url(entry.get("spreadsheet_url"))
        spreadsheet_edit_url = _clean_url(entry.get("spreadsheet_edit_url"))
        spreadsheet_view_url = _clean_url(entry.get("spreadsheet_view_url"))
        spreadsheet_csv_url_raw = _clean_url(entry.get("spreadsheet_csv_url"))
        spreadsheet_csv_url = _derive_csv_url(spreadsheet_csv_url_raw, spreadsheet_url)
        open_url = _resolve_spreadsheet_open_url(spreadsheet_edit_url, spreadsheet_view_url, spreadsheet_url)
        entry.update(
            {
                "spreadsheet_url": spreadsheet_url,
                "spreadsheet_edit_url": spreadsheet_edit_url,
                "spreadsheet_view_url": spreadsheet_view_url,
                "spreadsheet_csv_url": spreadsheet_csv_url,
                "open_url": open_url,
                "source": entry.get("source", "legacy"),
                "report_payload": entry.get("report_payload"),
                "has_payload": bool(entry.get("report_payload")),
            }
        )
        legacy_reports_display.append(entry)

    all_reports = saved_reports_display + legacy_reports_display

    return {
        "error": error,
        "teams": teams,
        "selected_team_input": selected_team_input,
        "match_count_value": match_count_value,
        "share_email_value": share_email_value,
        "all_reports": all_reports,
        "saved_reports": saved_reports_display,
        "legacy_reports": legacy_reports_display,
        "initial_job_id": active_job_id,
        "initial_job_meta": active_job_meta,
        "initial_terminal_events": initial_events,
        "share_email_default": share_email_default,
        "redis_available": redis_available,
    }


@app.route("/analytical_reports", methods=["GET"])
@login_required  # Protect this page
def analytical():
    context = build_analytical_generator_context()
    all_reports = context.pop("all_reports")
    return render_template(
        "ana_reports_overview.html",
        active_page="analytical",
        all_reports = all_reports,
        **context,
    )


@app.route("/analytical_reports/generator", methods=["GET"])
@login_required
def analytical_generator():
    context = build_analytical_generator_context()
    return render_template(
        "ana_reports_generate.html",
        active_page="analytical",
        **context,
    )


@app.route("/analytical_reports/jobs", methods=["POST"])
@login_required
def create_analytical_job():
    if analytical_job_store is None or analytical_queue is None:
        return (
            jsonify(
                {
                    "error": "Real-time generation is currently unavailable. Please ensure Redis is running and try again.",
                    "details": "Redis connection failed during startup.",
                }
            ),
            503,
        )

    payload = request.get_json(silent=True) or request.form
    share_email_default = app.config.get("ANALYTICAL_REPORT_SHARE_EMAIL", "pablolopezarauzo@gmail.com")
    raw_team = (payload.get("team") or "").strip()
    match_count_raw = (payload.get("match_count") or "").strip()
    share_email_value = (payload.get("share_email") or "").strip() or share_email_default

    try:
        teams_raw = get_teams()
    except Exception as exc:
        return jsonify({"error": f"Could not load teams list: {exc}"}), 500

    teams = [
        {"tag": t["tag"], "name": t.get("name", t["tag"])}
        for t in (teams_raw.values() if isinstance(teams_raw, dict) else teams_raw)
    ]
    teams.sort(key=lambda item: item["name"])
    teams_by_tag, teams_by_name = build_team_indexes(teams)

    if not raw_team:
        return jsonify({"error": "Please pick a team to analyze."}), 400

    try:
        match_count = int(match_count_raw) if match_count_raw else None
        if match_count is not None and match_count <= 0:
            raise ValueError
    except ValueError:
        return jsonify({"error": "Match count must be a positive integer."}), 400

    if share_email_value and "@" not in share_email_value:
        return jsonify({"error": "Please provide a valid email address."}), 400

    match_team = resolve_team_choice(raw_team, teams_by_tag, teams_by_name)
    if not match_team:
        return jsonify({"error": "Team not recognized. Please pick a roster from the list."}), 400

    selected_team_tag = match_team["tag"]
    job_id = uuid.uuid4().hex

    try:
        analytical_job_store.bootstrap(
            job_id,
            team_tag=selected_team_tag,
            match_count=match_count,
            share_email=share_email_value or None,
            created_by=session.get("username"),
        )
        analytical_job_store.merge_meta(
            job_id,
            {
                "team_name": match_team.get("name"),
            },
        )
        analytical_job_store.append_event(
            job_id,
            "progress",
            {"message": "Job queued. Waiting for the worker to pick it up."},
        )
        rq_job = analytical_queue.enqueue(
            run_analytical_report_job,
            job_id,
            team_tag=selected_team_tag,
            match_count=match_count,
            share_email=share_email_value or None,
            credentials_path=app.config.get("ANALYTICAL_REPORT_CREDENTIALS"),
            job_id=f"analytical-{job_id}",
            description=f"Analytical report for {selected_team_tag}",
            result_ttl=86400,
            failure_ttl=86400,
        )
    except Exception as exc:
        app.logger.exception("Failed to enqueue analytical report job")
        analytical_job_store.update_status(job_id, "failed", error=str(exc))
        return jsonify({"error": f"Could not enqueue job: {exc}"}), 500

    session["analytical_active_job"] = job_id
    session["analytical_last_form"] = {
        "team": selected_team_tag,
        "match_count": match_count_raw,
        "share_email": share_email_value,
    }
    session.modified = True

    meta = analytical_job_store.get_meta(job_id)
    events = list(analytical_job_store.log_lines(job_id))

    return jsonify(
        {
            "job_id": job_id,
            "rq_job_id": rq_job.id,
            "status": meta.get("status"),
            "meta": meta,
            "events": events,
        }
    ), 202


@app.route("/analytical_reports/jobs/<job_id>", methods=["GET"])
@login_required
def analytical_job_details(job_id: str):
    if analytical_job_store is None:
        return (
            jsonify(
                {
                    "error": "Live job tracking is unavailable because Redis is offline.",
                    "details": "Start Redis before requesting job details.",
                }
            ),
            503,
        )
    meta = analytical_job_store.get_meta(job_id)
    if not meta:
        return jsonify({"error": "Job not found."}), 404
    events = list(analytical_job_store.log_lines(job_id))
    return jsonify({"job_id": job_id, "meta": meta, "events": events})


@app.route("/analytical_reports/jobs/<job_id>/stream", methods=["GET"])
@login_required
def analytical_job_stream(job_id: str):
    if analytical_job_store is None or redis_connection is None:
        abort(503, description="Live streaming is unavailable because Redis is offline.")
    meta = analytical_job_store.get_meta(job_id)
    if not meta:
        abort(404, description="Job not found.")

    keys = analytical_job_store.keys(job_id)
    pubsub = redis_connection.pubsub()
    pubsub.subscribe(keys.channel)

    def generate_stream():
        try:
            for event in analytical_job_store.log_lines(job_id):
                event_type = event.get("type", "progress")
                yield f"event: {event_type}\n"
                yield f"data: {json.dumps(event)}\n\n"
            while True:
                message = pubsub.get_message(timeout=15)
                if message and message["type"] == "message":
                    raw = message["data"]
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8")
                    try:
                        event = json.loads(raw)
                        event_type = event.get("type", "progress")
                    except Exception:
                        event = {
                            "type": "progress",
                            "origin": "system",
                            "payload": {"message": str(raw)},
                            "timestamp": utc_now_iso(),
                        }
                        event_type = "progress"
                    yield f"event: {event_type}\n"
                    yield f"data: {json.dumps(event)}\n\n"
                else:
                    yield ": keep-alive\n\n"
        except GeneratorExit:
            return
        finally:
            try:
                pubsub.unsubscribe(keys.channel)
                pubsub.close()
            except Exception:
                pass

    response = Response(stream_with_context(generate_stream()), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    return response


@app.route("/analytical_reports/jobs/<job_id>/input", methods=["POST"])
@login_required
def analytical_job_input(job_id: str):
    if analytical_job_store is None:
        return (
            jsonify(
                {
                    "error": "Terminal input is disabled while Redis is offline.",
                    "details": "Restart Redis to continue the interactive session.",
                }
            ),
            503,
        )
    meta = analytical_job_store.get_meta(job_id)
    if not meta:
        return jsonify({"error": "Job not found."}), 404

    payload = request.get_json(silent=True) or request.form
    message = (payload.get("message") or payload.get("terminal_message") or "").strip()
    if not message:
        return jsonify({"error": "Message cannot be empty."}), 400

    prompt_id = (payload.get("prompt_id") or "").strip() or None
    analytical_job_store.push_user_input(
        job_id,
        message,
        author=session.get("username"),
        prompt_id=prompt_id,
    )
    return jsonify({"ok": True})


@app.route("/analytical_reports/jobs/<job_id>/cancel", methods=["POST"])
@login_required
def cancel_analytical_job(job_id: str):
    if analytical_job_store is None:
        return (
            jsonify(
                {
                    "error": "Cancellation unavailable while Redis is offline.",
                    "details": "Restart Redis to manage live jobs.",
                }
            ),
            503,
        )

    meta = analytical_job_store.get_meta(job_id)
    if not meta:
        return jsonify({"error": "Job not found."}), 404

    status = (meta.get("status") or "").lower()
    if status in {"finished", "failed", "cancelled"}:
        return jsonify({"message": "Job already completed.", "status": status}), 200

    cancelled_by = session.get("username")
    analytical_job_store.request_cancel(job_id, cancelled_by=cancelled_by)

    if redis_connection is not None:
        try:
            rq_job = Job.fetch(f"analytical-{job_id}", connection=redis_connection)
            rq_job.cancel()
        except NoSuchJobError:
            pass

    # Also clear the entire analytical queue so no further jobs remain pending
    # This ensures the whole report generation pipeline is halted as requested.
    if analytical_queue is not None:
        try:
            analytical_queue.empty()
        except Exception:
            app.logger.exception("Failed to clear analytical queue during cancellation")

    if session.get("analytical_active_job") == job_id:
        session.pop("analytical_active_job", None)
        session.modified = True

    return jsonify({"message": "Cancellation requested.", "status": "cancelling"}), 202


@app.route("/analytical_reports/preview/meta", methods=["GET"])
@login_required
def analytical_report_preview_meta():
    spreadsheet_url = (request.args.get("spreadsheet_url") or "").strip()
    if not spreadsheet_url:
        return jsonify({"error": "Missing spreadsheet URL."}), 400

    try:
        metadata = get_published_sheet_metadata(spreadsheet_url)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except requests.RequestException as exc:
        app.logger.warning("Failed to fetch published sheet metadata: %s", exc)
        return jsonify(
            {"error": "Unable to contact Google Sheets to load the report preview right now."}
        ), 502
    except Exception:
        app.logger.exception("Unexpected error while fetching published sheet metadata")
        return jsonify({"error": "Unexpected error while preparing the report preview."}), 500

    return jsonify(metadata)


@app.route("/analytical_reports/library", methods=["POST"])
@login_required
def save_analytical_report_entry():
    data = request.get_json(silent=True) or request.form
    spreadsheet_url = (data.get("report_spreadsheet_url") or data.get("spreadsheet_url") or "").strip()
    if not spreadsheet_url:
        return jsonify({"error": "Missing spreadsheet URL to save."}), 400

    saved_entries = load_analytical_library()
    if any(entry.get("spreadsheet_url") == spreadsheet_url for entry in saved_entries):
        return jsonify({"message": "This report is already in the quick access list.", "status": "exists"}), 200

    team_name = (data.get("report_team_name") or data.get("team_name") or "").strip() or "Unknown team"
    team_tag = (data.get("report_team_tag") or data.get("team_tag") or "").strip() or None
    match_count = (data.get("report_match_count") or data.get("match_count") or "").strip()
    created_at_iso = (data.get("report_created_at") or data.get("created_at") or dt.datetime.utcnow().isoformat())
    spreadsheet_edit_url = (
        data.get("report_spreadsheet_edit_url") or data.get("spreadsheet_edit_url") or ""
    ).strip()
    spreadsheet_view_url = (
        data.get("report_spreadsheet_view_url") or data.get("spreadsheet_view_url") or ""
    ).strip()
    spreadsheet_csv_url = _derive_csv_url(
        (data.get("report_spreadsheet_csv_url") or data.get("spreadsheet_csv_url") or "").strip(),
        spreadsheet_url,
    )
    spreadsheet_id = (data.get("report_spreadsheet_id") or data.get("spreadsheet_id") or "").strip()
    raw_payload = data.get("report_payload")
    if isinstance(raw_payload, str):
        try:
            report_payload = json.loads(raw_payload)
        except Exception:
            report_payload = None
    else:
        report_payload = raw_payload if isinstance(raw_payload, dict) else None

    entry_id = uuid.uuid4().hex
    entry = {
        "team_name": team_name,
        "team_tag": team_tag,
        "spreadsheet_url": spreadsheet_url,
        "match_count": match_count,
        "created_at": created_at_iso,
        "entry_id": entry_id,
    }
    if spreadsheet_edit_url:
        entry["spreadsheet_edit_url"] = spreadsheet_edit_url
    if spreadsheet_view_url:
        entry["spreadsheet_view_url"] = spreadsheet_view_url
    if spreadsheet_csv_url:
        entry["spreadsheet_csv_url"] = spreadsheet_csv_url
    if spreadsheet_id:
        entry["spreadsheet_id"] = spreadsheet_id
    if report_payload is not None:
        entry["report_payload"] = report_payload
        entry["has_payload"] = True
    else:
        entry["has_payload"] = False
    saved_entries.insert(0, entry)
    save_analytical_library(saved_entries)
    return jsonify({"message": "Report saved to the quick access list.", "status": "saved", "entry": entry})


@app.route("/analytical_reports/library", methods=["DELETE"])
@login_required
def delete_analytical_report_entry():
    data = request.get_json(silent=True) or {}
    target_url = (data.get("report_spreadsheet_url") or data.get("spreadsheet_url") or "").strip()
    if not target_url:
        target_url = (request.args.get("spreadsheet_url") or "").strip()
    if not target_url:
        return jsonify({"error": "Missing report identifier to delete."}), 400

    saved_entries = load_analytical_library()
    remaining_entries = [entry for entry in saved_entries if entry.get("spreadsheet_url") != target_url]
    if len(remaining_entries) == len(saved_entries):
        return jsonify({"error": "Report not found in saved library."}), 404

    save_analytical_library(remaining_entries)
    return jsonify({"message": "Report removed from the quick access list.", "status": "deleted"})
    
@app.template_filter()
def time_filter(date_str):
    date = convert_number_to_date(date_str)
    return date

from functions.rankeds import (
    get_players_data,
    regenerate_kda,
    get_other_players_data,
    RANKEDS_FILE,
    RANKEDS_KDA_FILE,
    RANKEDS_OTHER_FILE,
)
@app.route('/rankeds')
@login_required
def rankeds():
    today = datetime.date.today()
    default_start = today - timedelta(weeks=2)

    # parse 1st range
    start_str = request.args.get("start_date", default_start.isoformat())
    end_str   = request.args.get("end_date",   today.isoformat())
    try:
        start_date = datetime.datetime.strptime(start_str, "%Y-%m-%d").date()
        end_date   = datetime.datetime.strptime(end_str,   "%Y-%m-%d").date()
    except ValueError:
        start_date, end_date = default_start, today
        start_str, end_str = start_date.isoformat(), end_date.isoformat()

    # parse 2nd range
    start_str_2 = request.args.get("start_date_2", default_start.isoformat())
    end_str_2   = request.args.get("end_date_2",   today.isoformat())
    try:
        start_date_2 = datetime.datetime.strptime(start_str_2, "%Y-%m-%d").date()
        end_date_2   = datetime.datetime.strptime(end_str_2, "%Y-%m-%d").date()
    except ValueError:
        start_date_2, end_date_2 = default_start, today
        start_str_2, end_str_2   = start_date_2.isoformat(), end_date_2.isoformat()

    # decide which table to rebuild
    which = request.args.get('which', "rankeds")
    with PUUID_LIST_PATH.open("r", encoding="utf-8") as handle:
        full_list = json.load(handle)
    if which is None:
        get_players_data(start_date, end_date)
        get_other_players_data(start_date_2, end_date_2, full_list)
    elif which == "rankeds":
        get_players_data(start_date, end_date)
    elif which == "full_list":
        get_other_players_data(start_date_2, end_date_2, full_list)

    # now read both data files (they’re up-to-date)
    with RANKEDS_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)
        number_competitive = {p: sum(d["competitive"] for d in days.values()) for p, days in data.items()}
        number_deathmatchs = {p: sum(d["deathmatch"] for d in days.values()) for p, days in data.items()}
        number_hurms        = {p: sum(d["hurm"]        for d in days.values()) for p, days in data.items()}

    with RANKEDS_KDA_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)
        kd  = {p: round(d["all"][0]/d["all"][1], 2) for p, d in data.items()}
        vlr = {p: d["all"][2]/d["all"][1]          for p, d in data.items()}

    with RANKEDS_OTHER_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)
        rankds_2 = {p: sum(d["competitive"] for d in days.values()) for p, days in data.items()}
        dms_2    = {p: sum(d["deathmatch"]   for d in days.values()) for p, days in data.items()}
        hurms_2  = {p: sum(d["hurm"]         for d in days.values()) for p, days in data.items()}

    return render_template(
        'rankeds2.html',
        active_page="rankeds",
        role=session['role'],
        which=which,

        # 1st table
        start_date=start_str,
        end_date=end_str,
        number_competitive=number_competitive,
        number_deathmatchs=number_deathmatchs,
        number_hurms=number_hurms,
        kd=kd,

        # 2nd table
        start_date_2=start_str_2,
        end_date_2=end_str_2,
        players=full_list,
        rankds_2=rankds_2,
        dms_2=dms_2,
        hurms_2=hurms_2,
    )



@app.route('/update_kda', methods=['POST'])
@login_required
def update_kda():
    # only admins can do this
    if session.get('role') != 'admin':
        return jsonify(success=False, message="Unauthorized"), 403

    try:
        regenerate_kda()
        return jsonify(success=True)
    except Exception as e:
        return jsonify(success=False, message=str(e)), 500


@app.route('/search')
@login_required
def search():
    return render_template('search.html', active_page="search")

@app.route("/search_player", methods=["POST"])
@login_required  # Protect this page
def search_player():
    player_name = request.form.get("player_name")
    #time_range = request.form.get("time_range")
    time_range = "last_week"
    try:
        with OPPONENTS_PATH.open("r", encoding="utf-8") as json_file:
            data = json.load(json_file)
        if player_name not in data:
            print("Player not found")
            return jsonify({"success": False, "message": "Player not found in the database."}), 404
        else:
            print("Player found. Wait for the data.")
        data_player = data[player_name]
        puuid = get_puuid_by_riotid(data_player["gameName"], data_player["tagLine"], data_player["region"])["puuid"]
        matchlist = get_matchlist_by_puuid(puuid, data_player["region_matchlist"])
        if time_range == "last_week":
            ranked_data, last_day = get_ranked_info_twoweeks(matchlist, puuid, data_player)
        else:
            ranked_data = get_ranked_info(matchlist, puuid, data_player)
        return jsonify({
            "success": True,
            "ranked_data": ranked_data,
            "last_day": last_day
        })
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"success": False}), 500

from dateutil import parser

@app.route('/get-events')
@login_required
def get_events():
    service = get_calendar_service()
    
    # Fetch events from Google Calendar without time restrictions
    events_result = service.events().list(
        calendarId=calendar_id,
        singleEvents=True,
        orderBy='startTime'
    ).execute()
    
    events = events_result.get('items', [])
    events_for_frontend = []

    for event in events:
        start_time_str = event['start'].get('dateTime', event['start'].get('date'))
        end_time_str = event['end'].get('dateTime', event['end'].get('date'))

        start_time = parser.isoparse(start_time_str)
        end_time = parser.isoparse(end_time_str)
        if "Scrim" in event.get('summary', 'No Title'):
            color = "#214057"
        elif "Break" in event.get('summary', 'No Title') or "Lunch" in event.get('summary', 'No Title') or "Dinner" in event.get('summary', 'No Title'):
            color = "#1B2E3F"
        elif "Theory" in event.get('summary', 'No Title') or "Dry" in event.get('summary', 'No Title'):
            color = "#2A4E6A"
        else:
            color = "#2F5D7F"
        event_data = {
            'id': event['id'],
            'title': event.get('summary', 'No Title'),
            'start': start_time.isoformat(),  # Full date-time format
            'end': end_time.isoformat(),  # Full date-time format
            'description': event.get('description', ''),
            'color': color,  # Custom event color
        }

        events_for_frontend.append(event_data)
    return jsonify(events_for_frontend)

@app.route('/add-event', methods=['POST'])
@login_required
def add_event():
    data = request.json
    service = get_calendar_service()
    
    event = {
        'summary': data['title'],
        'start': {'dateTime': data['start'], 'timeZone': 'UTC'},
        'end': {'dateTime': data['end'], 'timeZone': 'UTC'}
    }
    created_event = service.events().insert(calendarId=calendar_id, body=event).execute()
    return jsonify({'id': created_event['id']})

@app.route('/update-event', methods=['PUT'])
@login_required
def update_event():
    data = request.json
    service = get_calendar_service()

    event = service.events().get(calendarId=calendar_id, eventId=data['id']).execute()
    event['summary'] = data['title']
    
    updated_event = service.events().update(calendarId=calendar_id, eventId=data['id'], body=event).execute()
    return jsonify({'message': 'Event updated'})

@app.route('/delete-event/<event_id>', methods=['DELETE'])
@login_required
def delete_event(event_id):
    service = get_calendar_service()
    service.events().delete(calendarId=calendar_id, eventId=event_id).execute()
    return jsonify({'message': 'Event deleted'})

@app.route('/update-event-datetime', methods=['PUT'])
@login_required
def update_event_datetime():
    data = request.json
    service = get_calendar_service()

    event = service.events().get(calendarId=calendar_id, eventId=data['id']).execute()
    event['start']['dateTime'] = data['start']
    event['end']['dateTime'] = data['end']

    updated_event = service.events().update(calendarId=calendar_id, eventId=data['id'], body=event).execute()
    return jsonify({'message': 'Event date/time updated'})

@app.route('/calendar')
@login_required
def show_calendar():
    return render_template('calendar.html', active_page="calendar", role = session['role'])

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        user = User.query.filter_by(username=username).first()
        if user and check_password_hash(user.password, password):
            session['user_id'] = user.id
            session['username'] = user.username
            session['role'] = user.role  # Store user role in session
            flash("Logged in successfully!", 'success')
            return redirect(url_for('index'))
        else:
            flash("Invalid username or password", 'danger')
    
    return render_template('login.html')


# Signup Route (public)
@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = (request.form.get('password') or '').strip()
        if not username or not password:
            flash('Please provide a username and password.', 'danger')
            return render_template('signup.html')

        existing = User.query.filter_by(username=username).first()
        if existing:
            flash('Username already exists. Choose another one.', 'danger')
            return render_template('signup.html')

        hashed = generate_password_hash(password)
        user = User(username=username, password=hashed, role='member')
        db.session.add(user)
        db.session.commit()

        session['user_id'] = user.id
        session['username'] = user.username
        session['role'] = user.role
        flash('Account created successfully!', 'success')
        return redirect(url_for('index'))

    return render_template('signup.html')


# Pricing Route (public)
@app.route('/pricing')
def pricing():
    return render_template('pricing.html', active_page='pricing')

# Logout Route
@app.route('/logout')
def logout():
    session.clear()
    flash("You have been logged out.", 'success')
    return redirect(url_for('login'))


# Protect specific routes with role-based access
@app.route('/admin')
@login_required
def admin():
    if session['role'] != 'admin':
        flash("You do not have permission to access this page.", 'danger')
        return redirect(url_for('index'))
    return render_template('admin.html', active_page="admin")


"""
@app.route('/save_strategy', methods=['POST'])
@login_required  # Protect this route
def save_strategy():
    data = request.get_json()
    map_name = data['map']
    comp_name = data['comp']
    url = data['url']
    
    save_strategy_to_file(map_name, comp_name, url)
    
    return jsonify({"success": True})

@app.route('/get_compositions')
def get_compositions():
    map_name = request.args.get('map')
    strategies = load_strategies()

    compositions = strategies.get(map_name, {}).keys()
    return jsonify({"compositions": list(compositions)})

@app.route('/remove_strategy', methods=['POST'])
def remove_strategy():
    data = request.json
    map_name = data.get('map')
    comp_name = data.get('comp')

    if not map_name or not comp_name:
        return jsonify({'success': False, 'message': 'Map and composition are required'}), 400

    strategies = load_strategies()

    if map_name in strategies and comp_name in strategies[map_name]:
        del strategies[map_name][comp_name]  # Remove the composition
        if not strategies[map_name]:  # If no compositions remain for the map, remove the map
            del strategies[map_name]
        save_strategies(strategies)
        return jsonify({'success': True, 'message': 'Strategy removed successfully'})

    return jsonify({'success': False, 'message': 'Strategy not found'}), 404
"""

@app.route('/save_strategy', methods=['POST'])
@login_required  # Protect this route
def save_strategy():
    data = request.get_json()
    map_name = data['map']
    comp_name = data['comp']
    url = data['url']
    
    save_strategy_to_file(map_name, comp_name, url)  # Save strategy to GCS
    
    return jsonify({"success": True})

@app.route('/get_compositions')
def get_compositions():
    map_name = request.args.get('map')
    strategies = load_strategies()  # Load strategies from GCS

    compositions = strategies.get(map_name, {}).keys()
    return jsonify({"compositions": list(compositions)})

@app.route('/remove_strategy', methods=['POST'])
def remove_strategy():
    """Remove a strategy based on map and composition."""
    data = request.json
    map_name = data.get('map')
    comp_name = data.get('comp')

    if not map_name or not comp_name:
        return jsonify({'success': False, 'message': 'Map and composition are required'}), 400

    strategies = load_strategies()  # Load strategies from GCS

    if map_name in strategies and comp_name in strategies[map_name]:
        del strategies[map_name][comp_name]  # Remove the composition
        if not strategies[map_name]:  # If no compositions remain for the map, remove the map
            del strategies[map_name]
        save_strategies(strategies)  # Save updated strategies back to GCS
        return jsonify({'success': True, 'message': 'Strategy removed successfully'})

    return jsonify({'success': False, 'message': 'Strategy not found'}), 404


@app.route("/search_player_autocomplete", methods=["POST"])
def search_player_autocomplete():
    query = request.form.get("query", "").lower()
    
    if not query:
        return jsonify({"success": True, "players": []})
    
    # Search for players whose names contain the query string
    matching_players = [player for player in opponents_data if query in player.lower()]

    return jsonify({"success": True, "players": matching_players})

@app.context_processor
def inject_user():
    current_user = {
        'logged_in': 'user_id' in session,
        'username': session.get('username'),
        'role': session.get('role'),
        'subscribed': session.get('subscribed', False),  # to be set after payment integration
    }
    return {'current_user': current_user}

@app.route('/upload_report', methods=['POST'])
@login_required  # Ensure the user is logged in
def upload_report():
    if session.get('role') != 'admin':  # Restrict to admins
        flash("You do not have permission to upload reports.", 'danger')
        return redirect(url_for('strategic'))

    # Get form data
    team_name = request.form.get("team_name")
    map_name = request.form.get("map_name")
    report_date_str = (request.form.get("report_date") or "").strip()  # from <input type="date">
    report_file = request.files.get("report_file")

    if not (team_name and map_name and report_file):
        return "Missing form data", 400

    # Validate file type
    if not report_file.filename.endswith('.pdf'):
        flash('Invalid file format! Only PDF files are allowed.', 'danger')
        return redirect(url_for('strategic'))

    if report_date_str:
        try:
            chosen_date = dt.datetime.strptime(report_date_str, "%Y-%m-%d").date()
        except ValueError:
            flash("Invalid report date.", "danger")
            return redirect(url_for('strategic'))
    else:
        chosen_date = dt.date.today()

    # Build name/path using the CHOSEN date
    date_str = chosen_date.strftime("%B_%d_%Y")  # e.g., August_20_2025
    year = chosen_date.year
    filename = f"{team_name}_{map_name}_{date_str}.pdf"
    destination_blob_name = f"{year}/{filename}"

    # Save the file securely
    file_url = upload_to_gcs(report_file, destination_blob_name)

    print(f"File uploaded successfully! <a href='{file_url}'>View File</a>")

    return redirect(url_for('strategic'))


USER_SECRET_KEY = "teamheretics"
@app.route('/2fa', methods=['GET', 'POST'])
def two_factor_auth():
    if 'username' not in session:
        return redirect(url_for('login'))

    if request.method == 'POST':
        totp = pyotp.TOTP(USER_SECRET_KEY)
        user_code = request.form['code']

        if totp.verify(user_code):
            session['authenticated'] = True
            return redirect(url_for('protected'))
        else:
            return "Invalid 2FA code, try again."

    return render_template('2fa.html')

if __name__ == "__main__":
    app.run(debug=True, port=int(os.environ.get("PORT", 5081)))


def role_required(roles):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user_id' not in session:
                flash("You need to log in first.", 'warning')
                return redirect(url_for('login'))
            if session.get('role') not in roles:
                flash("You do not have permission to access this page.", 'danger')
                return redirect(url_for('login'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator
