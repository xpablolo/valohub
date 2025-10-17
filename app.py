from flask import Flask, redirect, url_for, session, request, render_template, jsonify, Response, stream_with_context
from flask import session, flash, redirect
from flask_sqlalchemy import SQLAlchemy
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
import os
import json
import datetime as dt
from datetime import datetime, date
from google.oauth2.credentials import Credentials
from functions.help_functions import *
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from werkzeug.utils import secure_filename
import pyotp
from google.cloud import storage
from google.oauth2 import service_account
import calendar
import uuid
from functions import PROJECT_ROOT

app = Flask(__name__)
app.secret_key = 'teamheretics'

BUCKET_NAME = "bucket-reports1"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "api_keys/reports.json"
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

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
    expiration = datetime.timedelta(hours=1)
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
    open_routes = ['login', 'logout', 'pick_bans', 'static', "home"]  # endpoints that stay open
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



# Home route
@app.route('/')
@login_required  # Protect this page
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
                signed_url = blob.generate_signed_url(expiration=datetime.timedelta(hours=1), method="GET")
                
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



@app.route('/analytical_reports')
@login_required  # Protect this page
def analytical():
    return render_template('ana_reports.html', active_page="analytical")
    
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
    return {'user': {'is_admin': True}}  # Change this for non-admin users

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

if __name__ == '__main__':
    app.run(debug=True, port=int(os.environ.get('PORT', 5078)))


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
