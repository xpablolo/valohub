import datetime
import json
import os
import pickle
import re
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from flask import request, session
from google.auth.transport.requests import Request
from google.cloud import storage
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from hashlib import sha256

from .config import PROJECT_ROOT, get_settings
from .riot_api import (
    get_agent_by_puuid,
    get_map_by_id,
    get_match_by_match_id,
    get_matchlist_by_puuid,
    get_minimap_by_uuid,
    get_playerlocations_by_id,
    get_playerstats_by_id,
    get_puuid_by_riotid,
    get_riotid_by_puuid,
    get_team_by_id,
    get_teamstats_by_id,
    get_teams,
    get_weapon_by_puuid,
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MAP_RANKINGS_FILE = DATA_DIR / "map_rankings.json"
settings = get_settings()
openai_key = settings.openai_key

MAP_POOL = [
    "Haven",
    "Corrode",
    "Bind",
    "Sunset",
    "Lotus",
    "Ascent",
    "Abyss",
]
_MAP_PATTERN = re.compile(r"\\b(" + "|".join(re.escape(name.lower()) for name in MAP_POOL) + r")\\b")
_MAP_CANONICAL = {name.lower(): name for name in MAP_POOL}
_MAP_ALIASES = {
    re.sub(r'[^a-z]', '', name.lower()): name
    for name in MAP_POOL
}

def get_map_by_id(id):
    maps = get_maps()["data"]
    for mapa in maps:
        if mapa["mapUrl"] == id:
            return mapa["displayName"]
        
def get_ranked_info(matchlist, puuid, data_player):
    data = {"All": {}}
    for match in matchlist["history"]:
        day = match["gameStartTime"].split("T")[0]
        if day.split("-")[0] != "2025":
            break
        if match["queueId"] == "competitive":
            match_data = get_match_by_match_id(match["matchId"], data_player["region_matchlist"])
            map = get_map_by_id(match_data["matchInfo"]["mapId"])
            if map not in data.keys():
                data[map] = {}
            for player in match_data["players"]:
                if player["puuid"] == puuid:
                    agent = get_agent_by_puuid(player["characterId"])["data"]["displayName"]
                    if agent not in data[map].keys():
                        data[map][agent] = 0
                    if agent not in data["All"].keys():
                        data["All"][agent] = 0
                    data["All"][agent] += 1
                    data[map][agent] += 1
    data = {
        outer_key: dict(sorted(inner_dict.items(), key=lambda item: item[1]))
        for outer_key, inner_dict in data.items()}
    return data

def get_ranked_info_twoweeks(matchlist, puuid, data_player):
    data = {"All": {}}
    two_weeks_ago = datetime.datetime.today() - timedelta(weeks=3)
    last_day = str(two_weeks_ago).split(" ")[0]
    for match in matchlist["history"]:
        # Parse match date and check if it's within the last 7 days
        match_date_str = match["gameStartTime"].split("T")[0]
        match_date = datetime.datetime.strptime(match_date_str, "%Y-%m-%d")
        
        if match_date_str.split("-")[0] != "2025" or match_date < two_weeks_ago:
            break  # Exit loop as matches are sorted chronologically
        
        if match["queueId"] == "competitive":
            match_data = get_match_by_match_id(match["matchId"], data_player["region_matchlist"])
            map = get_map_by_id(match_data["matchInfo"]["mapId"])
            if map not in data.keys():
                data[map] = {}
            for player in match_data["players"]:
                if player["puuid"] == puuid:
                    agent = get_agent_by_puuid(player["characterId"])["data"]["displayName"]
                    if agent not in data[map].keys():
                        data[map][agent] = 0
                    if agent not in data["All"].keys():
                        data["All"][agent] = 0
                    data["All"][agent] += 1
                    data[map][agent] += 1
    for key in data:
        data[key] = dict(sorted(data[key].items(), key=lambda item: item[1], reverse=True))
    return data, last_day

def load_strategies():
    if os.path.exists('strategies.json'):
        with open('strategies.json', 'r') as f:
            return json.load(f)
    return {}

def save_strategy_to_file(map_name, comp_name, url):
    strategies = load_strategies()
    
    if map_name not in strategies:
        strategies[map_name] = {}
    
    strategies[map_name][comp_name] = url
    
    with open('strategies.json', 'w') as f:
        json.dump(strategies, f, indent=4)

def save_strategies(strategies):
    """Save strategies to the JSON file."""
    with open("strategies.json", 'w') as file:
        json.dump(strategies, file, indent=4)

def convert_number_to_date(date_str):
    """Converts a date string into 'Month Day, Year' format."""

    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):

        dt = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        month, day, year = dt.month, dt.day, dt.year

    # Case 1: Numeric format (MMDDYYYY)
    elif date_str.isdigit() and len(date_str) == 8:
        month = int(date_str[:2])  # Extract month
        day = int(date_str[2:4])   # Extract day
        year = date_str[4:]        # Extract year

    # Case 2: Text format (January_30_2025)
    else:
        try:
            # Replace underscores with spaces and parse into datetime object
            date_obj = datetime.datetime.strptime(date_str.replace("_", " "), "%B %d %Y")
            month = date_obj.month
            day = date_obj.day
            year = date_obj.year
        except ValueError:
            print(f"Invalid date format: {date_str}")
            return date_str  # Return unchanged if parsing fails

    # Get the full month name
    month_name = datetime.date(year, month, 1).strftime('%B')

    # Determine day suffix (st, nd, rd, th)
    day_suffix = "th" if 11 <= day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")

    return f"{month_name} {day}{day_suffix}, {year}"


def generate_signed_url(bucket_name, blob_name, expiration=3600):
    """Generate a signed URL for a GCS object (valid for `expiration` seconds)."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    url = blob.generate_signed_url(
        expiration=datetime.timedelta(seconds=expiration),
        method="GET"
    )
    return url

def credentials_to_dict(credentials):
    """Converts the credentials object into a dictionary."""
    return {
        'token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'token_uri': credentials.token_uri,
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'scopes': credentials.scopes
    }


def authenticate():
    credentials = None
    # The file token.pickle stores the user's credentials and is created automatically when the authorization flow completes for the first time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            credentials = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json',  # Your credentials file from Google Cloud Console
                ['https://www.googleapis.com/auth/calendar.readonly']  # Add appropriate scopes here
            )
            credentials = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(credentials, token)

    # Save credentials in session
    session['credentials'] = credentials_to_dict(credentials)
    return credentials

def get_credentials():
    # Fetch credentials from session (or wherever you stored them)
    credentials_info = session.get('credentials')
    
    if not credentials_info:
        raise ValueError("No credentials found, user needs to authenticate first")

    # Convert the dictionary back to credentials object
    credentials = Credentials.from_authorized_user_info(credentials_info)
    
    return credentials


def help_scrape(team, events):
    team_ids = {"BBL": 397, "VIT": 2059, "NAVI": 4915, "FUT": 1184, "TH": 1001, "M8": 12694, "MKOI": 7035, "GX": 14419, "KC": 8877, "TL": 474, "FNC": 2593,
                "SEN": 2, "MIBR": 7386, "BLG": 12010, "WOL": 13790, "GEN": 17, "PRX": 624, "G2": 11058, "XLG": 13581, "RRQ": 878, "100T": 120, "NRG": 1034, 
                "EDG": 1120, "TEC": 14137, "T1": 14, "DRG": 11981, "DRX": 8185}
    url = f"https://www.vlr.gg/team/matches/{team_ids[team]}"
    matches = []
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    items = soup.find_all('a', class_='wf-card fc-flex m-item')
    expanded_events = []
    for e in events:
        if e == "2025-stage-1-all":
            expanded_events.extend(["2025-emea-stage-1", "2025-pacific-stage-1", "2025-americas-stage-1", "2025-china-stage-1"])
        elif e == "2025-stage-2-all":
            expanded_events.extend(["2025-emea-stage-2", "2025-pacific-stage-2", "2025-americas-stage-2", "2025-china-stage-2"])
        else: expanded_events.append(e)
    
    matches.extend([item['href'] for item in items if any(event in item['href'] for event in expanded_events)])
    pick_ban = {"Bind": [0,0,0,0],
                "Split": [0,0,0,0],
                "Fracture": [0,0,0,0],
                "Lotus": [0,0,0,0],
                "Haven": [0,0,0,0],
                "Abyss": [0,0,0,0],
                "Pearl": [0,0,0,0],
                "Icebox": [0,0,0,0],
                "Ascent": [0,0,0,0],
                "Sunset": [0,0,0,0],
                "Corrode": [0,0,0,0]}
    overview = []
    for match in matches:
        res = requests.get(f"https://www.vlr.gg{match}")
        soup = BeautifulSoup(res.text, 'html.parser')
        items = soup.find_all(class_="match-header-note")
        items = [item.text for item in items]
        items = items[0]
        lista = items.split(";")
        overview.append(lista)
        for i, map in enumerate(lista):
            if i < 6:
                mapa = map.split()[2]
                t = map.split()[0]
                action = map.split()[1]
                if team == t:
                    if action == "ban" and i<=1:
                        pick_ban[mapa][0] += 1
                    elif action == "pick" and i<=3:
                        pick_ban[mapa][2] += 1
                    elif action == "ban" and i>1:
                        pick_ban[mapa][1] += 1
                    elif action == "pick" and i>3:
                        pick_ban[mapa][3] += 1
    return pick_ban, overview

BUCKET_NAME = "bucket-reports1"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "api_keys/reports.json"
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)


def load_strategies():
    bucket = storage_client.get_bucket(BUCKET_NAME)
    blob = bucket.blob('strategies.json')  # Assuming you're saving strategies as a single file
    if blob.exists():
        # Download the strategies file as a string
        strategies_data = blob.download_as_text()
        return json.loads(strategies_data)  # Parse the JSON data
    return {}

def save_strategies(strategies):
    bucket = storage_client.get_bucket(BUCKET_NAME)
    blob = bucket.blob('strategies.json')  # Save strategies under this name

    # Convert strategies dict to JSON string and upload to GCS
    strategies_data = json.dumps(strategies)
    blob.upload_from_string(strategies_data, content_type='application/json')

def save_strategy_to_file(map_name, comp_name, url):
    strategies = load_strategies()  # Load existing strategies
    if map_name not in strategies:
        strategies[map_name] = {}
    
    strategies[map_name][comp_name] = url  # Save the new composition URL
    
    save_strategies(strategies)

def process_scrim_data(scrim_data):
    processed_data = []
    last_date = None
    for row in scrim_data:
        # Skip rows that are completely empty
        if not row:
            continue
        # If the date cell is not empty, update last_date
        if row[0].strip():
            last_date = row[0]
        else:
            # If the date cell is empty, set it to the last_date
            row[0] = last_date
        processed_data.append(row)
    return processed_data

def compute_win_stats(scrim_data):
    stats = {}
    for row in scrim_data:
        # Skip rows that are headers or empty
        if not row or row[0] in ['VOD LINK', "NO VOD"] or row[0] == 'DATE':
            continue

        # Assuming the row structure is: [DATE, SCRIM TYPE, OPPONENT, MAP, RESULT, ...]
        try:
            scrim_date = datetime.datetime.strptime(row[0], '%B %d, %Y').date()
        except ValueError:
            continue  # Skip rows with invalid date format

        map_name = row[3]
        result = row[4].upper()
        def_won = int(row[6])
        atk_won = int(row[7])
        def_pistol = int(row[8])
        atk_pistol = int(row[9])
        if map_name not in stats:
            stats[map_name] = {'games': 0, 'wins': 0, 'draws': 0, 'losses': 0, 'def_won': 0, "def_played": 0, 'atk_won': 0, "atk_played": 0, 'def_pistol_won': 0, 'atk_pistol_won': 0}
        stats[map_name]['games'] += 1
        if result == 'WON':
            stats[map_name]['wins'] += 1
        elif result == 'DRAW':
            stats[map_name]['draws'] += 1
        elif result == 'LOST':
            stats[map_name]['losses'] += 1
        stats[map_name]['def_won'] += def_won
        stats[map_name]['def_played'] += 12
        stats[map_name]['atk_won'] += atk_won
        stats[map_name]['atk_played'] += 12
        stats[map_name]['def_pistol_won'] += def_pistol
        stats[map_name]['atk_pistol_won'] += atk_pistol

    win_stats = []
    for map_name, data in stats.items():
        win_rate = round((data['wins'] / data['games']) * 100) if data['games'] > 0 else 0
        def_win_rate = round((data['def_won'] / data['def_played']) * 100) if data['def_played'] > 0 else 0
        atk_win_rate = round((data['atk_won'] / data['atk_played']) * 100) if data['atk_played'] > 0 else 0
        def_pistol_rate = round((data['def_pistol_won'] / data['games']) * 100) if data['games'] > 0 else 0
        atk_pistol_rate = round((data['atk_pistol_won'] / data['games']) * 100) if data['games'] > 0 else 0

        win_stats.append({
            'map': map_name,
            'games': data['games'],
            'wins': data['wins'],
            'draws': data['draws'],
            'losses': data['losses'],
            'win_rate': win_rate,
            "def_winrate": def_win_rate,
            "atk_winrate": atk_win_rate,
            "def_pistol": def_pistol_rate,
            "atk_pistol": atk_pistol_rate
        })
    return win_stats

def get_agent_winrates(scrim_data):
    columns = ['DATE', 'SCRIM TYPE', 'OPPONENT', 'MAP', 'RESULT', 'TOTAL SCORE', 
               'DEFENSE', 'ATTACK', 'DEF', 'ATK', 'ENEMY TEAM COMP']
    
    # Convert each row (list) into a dictionary.
    rows = []
    for row in scrim_data:
        # Use zip to map columns to values.
        row_dict = {col: value for col, value in zip(columns, row)}
        rows.append(row_dict)
    
    # Filter out rows that have missing MAP or RESULT, or where MAP is empty.
    filtered = [
        row for row in rows 
        if row.get('MAP') is not None and row.get('RESULT') is not None and row.get('MAP') != '' and row.get('MAP') != 'MAP'
    ]
    
    # Explode the "ENEMY TEAM COMP" column: split by ', ' and create one row per agent.
    exploded_rows = []
    for row in filtered:
        comp = row.get('ENEMY TEAM COMP', '')
        # Split the composition into a list of agents.
        agents = comp.split(', ') if comp else []
        for agent in agents:
            new_row = row.copy()
            new_row['ENEMY TEAM COMP'] = agent
            exploded_rows.append(new_row)
    
    # Group the data by (MAP, ENEMY TEAM COMP)
    groups = {}
    for row in exploded_rows:
        key = (row['MAP'], row['ENEMY TEAM COMP'])
        if key not in groups:
            groups[key] = {'total_games': 0, 'wins': 0}
        groups[key]['total_games'] += 1
        if row['RESULT'] == 'WON':
            groups[key]['wins'] += 1
    
    # Build a list of dictionaries with the computed win rates.
    results = []
    for (map_name, agent), counts in groups.items():
        total = counts['total_games']
        wins = counts['wins']
        win_rate = (wins / total) * 100 if total > 0 else 0
        results.append({
            'MAP': map_name,
            'ENEMY TEAM COMP': agent,
            'total_games': total,
            'wins': wins,
            'win_rate': win_rate
        })
    
    # Sort results: first by MAP (alphabetically) then by win_rate in ascending order.
    results = sorted(results, key=lambda x: (x['MAP'], x['win_rate']))
    
    return results

def get_scrim_teams(scrim_data):
    teams = defaultdict(list)
    for row in scrim_data:
        if not row or row[0] in ['VOD LINK', "NO VOD"] or row[0] == 'DATE':
            continue
        try:
            scrim_date = datetime.datetime.strptime(row[0], '%B %d, %Y').date()
        except ValueError:
            continue  # Skip rows with invalid date format

        team = row[2]
        if team == "KarmineCorp" or team == "Karmine Corp":
            team = "KC"
        if team not in ["KOI", "KC", "BBL", "GIANTX", "GentleMates", "FUT", "Liquid"]:
            continue

        map_name = row[3]
        result = row[4].upper()
        comp = row[10].split(", ")
        comp.sort()

        if team not in teams:
            teams[team] = {'maps': {}}
        if map_name not in teams[team]['maps']:
            teams[team]['maps'][map_name] = {"comps": {}}
        if tuple(comp) not in teams[team]['maps'][map_name]["comps"]:
            teams[team]['maps'][map_name]["comps"][tuple(comp)] = {"games": 0, "wins": 0, "dates": []}
        teams[team]['maps'][map_name]["comps"][tuple(comp)]["games"] += 1
        teams[team]['maps'][map_name]["comps"][tuple(comp)]["dates"].append(row[0])
        if result == 'WON':
            teams[team]['maps'][map_name]["comps"][tuple(comp)]["wins"] += 1
    return teams


def compute_head_to_head_summary(scrim_data):
    """Aggregate head-to-head results by opponent (and per-map breakdown).
    Expects rows shaped like:
    [DATE, SCRIM TYPE, OPPONENT, MAP, RESULT, TOTAL SCORE, DEFENSE, ATTACK, DEF PISTOL, ATK PISTOL, ENEMY TEAM COMP]
    Returns a list of dicts:
      {
        'opponent': str,
        'games': int,
        'wins': int,
        'wr': int,  # percentage
        'last_played': 'Month D, YYYY',
        'last_sort': 'YYYY-MM-DD',
        'maps': [ {'map': str, 'games': int, 'wins': int, 'wr': int}, ... ]
      }
    """
    by_opp = {}
    for row in scrim_data:
        if not row or row[0] in ['VOD LINK', 'DATE']:
            continue
        try:
            d = datetime.datetime.strptime(row[0], '%B %d, %Y').date()
        except Exception:
            continue

        opponent = (row[2] or '').strip()
        if opponent in ("KarmineCorp", "Karmine Corp"):
            opponent = "KC"
        mapa = (row[3] or '').strip()
        result = (row[4] or '').strip().upper()

        if not opponent or not mapa:
            continue

        if opponent not in by_opp:
            by_opp[opponent] = {
                'games': 0,
                'wins': 0,
                'last_dt': d,
                'maps': {},
                'events': []  # list of {'dt': date, 'map': str, 'comp': str}
            }
        rec = by_opp[opponent]
        rec['games'] += 1
        if result == 'WON':
            rec['wins'] += 1
        if d > rec['last_dt']:
            rec['last_dt'] = d

        if mapa not in rec['maps']:
            rec['maps'][mapa] = {'games': 0, 'wins': 0}
        rec['maps'][mapa]['games'] += 1
        if result == 'WON':
            rec['maps'][mapa]['wins'] += 1

        # capture composition and date for detailed listing
        comp = (row[10] if len(row) > 10 else '') or ''
        rec['events'].append({'dt': d, 'map': mapa, 'comp': comp})

    summary = []
    for opp, rec in by_opp.items():
        games = rec['games']
        wins = rec['wins']
        wr = round((wins / games) * 100) if games else 0
        last_dt = rec['last_dt']
        maps_list = []
        for m, md in rec['maps'].items():
            mg = md['games']
            mw = md['wins']
            mwr = round((mw / mg) * 100) if mg else 0
            maps_list.append({'map': m, 'games': mg, 'wins': mw, 'wr': mwr})
        maps_list.sort(key=lambda x: (-x['games'], x['map']))
        # sort events by date desc and format
        evs = sorted(rec['events'], key=lambda e: e['dt'], reverse=True)
        events_fmt = [
            {
                'date': e['dt'].strftime('%B %d, %Y'),
                'date_sort': e['dt'].isoformat(),
                'map': e['map'],
                'comp': e['comp']
            }
            for e in evs
        ]

        summary.append({
            'opponent': opp,
            'games': games,
            'wins': wins,
            'wr': wr,
            'last_played': last_dt.strftime('%B %d, %Y'),
            'last_sort': last_dt.isoformat(),
            'maps': maps_list,
            'events': events_fmt
        })

    summary.sort(key=lambda x: (-x['games'], x['opponent']))
    return summary


def load_map_rankings():
    """Return stored map rankings sorted by creation timestamp ascending."""
    if not MAP_RANKINGS_FILE.exists():
        return []
    try:
        with MAP_RANKINGS_FILE.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
            if isinstance(data, list):
                return data
    except (json.JSONDecodeError, OSError):
        pass
    return []


def save_map_rankings(rankings):
    """Persist map rankings payload."""
    MAP_RANKINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with MAP_RANKINGS_FILE.open("w", encoding="utf-8") as handle:
        json.dump(rankings, handle, indent=2)


def parse_map_ranking(raw_text, map_pool=MAP_POOL):
    """Parse pasted text into a (order, found_any) tuple."""
    if not raw_text:
        return [], False

    text = raw_text.lower()
    matches = []
    for match in _MAP_PATTERN.finditer(text):
        canonical = _MAP_CANONICAL.get(match.group(1))
        if canonical:
            matches.append((match.start(), canonical))

    matches.sort(key=lambda item: item[0])
    ordered = []
    seen = set()
    for _, map_name in matches:
        if map_name not in seen:
            ordered.append(map_name)
            seen.add(map_name)

    found_any = bool(matches)

    if not ordered:
        for line in raw_text.splitlines():
            alias_key = re.sub(r'[^a-z]', '', line.lower())
            canonical = _MAP_ALIASES.get(alias_key)
            if canonical and canonical not in seen:
                ordered.append(canonical)
                seen.add(canonical)
        if ordered:
            found_any = True

    if not ordered:
        for token in re.split(r'[^a-z]+', text):
            if not token:
                continue
            canonical = _MAP_CANONICAL.get(token)
            if canonical and canonical not in seen:
                ordered.append(canonical)
                seen.add(canonical)
        if ordered:
            found_any = True

    # Append any maps that were not mentioned to preserve a full ordering.
    for map_name in map_pool:
        if map_name not in seen:
            ordered.append(map_name)
            seen.add(map_name)

    return ordered, found_any


def compute_average_map_ranking(player_rankings, map_pool=MAP_POOL):
    """Compute average placement per map given player orderings."""
    scores = {map_name: [] for map_name in map_pool}
    for order in player_rankings.values():
        for index, map_name in enumerate(order, start=1):
            if map_name in scores:
                scores[map_name].append(index)

    averages = {}
    for map_name, placements in scores.items():
        averages[map_name] = sum(placements) / len(placements) if placements else None

    sorted_maps = sorted(
        map_pool,
        key=lambda name: (averages[name] if averages[name] is not None else float("inf"), name)
    )

    return averages, sorted_maps


def order_to_positions(order):
    """Return a map -> placement mapping for a given ordering."""
    return {name: idx + 1 for idx, name in enumerate(order)}


from openai import OpenAI
client = OpenAI(api_key=openai_key)

def ai_prompt(overview, team, weight):
    matches_text = []
    for idx, match in enumerate(overview, start=1):
        # Strip whitespace and join the actions
        actions = "; ".join(item.strip() for item in match)
        matches_text.append(f"Match {idx}: {actions}")
    # build a simple prompt from your table data
    if weight == "default":
        addition = "Recent matches significantly carry much more influence than older ones. Weight the first ones higher. \n"
    elif weight == "high":
        addition = "Please have a very strong weight in favor of the most recent matches and focus your analysis predominantly on the most recent matches. Barely analyze the last ones. \n"
    elif weight == "low":
        addition = "Treat all matches equally, letting each match contribute similar weight. \n"
    prompt = (
        f"You are a Valorant analyst. Today you are evaluating the team “{team}”.\n\n"
        "I want you to rank their mappool, indicating on which maps they are stronger and weaker.\n"
        "Below are their pick & ban sequences for each match, listed from most recent to oldest:\n"
        + "\n".join(matches_text)
        + "\n\n"
        "When computing map strength:\n"
        f"  - If {team} picks a map, they are strong on it (add positive weight).\n"
        f"  - If {team} bans a map, they fear their own weakness (add negative weight).\n"
        f"  - If the opponent picks a map, {team} is likely weaker there (add negative weight).\n"
        f"  - If the opponent bans a map, they fear {team}'s strength there (add positive weight).\n"
        f"   - Remember that the picks and bans done by {team} contain always way more information than the picks or bans done by the other team, "
        "     in particular give higher importance to first pick/bans, and analyze the latter decisions depending on the picks/bans done by the other team\n"
        f"{addition}"
        "  - Use the full pick/ban sequence and the order they occurred—early bans/picks matter most.\n"
        "  - **Important**: the official map pool can change over time. "
        "The score of a map should only be computed regarding the matches where the map was in the pool, if not in the pool, then the score dosnt't change for that match.\n\n"
        "Return *only* a JSON array of [mapName,score] pairs (score between 0 and 1),\n"
        "sorted from strongest to weakest. No extra text or formatting.\n"
        "Example:\n"
        '[["Haven", 0.85], ["Split", 0.60], ["Ascent", 0.45]]'
    )
    return prompt
        
