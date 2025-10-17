import json
import requests
import time
import datetime
from datetime import timedelta

with open("api_keys/settings.json", "r") as file:
    file = json.load(file)
    api = file["riot_api_key"]
    valolytics_api = file["valolytics_key"]

def get_match_by_match_id(match_id: str, region: str):
    url = f"https://api.valolytics.gg/api/matches/{region}/{match_id}"
    response = requests.get(url, headers={"user-agent": "mozilla/5.0", "x-api-key": valolytics_api})
    match = response.json()
    return match

def get_puuid_by_riotid(gameName: str, tagLine: str, region: str):
    url = f"https://api.valolytics.gg/api/riot/account/v1/accounts/by-riot-id/{region}/{gameName}/{tagLine}"
    response = requests.get(url, headers={"user-agent": "mozilla/5.0", "x-api-key": valolytics_api})
    player_id = response.json()
    return player_id

def get_matchlist_by_puuid(puuid: str, region: str):
    url = f"https://api.valolytics.gg/api/riot/match/v1/matchlists/by-puuid/{region}/{puuid}"
    response = requests.get(url, headers={"user-agent": "mozilla/5.0", "x-api-key": valolytics_api})
    matchlist = response.json()
    return matchlist

def get_riotid_by_puuid(puuid: str, region: str):
    url = f"https://api.valolytics.gg/api/riot/account/v1/accounts/by-puuid/{region}/{puuid}"
    response = requests.get(url, headers={"user-agent": "mozilla/5.0", "x-api-key": valolytics_api})
    riotid = response.json()
    return riotid

def get_playerlocations_by_id(id:str, region:str):
    url = f"https://api.valolytics.gg/api/stats/playerlocations/{region}/{id}"
    response = requests.get(url, headers={"user-agent": "mozilla/5.0", "x-api-key": valolytics_api})
    playerlocations = response.json()
    return playerlocations

def get_playerstats_by_id(id:str, region:str):
    url = f"https://api.valolytics.gg/api/stats/playerstats/{region}/{id}"
    response = requests.post(url, headers={"user-agent": "mozilla/5.0", "x-api-key": valolytics_api})
    stats = response.json()
    return stats

def get_teamstats_by_id(id, region:str):
    url = f"https://api.valolytics.gg/api/stats/teamstats/{region}/{id}"
    response = requests.post(url, headers={"user-agent": "mozilla/5.0", "x-api-key": valolytics_api})
    stats = response.json()
    return stats

def get_minimap_by_uuid(uuid:str):
    url = f"https://api.valolytics.gg/api/stats/minimap/{uuid}"
    minimap = requests.post(url, headers={"user-agent": "mozilla/5.0", "x-api-key": valolytics_api})
    return minimap

def get_teams():
    url = "https://api.valolytics.gg/teams"
    response = requests.get(url, headers={"user-agent": "mozilla/5.0", "x-api-key": valolytics_api})
    teams = response.json()
    return teams

def get_team_by_id(id:str):
    url = f"https://api.valolytics.gg/teams/{id}"
    response = requests.get(url, headers={"user-agent": "mozilla/5.0", "x-api-key": valolytics_api})
    team = response.json()
    return team

def get_agent_by_puuid(puuid: str):
    url = f"https://valorant-api.com/v1/agents/{puuid}"
    response = requests.get(url, headers={"user-agent": "mozilla/5.0"})
    agent = response.json()
    return agent

def get_weapon_by_puuid(puuid: str):
    url = f"https://valorant-api.com/v1/weapons/{puuid}"
    response = requests.get(url, headers={"user-agent": "mozilla/5.0"})
    weapon = response.json()
    return weapon

def get_maps():
    url = "https://valorant-api.com/v1/maps"
    response = requests.get(url, headers={"user-agent": "mozilla/5.0"})
    weapon = response.json()
    return weapon

def get_map_by_id(id):
    maps = get_maps()["data"]
    for mapa in maps:
        if mapa["mapUrl"] == id:
            return mapa["displayName"]

two_weeks_ago = datetime.datetime.today() - timedelta(weeks=2)
players = {"benjy": 'vhTtIAHoN9juR-MTHWhQsRmSipbTmHdZxh-GKsnLI6qOElJiy9Lc5rWF8DIItfqjP9aJUjee6nYqaw',
            "Boo": '1MW2jdnkqSucdnkC0CeAqKxeizYBhaD6D2sirg6I3ZgaLLn4ZFAkvtP_cNWlE7aqfcJRZ8RK7rDRXg',
            "MiniBoo": '3kz0L8V5QVwpQt5hBaaV3i-F4dThz8BZsSOPCBPFYY0iH16aKvwyDHfyKgWMsBm9FO4992oSV5YBog',
            "wo0t": 'vBa51oohgZuj84ltdd2AJobhugPTKR7Uslwk7Cdlrl3oaGEW3L8ZRdG4-YNosa9G3Q-T8Ft5s0R_vQ',
            "RieNs": 'zkJA6-Zw_2s8Iaz5iouDA-22DC1j7S8kE4D4JG3rRxXyd6ibkrAD__0mNmucghI7RLN1CSh8nSfRuA'}

STATIC_FILES = {
    "benjy": "Benjyfishy.json",
    "Boo": "Boo.json",
    "MiniBoo": "MiniBoo.json",
    "wo0t": "Wo0t.json",
    "RieNs": "Riens.json",
}

# Load static data for each player
def load_static_data():
    static_data = {}
    for player, filepath in STATIC_FILES.items():
        try:
            with open(filepath, "r") as f:
                raw = json.load(f)
        except FileNotFoundError:
            static_data[player] = []
            continue
        # Determine if raw is wrapped in a dict under the player key
        if isinstance(raw, dict) and player in raw and isinstance(raw[player], list):
            entries = raw[player]
        # Or if it's a single-key dict whose value is a list
        elif isinstance(raw, dict) and len(raw) == 1:
            sole_list = next(iter(raw.values()))
            entries = sole_list if isinstance(sole_list, list) else []
        # Or if it's already a list
        elif isinstance(raw, list):
            entries = raw
        else:
            entries = []
        static_data[player] = entries
    return static_data

def save_static_data(static_data):
    for player, entries in static_data.items():
        filepath = STATIC_FILES.get(player)
        if not filepath:
            continue
        # Wrap back as a dict keyed by player for consistency
        out = {player: entries}
        with open(filepath, "w") as f:
            json.dump(out, f, indent=4)


def get_players_data(start_date: datetime.date, end_date: datetime.date, static_cutoff: datetime.date = datetime.date(2025, 6, 25)):
    data = {player: {} for player in players}
    static_data = load_static_data()

    for player, entries in static_data.items():
        if player not in data:
            continue
        for entry in entries:
            day_str = entry.get("fecha")
            try:
                match_date = datetime.datetime.strptime(day_str, "%Y-%m-%d").date()
            except Exception:
                continue
            if not (start_date <= match_date <= end_date):
                continue
            if match_date <= static_cutoff:
                data[player][day_str] = {
                    "competitive": entry.get("partidas_competitivas", 0),
                    "deathmatch": entry.get("dms_jugados", 0),
                    "hurm": entry.get("team_deathmatch", 0),
                }

    for player, puuid in players.items():
        matchlist = get_matchlist_by_puuid(puuid, "eu")
        new_entries = []
        for match in matchlist.get("history", []):
            date_str = match.get("gameStartTime", "").split("T")[0]
            try:
                match_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            except Exception:
                continue
            # Skip out-of-range or already-covered dates
            if match_date < start_date or match_date > end_date or match_date <= static_cutoff:
                continue
            q = match.get("queueId")
            if q not in ("competitive", "deathmatch", "hurm"):
                continue
            # Initialize day counts if needed
            if date_str not in data[player]:
                data[player][date_str] = {"competitive": 0, "deathmatch": 0, "hurm": 0}
            data[player][date_str][q] += 1
            existing = {e.get("fecha") for e in static_data.get(player, []) if isinstance(e, dict)}
            for day, counts in sorted(data[player].items()):
                d = datetime.datetime.strptime(day, "%Y-%m-%d").date()
                if d > static_cutoff and day not in existing:
                    new_entries.append({
                        "fecha": day,
                        "dms_jugados": counts.get("hurm", 0),
                        "partidas_competitivas": counts.get("competitive", 0),
                        "team_deathmatch": counts.get("deathmatch", 0),
                    })
            # append new entries
            static_data[player].extend(new_entries)

    # Save back static files
    save_static_data(static_data)

    with open("rankeds.json", "w") as f:
        json.dump(data, f, indent=4)
    return data

def regenerate_kda():
    data = {p: {"all": [0, 0, 0]} for p in players}
    two_weeks_ago = datetime.datetime.today() - timedelta(weeks=2)

    for player in players:
        print(player)
        matchlist = get_matchlist_by_puuid(players[player], "eu")
        for match in matchlist["history"]:
            day_str = match["gameStartTime"].split("T")[0]
            match_date = datetime.datetime.strptime(day_str, "%Y-%m-%d")
            if match_date.year != 2025 or match_date < two_weeks_ago:
                break
            if match["queueId"] in ["hurm", "ggteam", "swiftplay", "deathmatch"]:
                continue
            time.sleep(1)
            match_data = get_match_by_match_id(match["matchId"], "eu")
            for player_i in match_data["players"]:
                if player_i["puuid"] == players[player]:
                    data[player]["all"][0] += player_i["stats"]["kills"]
                    data[player]["all"][1] += player_i["stats"]["deaths"]
                    data[player]["all"][2] += player_i["stats"]["vlrRating2"]
    with open("rankeds_kda.json", "w") as f:
        json.dump(data, f, indent=4)
    return data

def get_other_players_data(start_date: datetime.date, end_date: datetime.date, players):
    data = {player: {} for player in players}
    for player, puuid in players.items():
        try:
            matchlist = get_matchlist_by_puuid(puuid, "eu")
            for match in matchlist["history"]:
                date_str = match["gameStartTime"].split("T")[0]
                match_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                if match_date < start_date or match_date > end_date:
                    continue
                q = match["queueId"]
                if q not in ("competitive", "deathmatch", "hurm"):
                    continue
                day = date_str
                if day not in data[player]:
                    data[player][day] = {"competitive": 0, "deathmatch": 0, "hurm": 0}
                data[player][day][q] += 1
        except:
            continue

    with open("rankeds_other.json", "w") as f:
        json.dump(data, f, indent=4)
    return data