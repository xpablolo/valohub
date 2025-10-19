import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment
from openpyxl.styles import Color, PatternFill, Font, Border
from openpyxl.styles import colors
from openpyxl.cell import Cell
import seaborn as sns
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import math
import numpy as np
import joypy
import random
from matplotlib import cm
from collections import defaultdict
from googleapiclient.http import MediaFileUpload
from googleapiclient.discovery import build
import matplotlib.image as mpimg
from .riot_api import (
    get_match_by_match_id,
    get_puuid_by_riotid,
    get_matchlist_by_puuid,
    get_riotid_by_puuid,
    get_playerlocations_by_id,
    get_playerstats_by_id,
    get_teamstats_by_id,
    get_minimap_by_uuid,
    get_teams,
    get_team_by_id,
    get_agent_by_puuid,
    get_weapon_by_puuid,
    get_maps,
    get_map_by_id,
)


#GET BASIC INFO
team = "TH"
map = "Abyss"
list_ids = ['717b9896-c047-46b1-a0ab-2616e0527ae6', '44a6cd68-cba6-4e90-9c08-201f2415fd4f', 'a34258b5-5eae-4370-8473-82284204de90', '0989a8c1-1b07-4e32-8374-27094421eb2b', '89b3e04c-8561-43c7-809c-d0a500cb2992']

def get_basic_info(team_name, map, data_matches):
    info = {"team": team_name, "matches": {}, "players": {}}
    for i, match_id in enumerate(data_matches):
        data = data_matches[match_id]
        try:
            if len(data["roundResults"]) < 13:
                print("Invalid match_id", i)
                break
            match_map = get_map_by_id(data["matchInfo"]["mapId"])
            if map != "all" and match_map != map:
                continue
            for player in data["players"]:
                if player["teamId"] != "Neutral" and team_name in player["gameName"].split()[0]:
                    info["players"][player["puuid"]] = player["gameName"]
                if player["teamId"] != "Neutral" and team_name not in player["gameName"].split()[0]:
                    rival_team = player["gameName"].split()[0]
                    rival_color = player["teamId"]
        except:
            print("match_id incorrect", i, match_id)
            continue
                
        
        for team in data["teams"]:
            if team["teamId"] == rival_color:
                rival_won = team["roundsWon"]
            else:
                team_won = team["roundsWon"]
        if rival_won < team_won:
            result = "Win"
        else:
            result = "Loss"
        team_atk, total_def, total_atk, team_def = 0,0,0,0
        for round in data["roundResults"]:
            if round["roundNum"] < 12 or (round["roundNum"] >= 24 and round["roundNum"]%2 == 0):
                if rival_color == "Red":
                    if round["winningTeam"] == "Blue":
                        team_def += 1
                    total_def += 1
                else:
                    if round["winningTeam"] == "Red":
                        team_atk += 1
                    total_atk += 1
            elif 24>round["roundNum"]>= 12 or (round["roundNum"] >= 24 and round["roundNum"]%2 == 1):
                if rival_color == "Red":
                    if round["winningTeam"] == "Blue":
                        team_atk += 1
                    total_atk += 1
                else:
                    if round["winningTeam"] == "Red":
                        team_def += 1
                    total_def += 1
        color = "Blue" if rival_color == "Red" else "Red"
        info["matches"][match_id] = {"rival": rival_team, "result": [result, (team_won, rival_won), (team_def, team_atk), (total_def, total_atk)], "map": match_map, "match_id": match_id, "color": color}

    return info

def get_comps(team, list_ids):
    compositions_count = defaultdict(list)
    # Iterate through the list of match ids
    for match_id in list_ids:
        data = get_playerstats_by_id(match_id, "esports")  # Fetch data for each match
        composition = []
        player_agent_mapping = {}

        # Iterate over each player in the match data
        for player in data:
            if team in data[player]["gameName"]:  # Check if the player is on the team we care about
                key = list(data[player]["map"].keys())[0]  # Get the map key (assuming 1 map per match)
                key2 = list(data[player]["map"][key]["agent"].keys())[0]  # Get the agent key
                agent = data[player]["map"][key]["agent"][key2]["agent"]  # Get the agent name
                player_name = data[player]["gameName"]  # Store the player's name

                player_agent_mapping[player_name] = agent  # Map player to agent
                composition.append(agent)  # Add agent to composition list
                win = data[player]["side"]["Total"]["wins"]
        # Ensure the composition has exactly 5 agents (one per player)
        if len(composition) == 5:
            composition_tuple = tuple(sorted(composition))  # Sort to ensure consistency in key
            compositions_count[composition_tuple].append({
                    "player_agent_mapping": player_agent_mapping,
                    "win": win
                })  

    return compositions_count


def get_image_link(name, url, creds):
    drive_service = build('drive', 'v3', credentials=creds)
    file_metadata = {'name': name}
    media = MediaFileUpload(url, mimetype='image/png')
    uploaded_file = drive_service.files().create(
        body=file_metadata, 
        media_body=media, 
        fields='id'
    ).execute()
    file_id = uploaded_file.get('id')
    permission = {
        'type': 'anyone',
        'role': 'reader',
    }
    drive_service.permissions().create(
        fileId=file_id,
        body=permission
    ).execute()
    return file_id

def get_plants(data_matches, basic_info):
    global_performance = {"plants": {}, "won_pp": {}, "opp_plants": {}, "won_retakes": {}}
    for match in data_matches:
        performance = {"plants": {}, "won_pp": {}, "opp_plants": {}, "won_retakes": {}}
        for round in data_matches[match]["roundResults"]:
            if round["bombPlanter"] in basic_info["players"].keys():
                if round["plantSite"] not in performance["plants"].keys():
                    performance["plants"][round["plantSite"]] = 0
                performance["plants"][round["plantSite"]] += 1
                if round["winningTeam"] == basic_info["matches"][match]["color"]:
                    if round["plantSite"] not in performance["won_pp"].keys():
                        performance["won_pp"][round["plantSite"]] = 0
                    performance["won_pp"][round["plantSite"]] += 1
            elif round["bombPlanter"] != None:
                if round["plantSite"] not in performance["opp_plants"].keys():
                    performance["opp_plants"][round["plantSite"]] = 0
                performance["opp_plants"][round["plantSite"]] += 1
                if round["winningTeam"] == basic_info["matches"][match]["color"]:
                    if round["plantSite"] not in performance["won_retakes"].keys():
                        performance["won_retakes"][round["plantSite"]] = 0
                    performance["won_retakes"][round["plantSite"]] += 1
        for key in performance.keys():
            for sub_key, value in performance[key].items():
                if sub_key not in global_performance[key]:
                    global_performance[key][sub_key] = 0
                global_performance[key][sub_key] += value
    table = [[]]
    table[0].append("All")
    num_plants = sum(global_performance["plants"].values())
    table[0].append(num_plants)
    num_won_plants = sum(global_performance["won_pp"].values())
    if num_plants == 0:
        table[0].append("-")
    else:
        table[0].append(f"{custom_round(100*num_won_plants/num_plants)}% ({num_won_plants}/{num_plants})")
    num_oppplants = sum(global_performance["opp_plants"].values())
    table[0].append(num_oppplants)
    num_won_oppplants = sum(global_performance["won_retakes"].values())
    table[0].append(f"{custom_round(100*num_won_oppplants/num_oppplants)}% ({num_won_oppplants}/{num_oppplants})")
    for i, site in enumerate(global_performance["plants"]):
        i = i+1
        table.append([])
        table[i].append(site)
        table[i].append("{} ({}%)".format(global_performance["plants"][site], custom_round(100*global_performance["plants"][site]/num_plants)))
        if site not in global_performance["won_pp"].keys():
            global_performance["won_pp"][site] = 0
        table[i].append("{}% ({}/{})".format(custom_round(100*global_performance["won_pp"][site]/global_performance["plants"][site]), global_performance["won_pp"][site], global_performance["plants"][site]))
        if site not in global_performance["opp_plants"].keys():
            global_performance["opp_plants"][site] = 0
        table[i].append("{} ({}%)".format(global_performance["opp_plants"][site], custom_round(100*global_performance["opp_plants"][site]/num_oppplants)))
        if site not in global_performance["won_retakes"].keys():
            global_performance["won_retakes"][site] = 0
        if global_performance["opp_plants"][site] == 0:
            table[i].append("-")
        else:
            table[i].append("{}% ({}/{})".format(custom_round(100*global_performance["won_retakes"][site]/global_performance["opp_plants"][site]), global_performance["won_retakes"][site], global_performance["opp_plants"][site]))

    return table

def custom_round(num):
    return int(num) if num == int(num) else round(num, 1)

def create_early_positioning(map_name, side, time, list_ids, data_matches, basic_info, path):
    points = []
    time_mil = time * 1000
    for match_id in list_ids:
        color = basic_info["matches"][match_id]["color"]
        data = data_matches[match_id]
        for round in data["roundResults"]:
            if (side == "def" and ((color == "Blue" and round["roundNum"]<12) or (color=="Blue" and round["roundNum"]>23 and round["roundNum"]%2 == 0) or (color == "Red" and round["roundNum"]>11) or (color=="Red" and round["roundNum"]>23 and round["roundNum"]%1 == 0))) or (side == "atk" and ((color == "Red" and round["roundNum"]<12) or (color=="Red" and round["roundNum"]>23 and round["roundNum"]%2 == 0) or (color == "Blue" and round["roundNum"]>11) or (color=="Blue" and round["roundNum"]>23 and round["roundNum"]%1 == 0))):
                kills = []
                for player in round["playerStats"]:
                    for kill in player["kills"]:
                        kills.append((kill["timeSinceRoundStartMillis"], kill))
                kills.sort(key=lambda x: x[0])
                if kills[0][0] < time_mil:
                    kill_list = {}
                    kill = kills[0][1]
                    if kill["victim"] in basic_info["players"].keys():
                        kill_list["victim"] = {"id": kill["victim"], "loc": kill["victimLocation"]}
                    else:
                        kill["victim"] = None
                    for pos in kill["playerLocations"]:
                        if pos["puuid"] in basic_info["players"].keys():
                            kill_list[pos["puuid"]] = pos["location"]
                    points.append(kill_list)
    params = {
    "ascent": {
        "xMultiplier": 0.00007,
        "yMultiplier": -0.00007,
        "xScalarToAdd": 0.813895,
        "yScalarToAdd": 0.573242,
    },
    "split": {
        "xMultiplier": 0.000078,
        "yMultiplier": -0.000078,
        "xScalarToAdd": 0.842188,
        "yScalarToAdd": 0.697578,
    },
    "fracture": {
        "xMultiplier": 0.000078,
        "yMultiplier": -0.000078,
        "xScalarToAdd": 0.556952,
        "yScalarToAdd": 1.155886,
    },
    "bind": {
        "xMultiplier": 0.000059,
        "yMultiplier": -0.000059,
        "xScalarToAdd": 0.576941,
        "yScalarToAdd": 0.967566,
    },
    "breeze": {
        "xMultiplier": 0.00007,
        "yMultiplier": -0.00007,
        "xScalarToAdd": 0.465123,
        "yScalarToAdd": 0.833078,
    },
    "abyss": {
        "xMultiplier": 0.000081,
        "yMultiplier": -0.000081,
        "xScalarToAdd": 0.5,
        "yScalarToAdd": 0.5,
    },
    "lotus": {
        "xMultiplier": 0.000072,
        "yMultiplier": -0.000072,
        "xScalarToAdd": 0.454789,
        "yScalarToAdd": 0.917752,
    },
    "sunset": {
        "xMultiplier": 0.000078,
        "yMultiplier": -0.000078,
        "xScalarToAdd": 0.5,
        "yScalarToAdd": 0.515625,
    },
    "pearl": {
        "xMultiplier": 0.000078,
        "yMultiplier": -0.000078,
        "xScalarToAdd": 0.480469,
        "yScalarToAdd": 0.916016,
    },
    "icebox": {
        "xMultiplier": 0.000072,
        "yMultiplier": -0.000072,
        "xScalarToAdd": 0.460214,
        "yScalarToAdd": 0.304687,
    },
    "haven": {
        "xMultiplier": 0.000075,
        "yMultiplier": -0.000075,
        "xScalarToAdd": 1.09345,
        "yScalarToAdd": 0.642728,
    },
    "corrode":{
        "xMultiplier": 0.000075,
        "yMultiplier": -0.000075,
        "xScalarToAdd":0.526158,
        "yScalarToAdd":0.5
    }
}
    sns.set_theme(style="white")
    map_name = map_name.lower()
    map_image_path = f"maps/{map_name}.png"
    map_img = mpimg.imread(map_image_path)
    if map_name in ["icebox", "sunset", "ascent", "pearl", "corrode", "bind"]:
        map_img = np.flipud(map_img)

    unique_players = [basic_info["players"][id].split()[1] for id in basic_info["players"]]
    fixed_colors = ["red", "skyblue", "yellow", "black", "blue"]
    player_colors = {player: fixed_colors[i % len(fixed_colors)] for i, player in enumerate(sorted(unique_players))}

    fig, ax = plt.subplots(figsize=(10, 10))
    ax.imshow(map_img, extent=[0, 1, 0, 1], origin='upper')
    for point in points:
        for player_id, loc in point.items():
            if player_id != "victim":  # Skip the victim marker
                name = basic_info["players"][player_id].split()[1]
                if map_name in ["haven", "split", "lotus", "icebox", "sunset", "ascent", "pearl", "corrode", "bind"]:
                    x, y = (loc["y"] * params[map_name]["xMultiplier"] + params[map_name]["xScalarToAdd"], loc["x"] * params[map_name]["yMultiplier"] + params[map_name]["yScalarToAdd"])
                else:
                    x, y = (loc["x"] * params[map_name]["xMultiplier"] + params[map_name]["xScalarToAdd"], loc["y"] * params[map_name]["yMultiplier"] + params[map_name]["yScalarToAdd"])
                ax.scatter(x,y, color= player_colors[name], label=name, alpha=1, s=40)

    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ax.legend(by_label.values(), by_label.keys(), title="Players", loc="upper left", bbox_to_anchor=(1, .5), fontsize=12)


    ax.tick_params(left=False, bottom=False)
    sns.despine(left=True, bottom=True)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    if side == "def":
        ax.set_title(f"Defending Team Positioning at the first kill (<{time}s)", fontsize=16, weight="bold")
    elif side == "atk":
        ax.set_title(f"Attacking Team Positioning at the first kill (<{time}s)", fontsize=16, weight="bold")
    plt.tight_layout()
    ax.axis('off')
    if map_name in ["haven", "split", "lotus", "icebox", "sunset", "ascent", "pearl", "corrode", "bind"]:
        ax.invert_yaxis()
    plt.savefig(path, bbox_inches="tight", dpi=600)
    plt.close('all')


def get_pistol_plants(data_matches, basic_info):
    global_performance = {"plants": {}, "won_pp": {}, "opp_plants": {}, "won_retakes": {}}
    for match_id in data_matches:
        performance = {"plants": {}, "won_pp": {}, "opp_plants": {}, "won_retakes": {}}
        data = data_matches[match_id]
        round = data["roundResults"][0]
        if round["bombPlanter"] in basic_info["players"].keys():
            if round["plantSite"] not in performance["plants"].keys():
                performance["plants"][round["plantSite"]] = 0
            performance["plants"][round["plantSite"]] += 1
            if round["winningTeam"] == basic_info["matches"][match_id]["color"]:
                if round["plantSite"] not in performance["won_pp"].keys():
                    performance["won_pp"][round["plantSite"]] = 0
                performance["won_pp"][round["plantSite"]] += 1
        elif round["bombPlanter"] != None:
            if round["plantSite"] not in performance["opp_plants"].keys():
                performance["opp_plants"][round["plantSite"]] = 0
            performance["opp_plants"][round["plantSite"]] += 1
            if round["winningTeam"] == basic_info["matches"][match_id]["color"]:
                if round["plantSite"] not in performance["won_retakes"].keys():
                    performance["won_retakes"][round["plantSite"]] = 0
                performance["won_retakes"][round["plantSite"]] += 1
        round = data["roundResults"][12]
        if round["bombPlanter"] in basic_info["players"].keys():
            if round["plantSite"] not in performance["plants"].keys():
                performance["plants"][round["plantSite"]] = 0
            performance["plants"][round["plantSite"]] += 1
            if round["winningTeam"] == basic_info["matches"][match_id]["color"]:
                if round["plantSite"] not in performance["won_pp"].keys():
                    performance["won_pp"][round["plantSite"]] = 0
                performance["won_pp"][round["plantSite"]] += 1
        elif round["bombPlanter"] != None:
            if round["plantSite"] not in performance["opp_plants"].keys():
                performance["opp_plants"][round["plantSite"]] = 0
            performance["opp_plants"][round["plantSite"]] += 1
            if round["winningTeam"] == basic_info["matches"][match_id]["color"]:
                if round["plantSite"] not in performance["won_retakes"].keys():
                    performance["won_retakes"][round["plantSite"]] = 0
                performance["won_retakes"][round["plantSite"]] += 1
        for key in performance.keys():
            for sub_key, value in performance[key].items():
                if sub_key not in global_performance[key]:
                    global_performance[key][sub_key] = 0
                global_performance[key][sub_key] += value
    table = [[]]
    table[0].append("All")
    num_plants = sum(global_performance["plants"].values())
    table[0].append(num_plants)
    num_won_plants = sum(global_performance["won_pp"].values())
    if num_plants == 0:
        table[0].append("")
    else:
        table[0].append(f"{custom_round(100*num_won_plants/num_plants)}% ({num_won_plants}/{num_plants})")
    num_oppplants = sum(global_performance["opp_plants"].values())
    table[0].append(num_oppplants)
    num_won_oppplants = sum(global_performance["won_retakes"].values())
    if num_oppplants == 0:
        table[0].append("")
    else:
        table[0].append(f"{custom_round(100*num_won_oppplants/num_oppplants)}% ({num_won_oppplants}/{num_oppplants})")
    for i, site in enumerate(global_performance["plants"]):
        i = i+1
        table.append([])
        table[i].append(site)
        table[i].append("{} ({}%)".format(global_performance["plants"][site], custom_round(100*global_performance["plants"][site]/num_plants)))
        if site not in global_performance["won_pp"].keys():
            global_performance["won_pp"][site] = 0
        table[i].append("{}% ({}/{})".format(custom_round(100*global_performance["won_pp"][site]/global_performance["plants"][site]), global_performance["won_pp"][site], global_performance["plants"][site]))
        if site not in global_performance["opp_plants"].keys():
            table[i].append(0)
            table[i].append("")
        else:
            table[i].append("{} ({}%)".format(global_performance["opp_plants"][site], custom_round(100*global_performance["opp_plants"][site]/num_oppplants)))
            if site not in global_performance["won_retakes"].keys():
                global_performance["won_retakes"][site] = 0
            table[i].append("{}% ({}/{})".format(custom_round(100*global_performance["won_retakes"][site]/global_performance["opp_plants"][site]), global_performance["won_retakes"][site], global_performance["opp_plants"][site]))

    return table


def get_sniper_kills(map_name, side, list_ids, data_matches, basic_info, path):
    points = []
    for match_id in list_ids:
        color = basic_info["matches"][match_id]["color"]
        data = data_matches[match_id]
        for round in data["roundResults"]:
            if (side == "def" and ((color == "Blue" and round["roundNum"]<12) or (color=="Blue" and round["roundNum"]>23 and round["roundNum"]%2 == 0) or (color == "Red" and round["roundNum"]>11) or (color=="Red" and round["roundNum"]>23 and round["roundNum"]%1 == 0))) or (side == "atk" and ((color == "Red" and round["roundNum"]<12) or (color=="Red" and round["roundNum"]>23 and round["roundNum"]%2 == 0) or (color == "Blue" and round["roundNum"]>11) or (color=="Blue" and round["roundNum"]>23 and round["roundNum"]%1 == 0))):
                for player in round["playerStats"]:
                    for kill in player["kills"]:
                        if kill["killer"] in basic_info["players"].keys() and kill["finishingDamage"]["damageType"] == "Weapon":
                            weapon = get_weapon_by_puuid(kill["finishingDamage"]["damageItem"])["data"]["displayName"]
                            if weapon in ["Operator", "Marshall", "Outlaw"]:
                                kill_list = {}
                                kill_list["victim"] = {"id": kill["victim"], "loc": kill["victimLocation"]}
                                for player in kill["playerLocations"]:
                                    if player["puuid"] == kill["killer"]:
                                        kill_list["killer"] = {"id": kill["killer"], "loc": player["location"]}
                                points.append(kill_list)
    sns.set_theme(style="white")
    map_name = map_name.lower()
    map_image_path = f"maps/{map_name}.png"
    map_img = mpimg.imread(map_image_path)
    if map_name in ["icebox", "sunset", "ascent", "pearl", "corrode", "bind"]:
        map_img = np.flipud(map_img)

    unique_players = [basic_info["players"][id].split()[1] for id in basic_info["players"]]
    fixed_colors = ["red", "green", "yellow", "cyan", "blue"]
    player_colors = {player: fixed_colors[i % len(fixed_colors)] for i, player in enumerate(sorted(unique_players))}

    params = {
    "ascent": {
        "xMultiplier": 0.00007,
        "yMultiplier": -0.00007,
        "xScalarToAdd": 0.813895,
        "yScalarToAdd": 0.573242,
    },
    "split": {
        "xMultiplier": 0.000078,
        "yMultiplier": -0.000078,
        "xScalarToAdd": 0.842188,
        "yScalarToAdd": 0.697578,
    },
    "fracture": {
        "xMultiplier": 0.000078,
        "yMultiplier": -0.000078,
        "xScalarToAdd": 0.556952,
        "yScalarToAdd": 1.155886,
    },
    "bind": {
        "xMultiplier": 0.000059,
        "yMultiplier": -0.000059,
        "xScalarToAdd": 0.576941,
        "yScalarToAdd": 0.967566,
    },
    "breeze": {
        "xMultiplier": 0.00007,
        "yMultiplier": -0.00007,
        "xScalarToAdd": 0.465123,
        "yScalarToAdd": 0.833078,
    },
    "abyss": {
        "xMultiplier": 0.000081,
        "yMultiplier": -0.000081,
        "xScalarToAdd": 0.5,
        "yScalarToAdd": 0.5,
    },
    "lotus": {
        "xMultiplier": 0.000072,
        "yMultiplier": -0.000072,
        "xScalarToAdd": 0.454789,
        "yScalarToAdd": 0.917752,
    },
    "sunset": {
        "xMultiplier": 0.000078,
        "yMultiplier": -0.000078,
        "xScalarToAdd": 0.5,
        "yScalarToAdd": 0.515625,
    },
    "pearl": {
        "xMultiplier": 0.000078,
        "yMultiplier": -0.000078,
        "xScalarToAdd": 0.480469,
        "yScalarToAdd": 0.916016,
    },
    "icebox": {
        "xMultiplier": 0.000072,
        "yMultiplier": -0.000072,
        "xScalarToAdd": 0.460214,
        "yScalarToAdd": 0.304687,
    },
    "haven": {
        "xMultiplier": 0.000075,
        "yMultiplier": -0.000075,
        "xScalarToAdd": 1.09345,
        "yScalarToAdd": 0.642728,
    },
    "corrode":{
        "xMultiplier": 0.000075,
        "yMultiplier": -0.000075,
        "xScalarToAdd":0.526158,
        "yScalarToAdd":0.5
    }
}

    fig, ax = plt.subplots(figsize=(10, 10))
    ax.imshow(map_img, extent=[0, 1, 0, 1], origin='upper')
    for point in points:
        for player_id, loc in point.items():
            if map_name in ["haven", "split", "lotus", "icebox", "sunset", "ascent", "pearl", "corrode", "bind"]:
                x, y = (loc["loc"]["y"] * params[map_name]["xMultiplier"] + params[map_name]["xScalarToAdd"]), (loc["loc"]["x"] * params[map_name]["yMultiplier"] + params[map_name]["yScalarToAdd"])
            else:
                x, y = (loc["loc"]["x"] * params[map_name]["xMultiplier"] + params[map_name]["xScalarToAdd"]), (loc["loc"]["y"] * params[map_name]["yMultiplier"] + params[map_name]["yScalarToAdd"]) 
            if player_id != "victim":  # Skip the victim marker
                name = basic_info["players"][loc["id"]].split()[1]
                ax.scatter(x,y, color= player_colors[name], label=name, alpha=1, s=40)
            else:
                ax.scatter(x,y, color= "red", alpha=1, s=40, marker = "x")
        if map_name in ["haven", "split", "lotus", "icebox", "sunset", "ascent", "pearl", "corrode", "bind"]:
            killer_x = point["killer"]["loc"]["y"] * params[map_name]["xMultiplier"] + params[map_name]["xScalarToAdd"]
            killer_y = point["killer"]["loc"]["x"] * params[map_name]["yMultiplier"] + params[map_name]["yScalarToAdd"]
            victim_x = point["victim"]["loc"]["y"] * params[map_name]["xMultiplier"] + params[map_name]["xScalarToAdd"]
            victim_y = point["victim"]["loc"]["x"] * params[map_name]["yMultiplier"] + params[map_name]["yScalarToAdd"]
        else:
            killer_x = point["killer"]["loc"]["y"] * params[map_name]["xMultiplier"] + params[map_name]["xScalarToAdd"]
            killer_y = point["killer"]["loc"]["x"] * params[map_name]["yMultiplier"] + params[map_name]["yScalarToAdd"]
            victim_x = point["victim"]["loc"]["y"] * params[map_name]["xMultiplier"] + params[map_name]["xScalarToAdd"]
            victim_y = point["victim"]["loc"]["x"] * params[map_name]["yMultiplier"] + params[map_name]["yScalarToAdd"]
        ax.plot([killer_x, victim_x], [killer_y, victim_y],
                color="black", linestyle="-", linewidth=.5)
    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ax.legend(by_label.values(), by_label.keys(), title="Players", loc="upper left", bbox_to_anchor=(1, .5), fontsize=12)

    ax.tick_params(left=False, bottom=False)
    sns.despine(left=True, bottom=True)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    if side == "def":
        ax.set_title("Defending Sniper Kills", fontsize=16, weight="bold")
    elif side == "atk":
        ax.set_title("Attacking Sniper Kills", fontsize=16, weight="bold")
    plt.tight_layout()
    ax.axis('off')
    if map_name in ["haven", "split", "lotus", "icebox", "sunset", "ascent", "pearl", "corrode", "bind"]:
        ax.invert_yaxis()
    plt.savefig(path, bbox_inches="tight", dpi=600)
    plt.close('all')


def _summarize_match(match_data: dict, own_tag: str) -> str:
    """Build a short 'TAG vs OPP - Map: MAP - Result: a:b' line."""
    try:
        map_name = get_map_by_id(match_data["matchInfo"]["mapId"])
    except Exception:
        map_name = "Unknown map"

    # Result (order as provided by API)
    try:
        res1, res2 = match_data["teams"][0]["roundsWon"], match_data["teams"][1]["roundsWon"]
        result = f"{res1}:{res2}"
    except Exception:
        result = "?:?"

    # Try to infer opponent tag from player gameName (e.g., "TH Boo")
    opp_tag = "UNKNOWN"
    try:
        for p in match_data.get("players", []):
            if p.get("teamId") in {"Blue", "Red"}:
                name = p.get("gameName", "")
                tag = name.split(" ")[0] if " " in name else ""
                if tag and tag != own_tag:
                    opp_tag = tag
                    break
    except Exception:
        pass

    return f"{own_tag} vs {opp_tag} - Map: {map_name} - Result: {result}"
