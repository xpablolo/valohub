import gspread
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from gspread_formatting import *
from googleapiclient.http import MediaFileUpload
import io
import requests
import string
import time
import sys

from functions import get_basic_info, get_match_by_match_id, get_comps, get_image_link, get_plants, create_early_positioning, get_pistol_plants, get_sniper_kills, get_teams, get_puuid_by_riotid, get_matchlist_by_puuid, get_map_by_id, _summarize_match

# data 
player_list = {"TH": "TH Boo", "TL": "TL nAts", "GX": "GX Cloud", "FNC": "FNC Boaster", "DRG": "DRG Nicc", "T1": "T1 Meteor", "G2": "G2 valyn", "NRG": "NRG s0m", "SEN": "SEN bang", "MIBR": "MIBR xenom", "BLG": "BLG whzy", "EDG": "EDG Smoggy", "XLG": "XLG Rarga", "PRX": "PRX f0rsakeN", "RRQ": "RRQ Jemkin", "DRX": "DRX MaKo"}

# Authenticate
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('valorant-sheets-credentials.json', scope)
client = gspread.authorize(creds)

# INTERACTIVE INPUT: 
try: 
    teams_raw = get_teams()
    all_teams = {t["tag"]: (t.get("name"), t.get("image")) for t in (teams_raw.values() if isinstance(teams_raw, dict) else teams_raw)}

    team = input("Enter team to analyze (e.g. TH): ").strip().upper()
    if not team:
        raise ValueError("Team code cannot be empty.")
    long_team, image = all_teams.get(team, (None, None))
    if not long_team:
        raise ValueError(f"Team code '{team}' not found.")
    
    player = player_list.get(team, None)
    if not player:
        raise ValueError(f"No player data for team '{team}'. Please add manually.")
    player_puuid = get_puuid_by_riotid(player, "epval", "esports")["puuid"]
    match_list = get_matchlist_by_puuid(player_puuid, "esports")["history"]
    if not match_list:
        raise ValueError("No matches found for this player/team.")

    available = len(match_list)
    print(f"Found {available} matches for {long_team} ({team}).")

    
    while True:
        try:
            n_str = input(f"How many most-recent matches to include? (1-{available})").strip()
            n = int(n_str)
            if n <= 0:
                raise ValueError("Number must be positive.")
        except ValueError:
            print("Please enter a valid integer.")
            continue
        
        selected = match_list[:n]
        list_ids = [m["matchId"] for m in selected]
        time.sleep(0.3)
        first_match = get_match_by_match_id(list_ids[0], "esports")
        time.sleep(0.3)
        last_match = get_match_by_match_id(list_ids[-1], "esports")

        print("\nYou are about to compute:")
        print("  • Most recent match:    " + _summarize_match(first_match, team))
        print("  • Oldest in selection:  " + _summarize_match(last_match, team))

        ans = input("Proceed with these matches? [y/N] ").strip().lower()
        if ans in {"y", "yes"}:
            break
    
    print(f"\nConfirmed. Using {n} matches for {long_team} ({team}).")

except KeyboardInterrupt:
    print("\nAborted by user.")
    sys.exit(1)
except Exception as e:
    print(f"Error while fetching match data: {e}")
    sys.exit(1)

#add team image
image_url = f"https://imagedelivery.net/WUSOKAY-iA_QQPngCXgUJg/{image}/w=10000"


# Create a new spreadsheet
spreadsheet = client.create("New Analysis Report")
sheet = spreadsheet.sheet1
sheet.update_title("Overall")

sheet.merge_cells('I4:J9')

sheet.update([[f'=IMAGE("{image_url}")']], "I4", value_input_option="USER_ENTERED")
format_cell_range(sheet, 'I4', CellFormat(
    backgroundColor=Color(0, 0, 0),  # #434343 in RGB (0.26, 0.26, 0.26)
    horizontalAlignment='CENTER'))

data_matches = {}
for match_id in list_ids:
    time.sleep(1)
    data = get_match_by_match_id(match_id, "esports")
    data_matches[match_id] = data
basic_info = get_basic_info(team, "all", data_matches)

print("Basic info gathered. Beginning " + long_team + " Report.")
# Add data
sheet.merge_cells('A1:G1')
title = "Analytical Report of " + long_team + " on Stage 2 & Champs: Overall"
sheet.update([[title]], "A1")

sheet.merge_cells('A3:G3')
sheet.update([["Matches Played"]], "A3")
header_data = [
    ["Team", "Result", "", "Rival", "Map", f"{team}'s DEF", f"{team}'s ATK"]
]

# Perform the batch update for the header row (A4:G4)
sheet.update(range_name='A4:G4', values=header_data)
sheet.merge_cells('B4:C4')

format_cell_range(sheet, 'A1', CellFormat(
    backgroundColor=Color(0.26, 0.26, 0.26),  # #434343 in RGB (0.26, 0.26, 0.26)
    horizontalAlignment='LEFT',
    textFormat=TextFormat(
        foregroundColor=Color(1.0, 1.0, 1.0),  # white text
        fontSize=14,
        bold=True
    )
))

format_cell_range(sheet, 'A3', CellFormat(
    backgroundColor=Color(0.36, 0.36, 0.36),  # #434343 in RGB (0.26, 0.26, 0.26)
    horizontalAlignment='LEFT',
    textFormat=TextFormat(
        foregroundColor=Color(1.0, 1.0, 1.0),  # white text
        fontSize=12,
        bold=True
    )
))

format_cell_range(sheet, 'A4:G4', CellFormat(
    backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
    horizontalAlignment='LEFT',
    textFormat=TextFormat(
        foregroundColor=Color(1.0, 1.0, 1.0),  # white text
        fontSize=10,
        bold=True
    )
))


format_cell_range(sheet, f'A5:G{4+len(basic_info["matches"])}', CellFormat(
    backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
    horizontalAlignment='LEFT',
    textFormat=TextFormat(
        foregroundColor=Color(1.0, 1.0, 1.0),  # white text
        fontSize=9,
        bold=False
    )
))
format_cell_range(sheet, f'B5:B{4+len(basic_info["matches"])}', CellFormat(
    backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
    horizontalAlignment='RIGHT',
    textFormat=TextFormat(
        foregroundColor=Color(1.0, 1.0, 1.0),  # white text
        fontSize=9,
        bold=False
    )
))

matches_data = []
maps_stats = {}
for i, match in enumerate(basic_info["matches"]):
    match = basic_info["matches"][match]
    match_row = [
        basic_info["team"],
        match["result"][1][0],
        match["result"][1][1],
        match["rival"],
        match["map"],
        f"{int(100 * match['result'][2][0] / match['result'][3][0])}% ({match['result'][2][0]}/{match['result'][3][0]})",
        f"{int(100 * match['result'][2][1] / match['result'][3][1])}% ({match['result'][2][1]}/{match['result'][3][1]})"
    ]
    matches_data.append(match_row)
    
    if match["map"] not in maps_stats:
        maps_stats[match["map"]] = [[0,0], [0,0], [0,0], []]
    if match["result"][0] == "Win":
        maps_stats[match["map"]][0][0] +=1
    else:
        maps_stats[match["map"]][0][1] +=1
    maps_stats[match["map"]][1][0] += match["result"][2][0]
    maps_stats[match["map"]][1][1] += match["result"][3][0]
    maps_stats[match["map"]][2][0] += match["result"][2][1]
    maps_stats[match["map"]][2][1] += match["result"][3][1]
    maps_stats[match["map"]][3].append(match["match_id"])
    if match["result"][1][0] < match["result"][1][1]:
        format_cell_range(sheet, f'B{5+i}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='RIGHT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 0, 0),  # white text
                fontSize=9,
                bold=False)))
    else:
        format_cell_range(sheet, f'B{5+i}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='RIGHT',
            textFormat=TextFormat(
                foregroundColor=Color(0.204, 0.659, 0.325),  # white text
                fontSize=9,
                bold=False
            )
        ))
    if int(100*match["result"][2][0]/match["result"][3][0]) > 50:
        format_cell_range(sheet, f'F{5+i}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(0.204, 0.659, 0.325),  # white text
                fontSize=9,
                bold=False
            )
        ))
    elif int(100*match["result"][2][0]/match["result"][3][0]) < 50:
        format_cell_range(sheet, f'F{5+i}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 0, 0),  # white text
                fontSize=9,
                bold=False)))
    else:
        format_cell_range(sheet, f'F{5+i}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(0.984, 0.737, 0.016),  # white text
                fontSize=9,
                bold=False)))
    time.sleep(2)
    if int(100*match["result"][2][1]/match["result"][3][1]) > 50:
        format_cell_range(sheet, f'G{5+i}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(0.204, 0.659, 0.325),  # white text
                fontSize=9,
                bold=False
            )
        ))
    elif int(100*match["result"][2][1]/match["result"][3][1]) < 50:
        format_cell_range(sheet, f'G{5+i}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 0, 0),  # white text
                fontSize=9,
                bold=False)))
    else:
        format_cell_range(sheet, f'G{5+i}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(0.984, 0.737, 0.016),  # white text
                fontSize=9,
                bold=False)))

sheet.update(range_name=f'A5:G{4+len(basic_info["matches"])}', values=matches_data)


format_cell_range(sheet, 'B4', CellFormat(
    backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
    horizontalAlignment='CENTER',
    textFormat=TextFormat(
        foregroundColor=Color(1.0, 1.0, 1.0),  # white text
        fontSize=10,
        bold=True
    )
))

len_basic_info = len(basic_info["matches"])
sheet.merge_cells(f'A{6+len(basic_info["matches"])}:G{6+len_basic_info}')
header_data = [
    ["Performance by Map"],
    ["Map", "Won", "Lost", "Winrate", "DEF Winrate", "ATK Winrate"]
]
sheet.update(range_name= f'A{6+len_basic_info}:G{7+len_basic_info}', values=header_data)

format_cell_range(sheet, f'A{6+len_basic_info}', CellFormat(
    backgroundColor=Color(0.36, 0.36, 0.36),  # #434343 in RGB (0.26, 0.26, 0.26)
    horizontalAlignment='LEFT',
    textFormat=TextFormat(
        foregroundColor=Color(1.0, 1.0, 1.0),  # white text
        fontSize=12,
        bold=True
    )
))

format_cell_range(sheet, f'A{7+len_basic_info}:G{7+len_basic_info}', CellFormat(
    backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
    horizontalAlignment='LEFT',
    textFormat=TextFormat(
        foregroundColor=Color(1.0, 1.0, 1.0),  # white text
        fontSize=10,
        bold=True
    )
))


format_cell_range(sheet, f'A{8+len_basic_info}:G{7+len_basic_info+len(maps_stats)}', CellFormat(
    backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
    horizontalAlignment='LEFT',
    textFormat=TextFormat(
        foregroundColor=Color(1.0, 1.0, 1.0),  # white text
        fontSize=9,
        bold=False
    )
))


map_performance_data = []
for l, map in enumerate(maps_stats):
    total_wins = maps_stats[map][0][0]
    total_losses = maps_stats[map][0][1]
    winrate = int(100 * total_wins / (total_wins + total_losses)) if (total_wins + total_losses) > 0 else 0
    def_winrate = int(100 * maps_stats[map][1][0] / maps_stats[map][1][1]) if maps_stats[map][1][1] > 0 else 0
    atk_winrate = int(100 * maps_stats[map][2][0] / maps_stats[map][2][1]) if maps_stats[map][2][1] > 0 else 0
    if winrate > 50:
        format_cell_range(sheet, f'D{8+len_basic_info+l}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(0.204, 0.659, 0.325),  # white text
                fontSize=9,
                bold=False)))
    elif winrate < 50:
        format_cell_range(sheet, f'D{8+len_basic_info+l}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 0, 0),  # white text
                fontSize=9,
                bold=False)))
    else:
        format_cell_range(sheet, f'D{8+len_basic_info+l}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(0.984, 0.737, 0.016),  # white text
                fontSize=9,
                bold=False)))
    if def_winrate > 50:
        format_cell_range(sheet, f'E{8+len_basic_info+l}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(0.204, 0.659, 0.325),  # white text
                fontSize=9,
                bold=False
            )
        ))
    elif def_winrate < 50:
        format_cell_range(sheet, f'E{8+len_basic_info+l}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 0, 0),  # white text
                fontSize=9,
                bold=False)))
    else:
        format_cell_range(sheet, f'E{8+len_basic_info+l}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(0.984, 0.737, 0.016),  # white text
                fontSize=9,
                bold=False)))
    if atk_winrate > 50:
        format_cell_range(sheet, f'F{8+len_basic_info+l}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(0.204, 0.659, 0.325),  # white text
                fontSize=9,
                bold=False
            )
        ))
    elif atk_winrate < 50:
        format_cell_range(sheet, f'F{8+len_basic_info+l}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 0, 0),  # white text
                fontSize=9,
                bold=False)))
    else:
        format_cell_range(sheet, f'F{8+len_basic_info+l}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(0.984, 0.737, 0.016),  # white text
                fontSize=9,
                bold=False)))
    
    print("First sheet completed.")
    time.sleep(3)
    map_sheet = spreadsheet.add_worksheet(title=map, rows="100", cols="15")
    map_performance_data.append([f'=HYPERLINK("#gid={map_sheet.id}", "{map}")', total_wins, total_losses, f"{winrate}%", f"{def_winrate}%", f"{atk_winrate}%"])
    
    if map == map:
        # Add data
        title = "Analytical Report of " + long_team + f": {map}"
        map_sheet.update([[title]], "A1")
        map_sheet.update([["Agent Compositions"]], "A3")
        compositions = get_comps(team, maps_stats[map][3])
        all_players = set()
        for comp, matches in compositions.items():
            for match in matches:
                all_players.update(player.split()[1] for player in match["player_agent_mapping"].keys())
        all_players = list(sorted(all_players))

        time.sleep(3)
        header_data = [["Picks"] + all_players + ["Winrate"]]
        letters = string.ascii_uppercase
        # Perform the batch update for the header row (A4:G4)
        map_sheet.merge_cells(f'A1:{letters[len(all_players)+1]}1')
        map_sheet.merge_cells(f'A3:{letters[len(all_players)+1]}3')
        map_sheet.update(range_name=f'A4:{letters[len(all_players)+1]}4', values=header_data)
        final = []
        for i, comp in enumerate(compositions):
            final.append([])
            final[i].append(len(compositions[comp]))
            for player in all_players:
                player = team + " " + player
                if player not in compositions[comp][0]["player_agent_mapping"].keys():
                    final[i].append(" ")
                else:
                    final[i].append(compositions[comp][0]["player_agent_mapping"][player])
            final[i].append("{}%".format(int(100*compositions[comp][0]["win"]/len(compositions[comp]))))
        map_sheet.update(range_name=f"A5:{letters[len(all_players)+1]}{4+len(final)}", values=final)

        header = [["General post-plant performance", "", "", "", ""],
                 ["", "Attacking", "", "Defending", ""],
                 ["Site", "Times Planted", "post-plant WR", "Opp Planted", "retaking WR"]]
        map_data ={}
        for map_id in maps_stats[map][3]:
            map_data[map_id] = data_matches[map_id]
        plant_performance = get_plants(map_data, basic_info)
        final_table = header + plant_performance

        print(f"{l+1} Map. First pause begins")
        time.sleep(2)
        map_sheet.update(final_table, f"A{6+len(final)}:E{9+len(final)+len(plant_performance)}")
        map_sheet.merge_cells(f'A{6+len(final)}:E{6+len(final)}')
        map_sheet.merge_cells(f'B{7+len(final)}:C{7+len(final)}')
        map_sheet.merge_cells(f'D{7+len(final)}:E{7+len(final)}')

        pistol_header = [["Pistol round post-plant performance", "", "", "", ""],
                 ["", "Attacking", "", "Defending", ""],
                 ["Site", "Times Planted", "post-plant WR", "Opp Planted", "retaking WR"]]
        pistol_plant_performance = get_pistol_plants(map_data, basic_info)
        final_table = pistol_header + pistol_plant_performance
        map_sheet.update(final_table, f"G{6+len(final)}:K{9+len(final)+len(pistol_plant_performance)}")
        map_sheet.merge_cells(f'G{6+len(final)}:K{6+len(final)}')
        map_sheet.merge_cells(f'H{7+len(final)}:I{7+len(final)}')
        map_sheet.merge_cells(f'J{7+len(final)}:K{7+len(final)}')

        print("Def positioning loading")
        map_sheet.update([["Defending early team positioning"]], f"A{10+len(final)+len(plant_performance)}")
        map_sheet.merge_cells(f'A{10+len(final)+len(plant_performance)}:L{10+len(final)+len(plant_performance)}')
        def_pos_10s = create_early_positioning(map, "def", 10, maps_stats[map][3], map_data, basic_info, "plots/def_pos_10s.png")
        def_pos_10s_link = get_image_link("def_pos_10s.png", "plots/def_pos_10s.png", creds)
        map_sheet.update([[f'=IMAGE("https://drive.google.com/uc?id={def_pos_10s_link}")']], f"A{11+len(final)+len(plant_performance)}", value_input_option="USER_ENTERED")
        map_sheet.merge_cells(f'A{11+len(final)+len(plant_performance)}:D{27+len(final)+len(plant_performance)}')

        def_pos_20s = create_early_positioning(map, "def", 20, maps_stats[map][3], map_data, basic_info, "plots/def_pos_20s.png")
        def_pos_20s_link = get_image_link("def_pos_20s.png", "plots/def_pos_20s.png", creds)
        map_sheet.update([[f'=IMAGE("https://drive.google.com/uc?id={def_pos_20s_link}")']], f"E{11+len(final)+len(plant_performance)}", value_input_option="USER_ENTERED")
        map_sheet.merge_cells(f'E{11+len(final)+len(plant_performance)}:H{27+len(final)+len(plant_performance)}')

        def_pos_30s = create_early_positioning(map, "def", 30, maps_stats[map][3], map_data, basic_info, "plots/def_pos_30s.png")
        def_pos_30s_link = get_image_link("def_pos_30s.png", "plots/def_pos_30s.png", creds)
        map_sheet.update([[f'=IMAGE("https://drive.google.com/uc?id={def_pos_30s_link}")']], f"I{11+len(final)+len(plant_performance)}", value_input_option="USER_ENTERED")
        map_sheet.merge_cells(f'I{11+len(final)+len(plant_performance)}:L{27+len(final)+len(plant_performance)}')

        map_sheet.update([["Defending sniper kills"]], f"A{29+len(final)+len(plant_performance)}")
        map_sheet.merge_cells(f'A{29+len(final)+len(plant_performance)}:D{29+len(final)+len(plant_performance)}')
        def_sniper_kills = get_sniper_kills(map, "def", maps_stats[map][3], map_data, basic_info, "plots/def_sniper.png")
        def_sniper_link = get_image_link("def_sniper.png", "plots/def_sniper.png", creds)
        map_sheet.update([[f'=IMAGE("https://drive.google.com/uc?id={def_sniper_link}")']], f"A{30+len(final)+len(plant_performance)}", value_input_option="USER_ENTERED")
        map_sheet.merge_cells(f'A{30+len(final)+len(plant_performance)}:D{46+len(final)+len(plant_performance)}')

        print("Atk positioning loading")
        map_sheet.update([["Attacking early team positioning"]], f"A{49+len(final)+len(plant_performance)}")
        map_sheet.merge_cells(f'A{49+len(final)+len(plant_performance)}:L{49+len(final)+len(plant_performance)}')
        atk_pos_10s = create_early_positioning(map, "atk", 10, maps_stats[map][3], map_data, basic_info, "plots/atk_pos_10s.png")
        atk_pos_10s_link = get_image_link("atk_pos_10s.png", "plots/atk_pos_10s.png", creds)
        map_sheet.update([[f'=IMAGE("https://drive.google.com/uc?id={atk_pos_10s_link}")']], f"A{50+len(final)+len(plant_performance)}", value_input_option="USER_ENTERED")
        map_sheet.merge_cells(f'A{50+len(final)+len(plant_performance)}:D{66+len(final)+len(plant_performance)}')

        atk_pos_20s = create_early_positioning(map, "atk", 20, maps_stats[map][3], map_data, basic_info, "plots/atk_pos_20s.png")
        atk_pos_20s_link = get_image_link("atk_pos_20s.png", "plots/atk_pos_20s.png", creds)
        map_sheet.update([[f'=IMAGE("https://drive.google.com/uc?id={atk_pos_20s_link}")']], f"E{50+len(final)+len(plant_performance)}", value_input_option="USER_ENTERED")
        map_sheet.merge_cells(f'E{50+len(final)+len(plant_performance)}:H{66+len(final)+len(plant_performance)}')

        atk_pos_30s = create_early_positioning(map, "atk", 30, maps_stats[map][3], map_data, basic_info, "plots/atk_pos_30s.png")
        atk_pos_30s_link = get_image_link("atk_pos_30s.png", "plots/atk_pos_30s.png", creds)
        map_sheet.update([[f'=IMAGE("https://drive.google.com/uc?id={atk_pos_30s_link}")']], f"I{50+len(final)+len(plant_performance)}", value_input_option="USER_ENTERED")
        map_sheet.merge_cells(f'I{50+len(final)+len(plant_performance)}:L{66+len(final)+len(plant_performance)}')

        map_sheet.update([["Attacking sniper kills"]], f"A{68+len(final)+len(plant_performance)}")
        map_sheet.merge_cells(f'A{68+len(final)+len(plant_performance)}:D{68+len(final)+len(plant_performance)}')
        atk_sniper_kills = get_sniper_kills(map, "atk", maps_stats[map][3], map_data, basic_info, "plots/atk_sniper.png")
        atk_sniper_link = get_image_link("atk_sniper.png", "plots/atk_sniper.png", creds)
        map_sheet.update([[f'=IMAGE("https://drive.google.com/uc?id={atk_sniper_link}")']], f"A{69+len(final)+len(plant_performance)}", value_input_option="USER_ENTERED")
        map_sheet.merge_cells(f'A{69+len(final)+len(plant_performance)}:D{85+len(final)+len(plant_performance)}')


        print(f"{l+1} Map. Second pause begins")
        time.sleep(3)
        format_cell_range(map_sheet, 'A1', CellFormat(
            backgroundColor=Color(0.26, 0.26, 0.26),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0),  # white text
                fontSize=14,
                bold=True)))

        format_cell_range(map_sheet, 'A3', CellFormat(
            backgroundColor=Color(0.36, 0.36, 0.36),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0),  # white text
                fontSize=12,
                bold=True)))

        format_cell_range(map_sheet, f'A4:{letters[len(all_players)+1]}4', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0),  # white text
                fontSize=10,
                bold=True)))  

        format_cell_range(map_sheet, f'A5:{letters[len(all_players)+1]}{4+len(final)}', CellFormat(
        backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
        horizontalAlignment='LEFT',
        textFormat=TextFormat(
            foregroundColor=Color(1.0, 1.0, 1.0),  # white text
            fontSize=9,
            bold=False)))
        
        print(f"{l+1} Map. Third pause begins")
        time.sleep(3)

        format_cell_range(map_sheet, f'A{6+len(final)}:E{7+len(final)}', CellFormat(
            backgroundColor=Color(0.36, 0.36, 0.36),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0),  # white text
                fontSize=12,
                bold=True)))

        format_cell_range(map_sheet, f'A{8+len(final)}:E{8+len(final)}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0),  # white text
                fontSize=10,
                bold=True)))  
        
        format_cell_range(map_sheet, f'A{9+len(final)}:E{8+len(final)+len(plant_performance)}', CellFormat(
        backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
        horizontalAlignment='LEFT',
        textFormat=TextFormat(
            foregroundColor=Color(1.0, 1.0, 1.0),  # white text
            fontSize=9,
            bold=False)))
        
        #pistols
        format_cell_range(map_sheet, f'G{6+len(final)}:K{7+len(final)}', CellFormat(
            backgroundColor=Color(0.36, 0.36, 0.36),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0),  # white text
                fontSize=12,
                bold=True)))

        format_cell_range(map_sheet, f'G{8+len(final)}:K{8+len(final)}', CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0),  # white text
                fontSize=10,
                bold=True)))  
        
        format_cell_range(map_sheet, f'G{9+len(final)}:K{8+len(final)+len(pistol_plant_performance)}', CellFormat(
        backgroundColor=Color(0.047, 0.204, 0.239),  # #434343 in RGB (0.26, 0.26, 0.26)
        horizontalAlignment='LEFT',
        textFormat=TextFormat(
            foregroundColor=Color(1.0, 1.0, 1.0),  # white text
            fontSize=9,
            bold=False)))
        
        format_cell_range(map_sheet, f'A{10+len(final)+len(plant_performance)}', CellFormat(
            backgroundColor=Color(0.36, 0.36, 0.36),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0),  # white text
                fontSize=12,
                bold=True))) 
        format_cell_range(map_sheet, f'A{49+len(final)+len(plant_performance)}', CellFormat(
            backgroundColor=Color(0.36, 0.36, 0.36),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0),  # white text
                fontSize=12,
                bold=True))) 
        format_cell_range(map_sheet, f'A{29+len(final)+len(plant_performance)}', CellFormat(
            backgroundColor=Color(0.36, 0.36, 0.36),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0),  # white text
                fontSize=12,
                bold=True))) 
        format_cell_range(map_sheet, f'A{68+len(final)+len(plant_performance)}', CellFormat(
            backgroundColor=Color(0.36, 0.36, 0.36),  # #434343 in RGB (0.26, 0.26, 0.26)
            horizontalAlignment='LEFT',
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0),  # white text
                fontSize=12,
                bold=True))) 
        
sheet.update(range_name=f'A{8+len_basic_info}:F{8+len_basic_info+len(map_performance_data)-1}', values=map_performance_data, value_input_option="USER_ENTERED")





spreadsheet.share('pablolopezarauzo@gmail.com', perm_type='user', role='writer')
print(f"Spreadsheet created! URL: {spreadsheet.url}")
