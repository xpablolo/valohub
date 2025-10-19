import os
import string
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence
from uuid import uuid4

import gspread
from gspread_formatting import CellFormat, Color, TextFormat, format_cell_range
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

from functions.functions import (
    _summarize_match,
    create_early_positioning,
    get_basic_info,
    get_comps,
    get_image_link,
    get_match_by_match_id,
    get_matchlist_by_puuid,
    get_pistol_plants,
    get_plants,
    get_puuid_by_riotid,
    get_sniper_kills,
    get_teams,
)

PLAYER_LIST: Dict[str, str] = {
    "TH": "TH Boo",
    "TL": "TL nAts",
    "GX": "GX Cloud",
    "FNC": "FNC Boaster",
    "DRG": "DRG Nicc",
    "T1": "T1 Meteor",
    "G2": "G2 valyn",
    "NRG": "NRG s0m",
    "SEN": "SEN bang",
    "MIBR": "MIBR xenom",
    "BLG": "BLG whzy",
    "EDG": "EDG Smoggy",
    "XLG": "XLG Rarga",
    "PRX": "PRX f0rsakeN",
    "RRQ": "RRQ Jemkin",
    "DRX": "DRX MaKo",
}

DEFAULT_SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
DEFAULT_CREDENTIALS_PATH = os.getenv(
    "ANALYTICAL_REPORT_CREDENTIALS", "api_keys/valorant-sheets-credentials.json"
)
DEFAULT_SHARE_EMAIL = os.getenv("ANALYTICAL_REPORT_SHARE_EMAIL", "pablolopezarauzo@gmail.com")
DEFAULT_SPREADSHEET_TITLE = "New Analysis Report"


class AnalyticalReportError(Exception):
    """Raised when the analytical report cannot be generated."""


@dataclass
class TeamContext:
    tag: str
    name: str
    image_id: str
    player_riot_id: str
    matches: List[Dict]


ProgressCallback = Optional[Callable[[str], None]]
PromptHandler = Optional[Callable[[Dict[str, Any]], str]]


def _build_google_services(creds: ServiceAccountCredentials):
    """Create Drive and Sheets API clients from the service account credentials."""
    try:
        drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
        sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    except Exception as exc:  # pragma: no cover - network/service errors
        raise AnalyticalReportError(f"Failed to initialize Google APIs: {exc}") from exc
    return drive_service, sheets_service


def _ensure_published(drive_service, spreadsheet_id: str) -> None:
    """Enable 'Publish to the web' on the latest revision of the spreadsheet."""
    try:
        revisions = (
            drive_service.revisions()
            .list(
                fileId=spreadsheet_id,
                fields="revisions(id,published,publishAuto,publishedOutsideDomain)",
            )
            .execute()
            .get("revisions", [])
        )
    except Exception as exc:  # pragma: no cover
        raise AnalyticalReportError(f"Failed to fetch revisions for spreadsheet {spreadsheet_id}: {exc}") from exc

    if not revisions:
        raise AnalyticalReportError(f"Spreadsheet {spreadsheet_id} has no revisions to publish.")

    last_rev_id = revisions[-1]["id"]
    last_revision = revisions[-1]
    if (
        last_revision.get("published")
        and last_revision.get("publishAuto")
        and last_revision.get("publishedOutsideDomain")
    ):
        return

    try:
        drive_service.revisions().update(
            fileId=spreadsheet_id,
            revisionId=last_rev_id,
            body={"published": True, "publishAuto": True, "publishedOutsideDomain": True},
        ).execute()
    except Exception as exc:  # pragma: no cover
        raise AnalyticalReportError(f"Failed to publish spreadsheet {spreadsheet_id}: {exc}") from exc


def _get_gid(sheets_service, spreadsheet_id: str, sheet_title: Optional[str] = None) -> int:
    """Return the numeric gid for the requested sheet title (or the first sheet)."""
    try:
        response = (
            sheets_service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                fields="sheets(properties(sheetId,title,index))",
            )
            .execute()
        )
    except Exception as exc:  # pragma: no cover
        raise AnalyticalReportError(f"Failed to read sheet metadata for {spreadsheet_id}: {exc}") from exc

    sheets = response.get("sheets") or []
    if not sheets:
        raise AnalyticalReportError(f"Spreadsheet {spreadsheet_id} does not contain any sheets.")

    properties = [sheet["properties"] for sheet in sheets if "properties" in sheet]
    if sheet_title:
        for prop in properties:
            if prop.get("title") == sheet_title:
                return int(prop["sheetId"])

    first_sheet = min(properties, key=lambda prop: prop.get("index", 0))
    return int(first_sheet["sheetId"])


def _build_publish_url(spreadsheet_id: str, gid: int) -> str:
    """Construct the publish-to-web embed URL for a spreadsheet tab."""
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/pubhtml?gid={gid}&single=true"


def _notify(message: str, progress_callback: ProgressCallback) -> None:
    if progress_callback:
        progress_callback(message)
    else:
        print(message)


def _request_prompt(prompt_handler: PromptHandler, spec: Dict[str, Any]) -> str:
    if prompt_handler is None:
        raise AnalyticalReportError("Interactive input requested but no prompt handler is available.")

    payload = dict(spec)
    payload.setdefault("id", uuid4().hex)
    response = prompt_handler(payload)
    if response is None:
        raise AnalyticalReportError("Interactive prompt aborted by user.")
    return str(response)


def _authorize(credentials_path: Optional[str]) -> tuple[ServiceAccountCredentials, gspread.Client]:
    path = credentials_path or DEFAULT_CREDENTIALS_PATH
    if not path:
        raise AnalyticalReportError("Missing Google service account credentials path.")

    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(path, DEFAULT_SCOPE)
    except Exception as exc:  # pragma: no cover - gspread raises generic errors
        raise AnalyticalReportError(f"Failed to load credentials from '{path}': {exc}") from exc

    try:
        client = gspread.authorize(creds)
    except Exception as exc:  # pragma: no cover
        raise AnalyticalReportError(f"Failed to authorize Google Sheets client: {exc}") from exc
    return creds, client


def _resolve_team(team_code: str) -> TeamContext:
    if not team_code:
        raise AnalyticalReportError("Team code cannot be empty.")

    teams_raw = get_teams()
    all_teams = {
        t["tag"]: (t.get("name"), t.get("image"))
        for t in (teams_raw.values() if isinstance(teams_raw, dict) else teams_raw)
    }

    long_team, image = all_teams.get(team_code.upper(), (None, None))
    if not long_team:
        raise AnalyticalReportError(f"Team code '{team_code}' not found.")

    player_riot_id = PLAYER_LIST.get(team_code.upper())
    if not player_riot_id:
        raise AnalyticalReportError(
            f"No player mapping configured for team '{team_code}'. Please update PLAYER_LIST."
        )

    player_puuid = get_puuid_by_riotid(player_riot_id, "epval", "esports")["puuid"]
    history = get_matchlist_by_puuid(player_puuid, "esports").get("history", [])
    if not history:
        raise AnalyticalReportError("No matches found for this player/team.")

    return TeamContext(
        tag=team_code.upper(),
        name=long_team,
        image_id=image,
        player_riot_id=player_riot_id,
        matches=history,
    )


def _pick_matches(
    context: TeamContext,
    match_count: Optional[int],
    *,
    prompt_handler: PromptHandler = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    progress_callback: ProgressCallback = None,
) -> List[str]:
    available = len(context.matches)
    if available == 0:
        raise AnalyticalReportError("No matches available for selection.")

    def request_count(current: Optional[int], reason: Optional[str] = None) -> int:
        if prompt_handler is None:
            candidate = current if current and current > 0 else available
            return max(1, min(candidate, available))

        while True:
            lines = [f"How many of the {available} most recent matches should be included?"]
            if reason:
                lines.append(reason)
            spec: Dict[str, Any] = {
                "title": "Select match count",
                "message": "\n".join(lines),
                "hint": f"Enter a number between 1 and {available}.",
            }
            if current:
                spec["default"] = str(current)

            response = _request_prompt(prompt_handler, spec).strip()
            if not response and current:
                return max(1, min(current, available))

            try:
                value = int(response)
                if value <= 0:
                    raise ValueError
            except ValueError:
                _notify("Please enter a positive integer.", progress_callback)
                continue

            if value > available:
                _notify(
                    f"Only {available} matches are available. Using the {available} most recent matches.",
                    progress_callback,
                )
                return available

            return value

    if match_count is None:
        count = request_count(None)
    else:
        if match_count <= 0:
            raise AnalyticalReportError("Number of matches must be positive.")
        if match_count > available:
            _notify(
                f"Only {available} matches are available. Using the {available} most recent matches.",
                progress_callback,
            )
            count = available
        else:
            count = match_count

    yes_values = {"yes", "y", "proceed", "p"}
    no_values = {"no", "n", "change", "c"}

    while True:
        selected = context.matches[:count]
        if not selected:
            raise AnalyticalReportError("No matches could be selected with the requested count.")

        match_ids = [m["matchId"] for m in selected]
        sleep_fn(0.3)
        first_match = get_match_by_match_id(match_ids[0], "esports")
        sleep_fn(0.3)
        last_match = get_match_by_match_id(match_ids[-1], "esports")

        most_recent = _summarize_match(first_match, context.tag)
        oldest = _summarize_match(last_match, context.tag)

        _notify(f"Most recent match: {most_recent}", progress_callback)
        if len(match_ids) > 1:
            _notify(f"Oldest in selection: {oldest}", progress_callback)

        if prompt_handler is None:
            return match_ids

        confirm_lines = [
            f"Proceed with the {count} most recent matches?",
            f"Most recent: {most_recent}",
        ]
        if len(match_ids) > 1:
            confirm_lines.append(f"Oldest in selection: {oldest}")

        confirm_response = (
            _request_prompt(
                prompt_handler,
                {
                    "title": "Confirm match selection",
                    "message": "\n".join(confirm_lines),
                    "hint": "Choose Proceed to continue or Change selection to adjust the number of matches.",
                    "options": [
                        {"label": "Proceed", "value": "yes"},
                        {"label": "Change selection", "value": "no"},
                    ],
                    "default": "yes",
                },
            )
            .strip()
            .lower()
        )

        if not confirm_response:
            confirm_response = "yes"

        if confirm_response in yes_values:
            return match_ids

        if confirm_response in no_values:
            count = request_count(count, "Enter a new number of matches to include.")
            continue

        _notify("Please respond with yes or no.", progress_callback)


def _fetch_match_payloads(
    match_ids: Sequence[str], sleep_fn: Callable[[float], None], progress_callback: ProgressCallback
) -> Dict[str, Dict]:
    data_matches: Dict[str, Dict] = {}
    for match_id in match_ids:
        _notify(f"Fetching match {match_id}", progress_callback)
        sleep_fn(1)
        data_matches[match_id] = get_match_by_match_id(match_id, "esports")
    return data_matches


def _format_overall_sheet(
    sheet: gspread.Worksheet,
    team_context: TeamContext,
    image_url: str,
    basic_info: Dict,
    maps_stats: Dict[str, List],
    sleep_fn: Callable[[float], None],
) -> None:
    sheet.update_title("Overall")
    sheet.merge_cells("I4:J9")
    sheet.update([[f'=IMAGE("{image_url}")']], "I4", value_input_option="USER_ENTERED")
    format_cell_range(
        sheet,
        "I4",
        CellFormat(backgroundColor=Color(0, 0, 0), horizontalAlignment="CENTER"),
    )

    sheet.merge_cells("A1:G1")
    title = f"Analytical Report of {team_context.name}: Overall"
    sheet.update([[title]], "A1")
    sheet.merge_cells("A3:G3")
    sheet.update([["Matches Played"]], "A3")
    header_data = [
        [
            "Team",
            "Result",
            "",
            "Rival",
            "Map",
            f"{team_context.tag}'s DEF",
            f"{team_context.tag}'s ATK",
        ]
    ]
    sheet.update(range_name="A4:G4", values=header_data)
    sheet.merge_cells("B4:C4")

    format_cell_range(
        sheet,
        "A1",
        CellFormat(
            backgroundColor=Color(0.26, 0.26, 0.26),
            horizontalAlignment="LEFT",
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0), fontSize=14, bold=True
            ),
        ),
    )
    format_cell_range(
        sheet,
        "A3",
        CellFormat(
            backgroundColor=Color(0.36, 0.36, 0.36),
            horizontalAlignment="LEFT",
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0), fontSize=12, bold=True
            ),
        ),
    )
    format_cell_range(
        sheet,
        "A4:G4",
        CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),
            horizontalAlignment="LEFT",
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0), fontSize=10, bold=True
            ),
        ),
    )

    matches_data = []
    for i, match_id in enumerate(basic_info["matches"]):
        match = basic_info["matches"][match_id]
        match_row = [
            basic_info["team"],
            match["result"][1][0],
            match["result"][1][1],
            match["rival"],
            match["map"],
            f"{int(100 * match['result'][2][0] / match['result'][3][0])}% ({match['result'][2][0]}/{match['result'][3][0]})",
            f"{int(100 * match['result'][2][1] / match['result'][3][1])}% ({match['result'][2][1]}/{match['result'][3][1]})",
        ]
        matches_data.append(match_row)

        if match["result"][0] == "Win":
            maps_stats.setdefault(match["map"], [[0, 0], [0, 0], [0, 0], []])[0][0] += 1
        else:
            maps_stats.setdefault(match["map"], [[0, 0], [0, 0], [0, 0], []])[0][1] += 1
        maps_stats[match["map"]][1][0] += match["result"][2][0]
        maps_stats[match["map"]][1][1] += match["result"][3][0]
        maps_stats[match["map"]][2][0] += match["result"][2][1]
        maps_stats[match["map"]][2][1] += match["result"][3][1]
        maps_stats[match["map"]][3].append(match["match_id"])

        def_color = (
            Color(0.204, 0.659, 0.325)
            if match["result"][1][0] >= match["result"][1][1]
            else Color(1.0, 0, 0)
        )
        format_cell_range(
            sheet,
            f"B{5+i}",
            CellFormat(
                backgroundColor=Color(0.047, 0.204, 0.239),
                horizontalAlignment="RIGHT",
                textFormat=TextFormat(
                    foregroundColor=def_color, fontSize=9, bold=False
                ),
            ),
        )

        for col_offset, idx in enumerate((0, 1)):
            pct = int(100 * match["result"][2][idx] / match["result"][3][idx])
            color = (
                Color(0.204, 0.659, 0.325)
                if pct > 50
                else Color(1.0, 0, 0)
                if pct < 50
                else Color(0.984, 0.737, 0.016)
            )
            format_cell_range(
                sheet,
                f"{chr(ord('F') + col_offset)}{5+i}",
                CellFormat(
                    backgroundColor=Color(0.047, 0.204, 0.239),
                    horizontalAlignment="LEFT",
                    textFormat=TextFormat(
                        foregroundColor=color, fontSize=9, bold=False
                    ),
                ),
            )
        sleep_fn(2)

    sheet.update(
        range_name=f"A5:G{4+len(matches_data)}", values=matches_data
    )
    format_cell_range(
        sheet,
        f"A5:G{4+len(matches_data)}",
        CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),
            horizontalAlignment="LEFT",
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0), fontSize=9, bold=False
            ),
        ),
    )
    format_cell_range(
        sheet,
        "B4",
        CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),
            horizontalAlignment="CENTER",
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0), fontSize=10, bold=True
            ),
        ),
    )


def _format_map_summary(
    sheet: gspread.Worksheet,
    start_row: int,
    maps_stats: Dict[str, List],
    map_performance_data: List[List[str]],
    sleep_fn: Callable[[float], None],
) -> int:
    sheet.merge_cells(f"A{start_row}:G{start_row}")
    sheet.update([["Performance by Map"]], f"A{start_row}")
    sheet.update(
        [["Map", "Won", "Lost", "Winrate", "DEF Winrate", "ATK Winrate"]],
        f"A{start_row+1}:F{start_row+1}",
    )
    format_cell_range(
        sheet,
        f"A{start_row}",
        CellFormat(
            backgroundColor=Color(0.36, 0.36, 0.36),
            horizontalAlignment="LEFT",
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0), fontSize=12, bold=True
            ),
        ),
    )
    format_cell_range(
        sheet,
        f"A{start_row+1}:G{start_row+1}",
        CellFormat(
            backgroundColor=Color(0.047, 0.204, 0.239),
            horizontalAlignment="LEFT",
            textFormat=TextFormat(
                foregroundColor=Color(1.0, 1.0, 1.0), fontSize=10, bold=True
            ),
        ),
    )

    if map_performance_data:
        sheet.update(
            range_name=f"A{start_row+2}:F{start_row+1+len(map_performance_data)}",
            values=map_performance_data,
            value_input_option="USER_ENTERED",
        )
        format_cell_range(
            sheet,
            f"A{start_row+2}:F{start_row+1+len(map_performance_data)}",
            CellFormat(
                backgroundColor=Color(0.047, 0.204, 0.239),
                horizontalAlignment="LEFT",
                textFormat=TextFormat(
                    foregroundColor=Color(1.0, 1.0, 1.0), fontSize=9, bold=False
                ),
            ),
        )

    map_names = list(maps_stats.keys())
    for idx, map_name in enumerate(map_names):
        total_wins = maps_stats[map_name][0][0]
        total_losses = maps_stats[map_name][0][1]
        def_wins, def_total = maps_stats[map_name][1]
        atk_wins, atk_total = maps_stats[map_name][2]

        winrate = int(100 * total_wins / (total_wins + total_losses)) if (total_wins + total_losses) else 0
        def_winrate = int(100 * def_wins / def_total) if def_total else 0
        atk_winrate = int(100 * atk_wins / atk_total) if atk_total else 0

        for col, pct in zip(("D", "E", "F"), (winrate, def_winrate, atk_winrate)):
            color = (
                Color(0.204, 0.659, 0.325)
                if pct > 50
                else Color(1.0, 0, 0)
                if pct < 50
                else Color(0.984, 0.737, 0.016)
            )
            format_cell_range(
                sheet,
                f"{col}{start_row+2+idx}",
                CellFormat(
                    backgroundColor=Color(0.047, 0.204, 0.239),
                    horizontalAlignment="LEFT",
                    textFormat=TextFormat(
                        foregroundColor=color, fontSize=9, bold=False
                    ),
                ),
            )
        sleep_fn(0.1)

    return start_row + 2 + len(map_performance_data)


def _populate_map_tabs(
    spreadsheet: gspread.Spreadsheet,
    team_context: TeamContext,
    maps_stats: Dict[str, List],
    data_matches: Dict[str, Dict],
    basic_info: Dict,
    creds: ServiceAccountCredentials,
    sleep_fn: Callable[[float], None],
    progress_callback: ProgressCallback,
) -> List[List[str]]:
    map_performance_data: List[List[str]] = []
    letters = string.ascii_uppercase

    for index, map_name in enumerate(maps_stats):
        _notify(f"Creating worksheet for map {map_name}", progress_callback)
        total_wins = maps_stats[map_name][0][0]
        total_losses = maps_stats[map_name][0][1]
        def_wins, def_total = maps_stats[map_name][1]
        atk_wins, atk_total = maps_stats[map_name][2]
        winrate = int(100 * total_wins / (total_wins + total_losses)) if (total_wins + total_losses) else 0
        def_winrate = int(100 * def_wins / def_total) if def_total else 0
        atk_winrate = int(100 * atk_wins / atk_total) if atk_total else 0

        map_sheet = spreadsheet.add_worksheet(title=map_name, rows="120", cols="20")
        map_performance_data.append(
            [
                f'=HYPERLINK("#gid={map_sheet.id}", "{map_name}")',
                total_wins,
                total_losses,
                f"{winrate}%",
                f"{def_winrate}%",
                f"{atk_winrate}%",
            ]
        )

        title = f"Analytical Report of {team_context.name}: {map_name}"
        map_sheet.update([[title]], "A1")
        map_sheet.update([["Agent Compositions"]], "A3")
        compositions = get_comps(team_context.tag, maps_stats[map_name][3])
        all_players = set()
        for comp in compositions.values():
            for match in comp:
                all_players.update(
                    player.split()[1] for player in match["player_agent_mapping"].keys()
                )
        all_players = list(sorted(all_players))

        header_data = [["Picks"] + all_players + ["Winrate"]]
        map_sheet.merge_cells(f"A1:{letters[len(all_players)+1]}1")
        map_sheet.merge_cells(f"A3:{letters[len(all_players)+1]}3")
        map_sheet.update(
            range_name=f"A4:{letters[len(all_players)+1]}4", values=header_data
        )

        final: List[List[str]] = []
        for comp in compositions:
            row: List[str] = [len(compositions[comp])]
            for player in all_players:
                player_full = f"{team_context.tag} {player}"
                if player_full not in compositions[comp][0]["player_agent_mapping"]:
                    row.append(" ")
                else:
                    row.append(
                        compositions[comp][0]["player_agent_mapping"][player_full]
                    )
            row.append(
                "{}%".format(
                    int(100 * compositions[comp][0]["win"] / len(compositions[comp]))
                )
            )
            final.append(row)

        if final:
            map_sheet.update(
                range_name=f"A5:{letters[len(all_players)+1]}{4+len(final)}",
                values=final,
            )

        header = [
            ["General post-plant performance", "", "", "", ""],
            ["", "Attacking", "", "Defending", ""],
            ["Site", "Times Planted", "post-plant WR", "Opp Planted", "retaking WR"],
        ]
        map_data = {map_id: data_matches[map_id] for map_id in maps_stats[map_name][3]}
        plant_performance = get_plants(map_data, basic_info)
        final_table = header + plant_performance
        offset = 6 + len(final)
        map_sheet.update(
            final_table,
            f"A{offset}:E{offset + len(final_table) - 1}",
        )
        map_sheet.merge_cells(f"A{offset}:E{offset}")
        map_sheet.merge_cells(f"B{offset+1}:C{offset+1}")
        map_sheet.merge_cells(f"D{offset+1}:E{offset+1}")

        pistol_header = [
            ["Pistol round post-plant performance", "", "", "", ""],
            ["", "Attacking", "", "Defending", ""],
            ["Site", "Times Planted", "post-plant WR", "Opp Planted", "retaking WR"],
        ]
        pistol_plant_performance = get_pistol_plants(map_data, basic_info)
        pistol_table = pistol_header + pistol_plant_performance
        map_sheet.update(
            pistol_table,
            f"G{offset}:K{offset + len(pistol_table) - 1}",
        )
        map_sheet.merge_cells(f"G{offset}:K{offset}")
        map_sheet.merge_cells(f"H{offset+1}:I{offset+1}")
        map_sheet.merge_cells(f"J{offset+1}:K{offset+1}")

        def_pos_times = [10, 20, 30]
        for idx_time, seconds in enumerate(def_pos_times):
            path = f"plots/def_pos_{seconds}s.png"
            create_early_positioning(
                map_name,
                "def",
                seconds,
                maps_stats[map_name][3],
                map_data,
                basic_info,
                path,
            )
            def_pos_link = get_image_link(f"def_pos_{seconds}s.png", path, creds)
            col = ("A", "E", "I")[idx_time]
            map_sheet.update(
                [[f'=IMAGE("https://drive.google.com/uc?id={def_pos_link}")']],
                f"{col}{offset+5}",
                value_input_option="USER_ENTERED",
            )
            map_sheet.merge_cells(
                f"{col}{offset+5}:{chr(ord(col)+3)}{offset+21}"
            )

        get_sniper_kills(
            map_name, "def", maps_stats[map_name][3], map_data, basic_info, "plots/def_sniper.png"
        )
        def_sniper_link = get_image_link(
            "def_sniper.png", "plots/def_sniper.png", creds
        )
        def_sniper_row = offset + 23
        map_sheet.update(
            [["Defending sniper kills"]],
            f"A{def_sniper_row}",
        )
        map_sheet.merge_cells(f"A{def_sniper_row}:D{def_sniper_row}")
        map_sheet.update(
            [[f'=IMAGE("https://drive.google.com/uc?id={def_sniper_link}")']],
            f"A{def_sniper_row+1}",
            value_input_option="USER_ENTERED",
        )
        map_sheet.merge_cells(
            f"A{def_sniper_row+1}:D{def_sniper_row+17}"
        )

        _notify("Atk positioning loading", progress_callback)

        atk_pos_times = [10, 20, 30]
        atk_offset = def_sniper_row + 20
        map_sheet.update(
            [["Attacking early team positioning"]],
            f"A{atk_offset}",
        )
        map_sheet.merge_cells(f"A{atk_offset}:L{atk_offset}")
        for idx_time, seconds in enumerate(atk_pos_times):
            path = f"plots/atk_pos_{seconds}s.png"
            create_early_positioning(
                map_name,
                "atk",
                seconds,
                maps_stats[map_name][3],
                map_data,
                basic_info,
                path,
            )
            atk_pos_link = get_image_link(f"atk_pos_{seconds}s.png", path, creds)
            col = ("A", "E", "I")[idx_time]
            map_sheet.update(
                [[f'=IMAGE("https://drive.google.com/uc?id={atk_pos_link}")']],
                f"{col}{atk_offset+1}",
                value_input_option="USER_ENTERED",
            )
            map_sheet.merge_cells(
                f"{col}{atk_offset+1}:{chr(ord(col)+3)}{atk_offset+17}"
            )

        atk_sniper_row = atk_offset + 19
        map_sheet.update(
            [["Attacking sniper kills"]],
            f"A{atk_sniper_row}",
        )
        map_sheet.merge_cells(f"A{atk_sniper_row}:D{atk_sniper_row}")
        get_sniper_kills(
            map_name, "atk", maps_stats[map_name][3], map_data, basic_info, "plots/atk_sniper.png"
        )
        atk_sniper_link = get_image_link(
            "atk_sniper.png", "plots/atk_sniper.png", creds
        )
        map_sheet.update(
            [[f'=IMAGE("https://drive.google.com/uc?id={atk_sniper_link}")']],
            f"A{atk_sniper_row+1}",
            value_input_option="USER_ENTERED",
        )
        map_sheet.merge_cells(
            f"A{atk_sniper_row+1}:D{atk_sniper_row+17}"
        )

        format_cell_range(
            map_sheet,
            "A1",
            CellFormat(
                backgroundColor=Color(0.26, 0.26, 0.26),
                horizontalAlignment="LEFT",
                textFormat=TextFormat(
                    foregroundColor=Color(1.0, 1.0, 1.0), fontSize=14, bold=True
                ),
            ),
        )
        format_cell_range(
            map_sheet,
            "A3",
            CellFormat(
                backgroundColor=Color(0.36, 0.36, 0.36),
                horizontalAlignment="LEFT",
                textFormat=TextFormat(
                    foregroundColor=Color(1.0, 1.0, 1.0), fontSize=12, bold=True
                ),
            ),
        )
        format_cell_range(
            map_sheet,
            f"A4:{letters[len(all_players)+1]}4",
            CellFormat(
                backgroundColor=Color(0.047, 0.204, 0.239),
                horizontalAlignment="LEFT",
                textFormat=TextFormat(
                    foregroundColor=Color(1.0, 1.0, 1.0), fontSize=10, bold=True
                ),
            ),
        )

        if final:
            format_cell_range(
                map_sheet,
                f"A5:{letters[len(all_players)+1]}{4+len(final)}",
                CellFormat(
                    backgroundColor=Color(0.047, 0.204, 0.239),
                    horizontalAlignment="LEFT",
                    textFormat=TextFormat(
                        foregroundColor=Color(1.0, 1.0, 1.0), fontSize=9, bold=False
                    ),
                ),
            )

        for header_row in (
            offset,
            offset + 3 + len(plant_performance),
            atk_offset,
            def_sniper_row,
            atk_sniper_row,
        ):
            format_cell_range(
                map_sheet,
                f"A{header_row}:L{header_row}",
                CellFormat(
                    backgroundColor=Color(0.36, 0.36, 0.36),
                    horizontalAlignment="LEFT",
                    textFormat=TextFormat(
                        foregroundColor=Color(1.0, 1.0, 1.0),
                        fontSize=12,
                        bold=True,
                    ),
                ),
            )

        sleep_fn(3)

    return map_performance_data


def generate_analytical_report(
    team_code: str,
    match_count: Optional[int] = None,
    *,
    share_email: Optional[str] = None,
    spreadsheet_title: Optional[str] = None,
    credentials_path: Optional[str] = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    progress_callback: ProgressCallback = None,
    prompt_handler: PromptHandler = None,
) -> Dict[str, str]:
    progress_callback = progress_callback or (lambda msg: print(msg))
    team_context = _resolve_team(team_code)
    match_ids = _pick_matches(
        team_context,
        match_count,
        prompt_handler=prompt_handler,
        sleep_fn=sleep_fn,
        progress_callback=progress_callback,
    )

    creds, client = _authorize(credentials_path)
    drive_service, sheets_service = _build_google_services(creds)
    spreadsheet = client.create(spreadsheet_title or DEFAULT_SPREADSHEET_TITLE)
    sheet = spreadsheet.sheet1

    image_url = f"https://imagedelivery.net/WUSOKAY-iA_QQPngCXgUJg/{team_context.image_id}/w=10000"
    _notify("Preparing match data…", progress_callback)
    data_matches = _fetch_match_payloads(match_ids, sleep_fn, progress_callback)
    basic_info = get_basic_info(team_context.tag, "all", data_matches)

    _notify(f"Basic info gathered. Building {team_context.name} report.", progress_callback)
    maps_stats: Dict[str, List] = {}
    _format_overall_sheet(sheet, team_context, image_url, basic_info, maps_stats, sleep_fn)

    _notify("Creating per-map tabs…", progress_callback)
    map_performance_data = _populate_map_tabs(
        spreadsheet,
        team_context,
        maps_stats,
        data_matches,
        basic_info,
        creds,
        sleep_fn,
        progress_callback,
    )
    if map_performance_data:
        summary_start = 6 + len(basic_info["matches"])
        _format_map_summary(sheet, summary_start, maps_stats, map_performance_data, sleep_fn)

    recipient = share_email or DEFAULT_SHARE_EMAIL
    if recipient:
        spreadsheet.share(recipient, perm_type="user", role="writer")

    publish_url = spreadsheet.url
    try:
        _notify("Publishing spreadsheet for embedding…", progress_callback)
        _ensure_published(drive_service, spreadsheet.id)
        gid = _get_gid(sheets_service, spreadsheet.id, sheet.title)
        publish_url = _build_publish_url(spreadsheet.id, gid)
    except AnalyticalReportError as exc:
        _notify(f"Unable to publish spreadsheet automatically: {exc}", progress_callback)
    else:
        _notify("Spreadsheet published and ready to embed.", progress_callback)

    _notify(f"Spreadsheet created! URL: {publish_url}", progress_callback)
    return {
        "spreadsheet_url": publish_url,
        "spreadsheet_edit_url": spreadsheet.url,
        "spreadsheet_id": spreadsheet.id,
        "team_tag": team_context.tag,
        "team_name": team_context.name,
        "match_count": str(len(match_ids)),
    }


def _console_prompt_handler(spec: Dict[str, Any]) -> str:
    title = spec.get("title")
    message = spec.get("message")
    hint = spec.get("hint")
    options = spec.get("options") or []
    default = spec.get("default")

    if title:
        print(f"\n{title}")
    if message:
        print(message)
    if options:
        option_labels = []
        for option in options:
            if isinstance(option, dict):
                label = option.get("label") or option.get("value") or ""
            else:
                label = str(option)
            option_labels.append(label)
        if option_labels:
            print("Options:", ", ".join(option_labels))
    if hint:
        print(hint)

    prompt = "> "
    if default is not None:
        prompt = f"[default: {default}] > "

    try:
        response = input(prompt)
    except KeyboardInterrupt as exc:
        print()
        raise AnalyticalReportError("Prompt cancelled by user.") from exc

    response = response.strip()
    if not response and default is not None:
        return str(default)
    return response


def _cli() -> int:
    try:
        team = input("Enter team to analyze (e.g. TH): ").strip().upper()
        count_str = input("How many most-recent matches to include? (Leave blank for all): ").strip()
        count = int(count_str) if count_str else None
        generate_analytical_report(team, count, prompt_handler=_console_prompt_handler)
        return 0
    except AnalyticalReportError as exc:
        print(f"Error while generating report: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nAborted by user.")
        return 1


if __name__ == "__main__":
    sys.exit(_cli())
