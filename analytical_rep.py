import os
import string
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence
from uuid import uuid4
from datetime import datetime

import gspread
from gspread.exceptions import APIError
from gspread_formatting import CellFormat, Color, TextFormat, format_cell_range as _format_cell_range_raw
from gspread.utils import a1_to_rowcol
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
import urllib.error
import urllib.request

from functions.functions import (
    _summarize_match,
    create_early_positioning,
    get_basic_info,
    get_comps,
    get_image_link,
    get_match_by_match_id,
    get_matchlist_by_puuid,
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


class WorksheetUpdateBuffer:
    """Aggregate worksheet value updates to reduce API write calls."""

    def __init__(self, worksheet: gspread.Worksheet, *, max_batch_size: int = 30):
        self._worksheet = worksheet
        self._max = max_batch_size
        self._buffers: Dict[str, List[Dict[str, Any]]] = {
            "RAW": [],
            "USER_ENTERED": [],
        }

    def __getattr__(self, name: str) -> Any:
        return getattr(self._worksheet, name)

    def update(self, *args, **kwargs) -> None:
        range_name = kwargs.pop("range_name", None)
        values = kwargs.pop("values", None)

        if range_name is None or values is None:
            if len(args) == 2:
                first, second = args
                if isinstance(first, str):
                    range_name, values = first, second
                else:
                    values, range_name = first, second
            elif len(args) == 1 and values is not None:
                range_name = args[0]
            else:
                raise ValueError("Expected a cell range and values for update().")

        if range_name is None or values is None:
            raise ValueError("Both range_name and values must be provided.")

        value_input_option = kwargs.pop("value_input_option", "RAW")
        value_input_option = str(value_input_option or "RAW").upper()
        target_key = "USER_ENTERED" if value_input_option == "USER_ENTERED" else "RAW"

        # Ignore unsupported kwargs for now.
        kwargs.clear()

        target_buffer = self._buffers[target_key]
        target_buffer.append({"range": range_name, "values": values})
        if len(target_buffer) >= self._max:
            self._flush_option(target_key)

    def flush(self) -> None:
        self._flush_option("RAW")
        self._flush_option("USER_ENTERED")

    def _flush_option(self, key: str) -> None:
        buffer = self._buffers[key]
        if not buffer:
            return
        raw = key == "RAW"
        payload = [{"range": item["range"], "values": item["values"]} for item in buffer]
        sheet_title = getattr(self._worksheet, "title", "worksheet")
        _execute_with_retry(
            lambda: self._worksheet.batch_update(payload, raw=raw),
            description=f"Batch updating {len(payload)} range(s) on {sheet_title}",
        )
        buffer.clear()

DEFAULT_SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
DEFAULT_CREDENTIALS_PATH = os.getenv(
    "ANALYTICAL_REPORT_CREDENTIALS", "api_keys/valorant-sheets-credentials.json"
)
DEFAULT_SHARE_EMAIL = os.getenv("ANALYTICAL_REPORT_SHARE_EMAIL", "pablolopezarauzo@gmail.com")
DEFAULT_SPREADSHEET_TITLE = "New Analysis Report"


class AnalyticalReportError(Exception):
    """Raised when the analytical report cannot be generated."""


def _safe_float_env(var_name: str, default: float) -> float:
    value = os.getenv(var_name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return parsed if parsed >= 0 else default


def _safe_int_env(var_name: str, default: int) -> int:
    value = os.getenv(var_name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed >= 0 else default


_WRITE_THROTTLE_SECONDS = _safe_float_env("GOOGLE_SHEETS_WRITE_THROTTLE_SECONDS", 1.0)
_WRITE_BACKOFF_SECONDS = max(0.5, _safe_float_env("GOOGLE_SHEETS_BACKOFF_SECONDS", 5.0))
_WRITE_MAX_RETRIES = _safe_int_env("GOOGLE_SHEETS_MAX_RETRIES", 6)

_SLEEP_FN: Callable[[float], None] = time.sleep
_LAST_WRITE_TS: float = 0.0


def _set_sleep_fn(sleep_fn: Callable[[float], None]) -> None:
    global _SLEEP_FN
    _SLEEP_FN = sleep_fn or time.sleep


def _sleep(duration: float) -> None:
    if duration <= 0:
        return
    try:
        _SLEEP_FN(duration)
    except Exception:
        # Fallback to time.sleep if a custom sleeper fails.
        time.sleep(duration)


def _throttle_writes() -> None:
    if _WRITE_THROTTLE_SECONDS <= 0:
        return
    global _LAST_WRITE_TS
    now = time.monotonic()
    wait_time = _WRITE_THROTTLE_SECONDS - (now - _LAST_WRITE_TS)
    if wait_time > 0:
        _sleep(wait_time)
        now = time.monotonic()
    _LAST_WRITE_TS = now


def _extract_api_status(error: APIError) -> Optional[int]:
    response = getattr(error, "response", None)
    if response is not None:
        status = getattr(response, "status_code", None)
        if status is not None:
            try:
                return int(status)
            except (TypeError, ValueError):
                return None
    try:
        payload = error.args[0]
        if isinstance(payload, dict):
            code = payload.get("code")
            if code is not None:
                return int(code)
    except (IndexError, ValueError, TypeError):  # pragma: no cover - defensive
        return None
    return None


def _execute_with_retry(operation: Callable[[], Any], *, description: str) -> Any:
    attempt = 0
    while True:
        _throttle_writes()
        try:
            return operation()
        except APIError as exc:
            status_code = _extract_api_status(exc)
            if status_code == 429 and attempt < _WRITE_MAX_RETRIES:
                backoff = min(60.0, _WRITE_BACKOFF_SECONDS * (2 ** attempt))
                _sleep(backoff)
                attempt += 1
                continue
            raise AnalyticalReportError(f"{description} failed: {exc}") from exc


def format_cell_range(
    worksheet: gspread.Worksheet,
    range_name: str,
    cell_format: CellFormat,
    **kwargs: Any,
):
    label = range_name if isinstance(range_name, str) else kwargs.get("range_name", "range")
    title = getattr(worksheet, "title", "worksheet")
    return _execute_with_retry(
        lambda: _format_cell_range_raw(worksheet, range_name, cell_format, **kwargs),
        description=f"Formatting {label} on {title}",
    )


def _merge_cells(worksheet: gspread.Worksheet, range_name: str) -> None:
    title = getattr(worksheet, "title", "worksheet")
    _execute_with_retry(
        lambda: worksheet.merge_cells(range_name),
        description=f"Merging {range_name} on {title}",
    )


def _update_title(worksheet: gspread.Worksheet, title: str) -> None:
    current = getattr(worksheet, "title", "worksheet")
    _execute_with_retry(
        lambda: worksheet.update_title(title),
        description=f"Retitling worksheet {current!r} to {title!r}",
    )


def _add_worksheet(spreadsheet: gspread.Spreadsheet, *, title: str, rows: str, cols: str) -> gspread.Worksheet:
    return _execute_with_retry(
        lambda: spreadsheet.add_worksheet(title=title, rows=rows, cols=cols),
        description=f"Creating worksheet {title!r}",
    )


def _share_spreadsheet(spreadsheet: gspread.Spreadsheet, recipient: str) -> None:
    _execute_with_retry(
        lambda: spreadsheet.share(recipient, perm_type="user", role="writer"),
        description=f"Sharing spreadsheet with {recipient}",
    )


@dataclass
class TeamContext:
    tag: str
    name: str
    image_id: str
    player_riot_id: str
    matches: List[Dict]


ProgressCallback = Optional[Callable[[str], None]]
PromptHandler = Optional[Callable[[Dict[str, Any]], str]]


def _percent(value: int, total: int) -> Optional[int]:
    if total and total > 0:
        return int(round(100 * value / total))
    return None


def _rate(value: int, total: int) -> Dict[str, Optional[int]]:
    value = int(value or 0)
    total = int(total or 0)
    return {
        "value": _percent(value, total),
        "won": value,
        "total": total,
    }


def _count_share(count: int, total: int) -> Dict[str, Optional[int]]:
    count = int(count or 0)
    total = int(total or 0)
    return {
        "count": count,
        "share_pct": _percent(count, total),
    }


def _col_letter(index: int) -> str:
    """Convert a 1-based column index to its spreadsheet column letter."""
    if index <= 0:
        raise ValueError("Column index must be positive.")
    result = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _compute_post_plant_struct(
    map_data: Dict[str, Dict],
    basic_info: Dict[str, Any],
    *,
    pistol_only: bool = False,
) -> Dict[str, Any]:
    totals = {"plants": 0, "won_pp": 0, "opp_plants": 0, "won_retakes": 0}
    per_site: Dict[str, Dict[str, int]] = {}

    for match_id, match_payload in map_data.items():
        rounds = match_payload.get("roundResults") or []
        if pistol_only:
            candidate_indexes = []
            if rounds:
                candidate_indexes.append(0)
            if len(rounds) > 12:
                candidate_indexes.append(12)
        else:
            candidate_indexes = range(len(rounds))

        team_color = (basic_info.get("matches", {}).get(match_id) or {}).get("color")
        for idx in candidate_indexes:
            if idx >= len(rounds):
                continue
            round_data = rounds[idx]
            site = round_data.get("plantSite")
            if not site:
                continue
            planter = round_data.get("bombPlanter")
            winning_team = round_data.get("winningTeam")

            stats = per_site.setdefault(
                site, {"plants": 0, "won_pp": 0, "opp_plants": 0, "won_retakes": 0}
            )

            if planter and planter in basic_info.get("players", {}):
                stats["plants"] += 1
                totals["plants"] += 1
                if winning_team == team_color:
                    stats["won_pp"] += 1
                    totals["won_pp"] += 1
            elif planter:
                stats["opp_plants"] += 1
                totals["opp_plants"] += 1
                if winning_team == team_color:
                    stats["won_retakes"] += 1
                    totals["won_retakes"] += 1

    overall = {
        "team_plants": {"count": totals["plants"]},
        "post_plant": _rate(totals["won_pp"], totals["plants"]),
        "opponent_plants": {"count": totals["opp_plants"]},
        "retake_win": _rate(totals["won_retakes"], totals["opp_plants"]),
    }

    sites: List[Dict[str, Any]] = []
    for site, stats in sorted(per_site.items(), key=lambda item: item[0]):
        sites.append(
            {
                "site": site,
                "team_plants": _count_share(stats["plants"], totals["plants"]),
                "post_plant": _rate(stats["won_pp"], stats["plants"]),
                "opponent_plants": _count_share(stats["opp_plants"], totals["opp_plants"]),
                "retake_win": _rate(stats["won_retakes"], stats["opp_plants"]),
            }
        )

    return {"overall": overall, "sites": sites}


def _build_google_services(creds: ServiceAccountCredentials):
    """Create Drive and Sheets API clients from the service account credentials."""
    try:
        drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
        sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    except Exception as exc:  # pragma: no cover - network/service errors
        raise AnalyticalReportError(f"Failed to initialize Google APIs: {exc}") from exc
    return drive_service, sheets_service


def _ensure_public_view_permission(drive_service, spreadsheet_id: str) -> None:
    """Make sure the spreadsheet is viewable by anyone with the link."""
    try:
        permissions = (
            drive_service.permissions()
            .list(fileId=spreadsheet_id, fields="permissions(id,type,role)", supportsAllDrives=False)
            .execute()
            .get("permissions", [])
        )
    except Exception as exc:  # pragma: no cover
        raise AnalyticalReportError(f"Failed to read sharing settings for spreadsheet {spreadsheet_id}: {exc}") from exc

    for perm in permissions:
        if perm.get("type") == "anyone" and perm.get("role") in {"reader", "commenter", "writer"}:
            return

    try:
        drive_service.permissions().create(
            fileId=spreadsheet_id,
            body={"type": "anyone", "role": "reader", "allowFileDiscovery": False},
            fields="id",
            supportsAllDrives=False,
        ).execute()
    except HttpError as exc:  # pragma: no cover
        status = getattr(exc, "status_code", None)
        if status is None:
            resp = getattr(exc, "resp", None)
            status = getattr(resp, "status", None) if resp is not None else None
        if status in (400, 403):
            raise AnalyticalReportError(
                "Google Drive blocked public viewer access. Check your Workspace sharing settings."
            ) from exc
        raise AnalyticalReportError(f"Failed to update sharing for spreadsheet {spreadsheet_id}: {exc}") from exc
    except Exception as exc:  # pragma: no cover
        raise AnalyticalReportError(f"Failed to update sharing for spreadsheet {spreadsheet_id}: {exc}") from exc


def _add_image(
    sheets_service,
    spreadsheet_id: str,
    sheet_id: int,
    cell_a1: str,
    image_url: str,
    *,
    offset_x: int = 0,
    offset_y: int = 0,
) -> None:
    """Embed an image anchored to the given cell using the Sheets API."""
    row, col = a1_to_rowcol(cell_a1)
    request = {
        "addImage": {
            "imageObjectId": f"img_{uuid4().hex}",
            "url": image_url,
            "anchorCell": {
                "sheetId": int(sheet_id),
                "rowIndex": max(0, row - 1),
                "columnIndex": max(0, col - 1),
            },
            "offsetXPixels": offset_x,
            "offsetYPixels": offset_y,
        }
    }
    attempt = 0
    while True:
        _throttle_writes()
        try:
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [request]},
            ).execute()
            return
        except HttpError as exc:
            status = _extract_http_status(exc)
            if status == 429 and attempt < _WRITE_MAX_RETRIES:
                backoff = min(60.0, _WRITE_BACKOFF_SECONDS * (2 ** attempt))
                _sleep(backoff)
                attempt += 1
                continue
            raise AnalyticalReportError(
                f"Failed to insert image at {cell_a1}: {exc}"
            ) from exc


def _extract_http_status(error: HttpError) -> Optional[int]:
    status = getattr(error, "status_code", None)
    if status is not None:
        try:
            return int(status)
        except (TypeError, ValueError):
            return None
    resp = getattr(error, "resp", None)
    if resp is not None:
        status = getattr(resp, "status", None)
        if status is not None:
            try:
                return int(status)
            except (TypeError, ValueError):
                return None
    return None

def _ensure_public_asset(drive_service, file_id: str) -> None:
    """Grant 'anyone with the link -> reader' on the image asset."""
    try:
        perms = drive_service.permissions().list(
            fileId=file_id,
            fields="permissions(id,type,role)"
        ).execute().get("permissions", [])
        if any(p.get("type") == "anyone" and p.get("role") in {"reader", "commenter", "writer"} for p in perms):
            return
        drive_service.permissions().create(
            fileId=file_id,
            body={"type": "anyone", "role": "reader", "allowFileDiscovery": False},
            fields="id"
        ).execute()
    except HttpError as exc:
        raise AnalyticalReportError(f"Failed to open image {file_id} publicly: {exc}") from exc

def _drive_image_url(file_id: str) -> str:
    # Direct image host (no cookies, works in published embeds)
    # You can add sizing like '=s2048' if you wish.
    return f"https://lh3.googleusercontent.com/d/{file_id}"


def _ensure_published(drive_service, spreadsheet_id: str) -> str:
    """Publish the spreadsheet to the web and return the base published link."""
    try:
        revisions = (
            drive_service.revisions()
            .list(
                fileId=spreadsheet_id,
                fields="revisions(id,published,publishAuto,publishedOutsideDomain,publishedLink,exportLinks)",
            )
            .execute()
            .get("revisions", [])
        )
    except Exception as exc:  # pragma: no cover
        raise AnalyticalReportError(f"Failed to fetch revisions for spreadsheet {spreadsheet_id}: {exc}") from exc

    if not revisions:
        raise AnalyticalReportError(f"Spreadsheet {spreadsheet_id} has no revisions to publish.")

    last_revision = revisions[-1]
    if last_revision.get("published"):
        published_link = last_revision.get("publishedLink")
        if published_link:
            return published_link

    def _try_publish(body: Dict[str, Any]) -> None:
        try:
            drive_service.revisions().update(
                fileId=spreadsheet_id,
                revisionId=last_revision["id"],
                body=body,
            ).execute()
        except HttpError as exc:  # pragma: no cover
            status = _extract_http_status(exc)
            if status in (400, 403):
                raise AnalyticalReportError(
                    "Google Drive denied publish-to-web. Check your Workspace policies."
                ) from exc
            raise AnalyticalReportError(f"Failed to publish spreadsheet {spreadsheet_id}: {exc}") from exc
        except Exception as exc:  # pragma: no cover
            raise AnalyticalReportError(f"Failed to publish spreadsheet {spreadsheet_id}: {exc}") from exc

    try:
        _try_publish({"published": True, "publishAuto": True, "publishedOutsideDomain": True})
    except AnalyticalReportError as exc:
        # Retry without publishedOutsideDomain for stricter domains
        _try_publish({"published": True, "publishAuto": True})

    try:
        updated = (
            drive_service.revisions()
            .get(
                fileId=spreadsheet_id,
                revisionId=last_revision["id"],
                fields="publishedLink,exportLinks",
            )
            .execute()
        )
    except Exception as exc:  # pragma: no cover
        raise AnalyticalReportError(f"Failed to confirm publish state for spreadsheet {spreadsheet_id}: {exc}") from exc

    published_link = updated.get("publishedLink") or (updated.get("exportLinks") or {}).get("text/html")
    if published_link:
        return published_link

    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/pubhtml"


def _resolve_published_link(url: str, timeout: float = 8.0) -> str:
    if not url:
        return url
    try:
        request = urllib.request.Request(url, method="GET", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.geturl()
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, Exception):
        return url


def _compose_published_urls(published_link: str, spreadsheet_id: str, gid: Optional[int]) -> tuple[str, str, str]:
    if not published_link:
        published_link = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/pubhtml"

    resolved_link = _resolve_published_link(published_link)
    if resolved_link:
        published_link = resolved_link

    parsed = urlparse(published_link)
    if not parsed.scheme:
        published_link = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/pubhtml"
        parsed = urlparse(published_link)

    base_query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if gid is not None:
        base_query["gid"] = str(int(gid))

    def build_url(query: Dict[str, Any], path: Optional[str] = None) -> str:
        target_path = path if path is not None else parsed.path
        return urlunparse(parsed._replace(path=target_path, query=urlencode(query, doseq=True)))

    view_url = build_url(base_query)

    embed_query = dict(base_query)
    embed_query["widget"] = "true"
    embed_query["headers"] = "false"
    embed_url = build_url(embed_query)

    csv_query = dict(base_query)
    csv_query["output"] = "csv"
    csv_path = parsed.path
    if csv_path.endswith("/pubhtml"):
        csv_path = csv_path[:-4]  # strip "html" -> /pub
    csv_url = build_url(csv_query, path=csv_path)

    return embed_url, view_url, csv_url



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
    sheets_service,
    spreadsheet_id: str,
    team_context: TeamContext,
    image_url: str,
    basic_info: Dict,
    maps_stats: Dict[str, List],
    sleep_fn: Callable[[float], None],
) -> None:
    _update_title(sheet, "Overall")
    _merge_cells(sheet, "I4:J9")
    sheet_buffer = WorksheetUpdateBuffer(sheet)
    try:
        _add_image(sheets_service, spreadsheet_id, sheet.id, "I4", image_url)
    except Exception:
        sheet_buffer.update("I4", [[f'=IMAGE("{image_url}")']], value_input_option="USER_ENTERED")
    format_cell_range(
        sheet,
        "I4",
        CellFormat(backgroundColor=Color(0, 0, 0), horizontalAlignment="CENTER"),
    )

    _merge_cells(sheet, "A1:G1")
    title = f"Analytical Report of {team_context.name}: Overall"
    sheet_buffer.update("A1", [[title]])
    _merge_cells(sheet, "A3:G3")
    sheet_buffer.update("A3", [["Matches Played"]])
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
    sheet_buffer.update("A4:G4", header_data)
    _merge_cells(sheet, "B4:C4")

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

    sheet_buffer.update(
        f"A5:G{4+len(matches_data)}", matches_data
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

    sheet_buffer.flush()
    sleep_fn(1)


def _format_map_summary(
    sheet: gspread.Worksheet,
    start_row: int,
    maps_stats: Dict[str, List],
    map_performance_data: List[List[str]],
    sleep_fn: Callable[[float], None],
) -> int:
    _merge_cells(sheet, f"A{start_row}:G{start_row}")
    summary_buffer = WorksheetUpdateBuffer(sheet)
    summary_buffer.update(f"A{start_row}", [["Performance by Map"]])
    summary_buffer.update(
        f"A{start_row+1}:F{start_row+1}",
        [["Map", "Won", "Lost", "Winrate", "DEF Winrate", "ATK Winrate"]],
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
        summary_buffer.update(
            f"A{start_row+2}:F{start_row+1+len(map_performance_data)}",
            map_performance_data,
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
    summary_buffer.flush()
    sleep_fn(0.5)

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


def _build_report_payload(
    team_context: TeamContext,
    image_url: str,
    match_ids: Sequence[str],
    basic_info: Dict[str, Any],
    data_matches: Dict[str, Dict[str, Any]],
    map_details: List[Dict[str, Any]],
) -> Dict[str, Any]:
    matches_summary: List[Dict[str, Any]] = []
    for match_id in match_ids:
        info = basic_info["matches"].get(match_id)
        if not info:
            continue
        team_rounds, opponent_rounds = info["result"][1]
        defence_wins, attack_wins = info["result"][2]
        defence_total, attack_total = info["result"][3]
        match_payload = data_matches.get(match_id, {})
        match_info = match_payload.get("matchInfo", {}) if isinstance(match_payload, dict) else {}
        started_at = match_info.get("gameStartMillis")
        started_at_iso = None
        if started_at:
            try:
                started_at_iso = datetime.utcfromtimestamp(started_at / 1000).isoformat()
            except Exception:
                started_at_iso = None

        matches_summary.append(
            {
                "match_id": match_id,
                "map": info.get("map"),
                "opponent": info.get("rival"),
                "result": info.get("result", [""])[0],
                "score": {"team": int(team_rounds), "opponent": int(opponent_rounds)},
                "defence": _rate(defence_wins, defence_total),
                "attack": _rate(attack_wins, attack_total),
                "team_color": info.get("color"),
                "started_at": started_at_iso,
            }
        )

    match_lookup = {match["match_id"]: match for match in matches_summary}
    map_payloads: List[Dict[str, Any]] = []
    total_wins = 0
    total_losses = 0

    for detail in map_details:
        detail_copy = dict(detail)
        match_refs = []
        for match_id in detail_copy.pop("match_ids", []):
            match_entry = match_lookup.get(match_id)
            if match_entry:
                match_refs.append(match_entry)
        detail_copy["matches"] = match_refs
        map_payloads.append(detail_copy)
        total_wins += int(detail_copy.get("wins") or 0)
        total_losses += int(detail_copy.get("losses") or 0)

    payload = {
        "team": {
            "name": team_context.name,
            "tag": team_context.tag,
            "player_riot_id": team_context.player_riot_id,
            "image_url": image_url,
        },
        "summary": {
            "match_count": len(match_ids),
            "map_count": len(map_payloads),
            "record": {"wins": total_wins, "losses": total_losses},
        },
        "matches": matches_summary,
        "maps": map_payloads,
    }
    return payload


def _populate_map_tabs(
    spreadsheet: gspread.Spreadsheet,
    sheets_service,
    spreadsheet_id: str,
    team_context: TeamContext,
    maps_stats: Dict[str, List],
    data_matches: Dict[str, Dict],
    basic_info: Dict,
    creds: ServiceAccountCredentials,
    sleep_fn: Callable[[float], None],
    progress_callback: ProgressCallback,
    drive_service,
) -> tuple[List[List[str]], List[Dict[str, Any]]]:
    map_performance_data: List[List[str]] = []
    map_details: List[Dict[str, Any]] = []
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

        map_sheet = _add_worksheet(spreadsheet, title=map_name, rows="120", cols="20")
        map_buffer = WorksheetUpdateBuffer(map_sheet)
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

        map_detail: Dict[str, Any] = {
            "name": map_name,
            "wins": total_wins,
            "losses": total_losses,
            "winrate": _percent(total_wins, total_wins + total_losses),
            "defence": _rate(def_wins, def_total),
            "attack": _rate(atk_wins, atk_total),
            "match_ids": list(maps_stats[map_name][3]),
        }

        title = f"Analytical Report of {team_context.name}: {map_name}"
        map_buffer.update("A1", [[title]])
        map_buffer.update("A3", [["Agent Compositions"]])
        compositions = get_comps(team_context.tag, maps_stats[map_name][3])
        all_players = set()
        for comp in compositions.values():
            for match in comp:
                all_players.update(
                    player.split()[1] for player in match["player_agent_mapping"].keys()
                )
        all_players = list(sorted(all_players))

        header_data = [["Picks"] + all_players + ["Winrate"]]
        _merge_cells(map_sheet, f"A1:{letters[len(all_players)+1]}1")
        _merge_cells(map_sheet, f"A3:{letters[len(all_players)+1]}3")
        map_buffer.update(
            f"A4:{letters[len(all_players)+1]}4", header_data
        )

        final: List[List[str]] = []
        compositions_struct: List[Dict[str, Any]] = []
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

            matches_played = len(compositions[comp])
            wins_count = sum(1 for appearance in compositions[comp] if appearance.get("win"))
            losses_count = matches_played - wins_count
            compositions_struct.append(
                {
                    "agents": list(comp),
                    "played": matches_played,
                    "wins": wins_count,
                    "losses": losses_count,
                    "winrate": _percent(wins_count, matches_played),
                    "player_agents": compositions[comp][0].get("player_agent_mapping", {}),
                }
            )

        if final:
            map_buffer.update(
                f"A5:{letters[len(all_players)+1]}{4+len(final)}",
                final,
            )

        map_data = {map_id: data_matches[map_id] for map_id in maps_stats[map_name][3]}
        post_plants_struct = _compute_post_plant_struct(map_data, basic_info, pistol_only=False)
        pistol_struct = _compute_post_plant_struct(map_data, basic_info, pistol_only=True)

        def format_count(entry: Optional[Dict[str, Any]]) -> str:
            if not entry:
                return "0"
            count = entry.get("count") or 0
            share = entry.get("share_pct")
            return f"{count} ({share}%)" if share is not None else f"{count}"

        def format_rate(rate: Optional[Dict[str, Any]]) -> str:
            if not rate or rate.get("value") is None or not rate.get("total"):
                return "—"
            return f"{rate['value']}% ({rate['won']}/{rate['total']})"

        def write_combined_postplant_section(
            general_struct: Dict[str, Any],
            pistol_struct: Dict[str, Any],
            *,
            start_col: int,
            current_row_ref: List[int],
        ) -> int:
            title_row = current_row_ref[0]
            row = title_row
            end_col = start_col + 8
            start_letter = _col_letter(start_col)
            end_letter = _col_letter(end_col)

            title_range = f"{start_letter}{row}:{end_letter}{row}"
            map_buffer.update(title_range, [["Post-plant performance overview"]])
            _merge_cells(map_sheet, title_range)
            format_cell_range(
                map_sheet,
                title_range,
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
            row += 1

            group_row = row
            group_headers = [[
                "Site",
                "General post-plant",
                "",
                "",
                "",
                "Pistol round post-plant",
                "",
                "",
                "",
            ]]
            map_buffer.update(f"{start_letter}{group_row}:{end_letter}{group_row}", group_headers)
            _merge_cells(
                map_sheet,
                f"{_col_letter(start_col+1)}{group_row}:{_col_letter(start_col+4)}{group_row}"
            )
            _merge_cells(
                map_sheet,
                f"{_col_letter(start_col+5)}{group_row}:{end_letter}{group_row}"
            )
            format_cell_range(
                map_sheet,
                f"{start_letter}{group_row}:{start_letter}{group_row}",
                CellFormat(
                    backgroundColor=Color(0.26, 0.26, 0.26),
                    horizontalAlignment="CENTER",
                    textFormat=TextFormat(
                        foregroundColor=Color(1.0, 1.0, 1.0),
                        fontSize=10,
                        bold=True,
                    ),
                ),
            )
            format_cell_range(
                map_sheet,
                f"{_col_letter(start_col+1)}{group_row}:{_col_letter(start_col+4)}{group_row}",
                CellFormat(
                    backgroundColor=Color(0.047, 0.204, 0.239),
                    horizontalAlignment="CENTER",
                    textFormat=TextFormat(
                        foregroundColor=Color(1.0, 1.0, 1.0),
                        fontSize=10,
                        bold=True,
                    ),
                ),
            )
            format_cell_range(
                map_sheet,
                f"{_col_letter(start_col+5)}{group_row}:{end_letter}{group_row}",
                CellFormat(
                    backgroundColor=Color(0.976, 0.451, 0.086),
                    horizontalAlignment="CENTER",
                    textFormat=TextFormat(
                        foregroundColor=Color(1.0, 1.0, 1.0),
                        fontSize=10,
                        bold=True,
                    ),
                ),
            )
            row += 1

            detail_headers = [[
                "Site",
                "Team plants",
                "Post-plant WR",
                "Opp plants",
                "Retake WR",
                "Team plants",
                "Post-plant WR",
                "Opp plants",
                "Retake WR",
            ]]
            map_buffer.update(f"{start_letter}{row}:{end_letter}{row}", detail_headers)
            format_cell_range(
                map_sheet,
                f"{start_letter}{row}:{start_letter}{row}",
                CellFormat(
                    backgroundColor=Color(0.26, 0.26, 0.26),
                    horizontalAlignment="LEFT",
                    textFormat=TextFormat(
                        foregroundColor=Color(1.0, 1.0, 1.0),
                        fontSize=9,
                        bold=True,
                    ),
                ),
            )
            format_cell_range(
                map_sheet,
                f"{_col_letter(start_col+1)}{row}:{_col_letter(start_col+4)}{row}",
                CellFormat(
                    backgroundColor=Color(0.047, 0.204, 0.239),
                    horizontalAlignment="LEFT",
                    textFormat=TextFormat(
                        foregroundColor=Color(1.0, 1.0, 1.0),
                        fontSize=9,
                        bold=True,
                    ),
                ),
            )
            format_cell_range(
                map_sheet,
                f"{_col_letter(start_col+5)}{row}:{end_letter}{row}",
                CellFormat(
                    backgroundColor=Color(0.976, 0.451, 0.086),
                    horizontalAlignment="LEFT",
                    textFormat=TextFormat(
                        foregroundColor=Color(1.0, 1.0, 1.0),
                        fontSize=9,
                        bold=True,
                    ),
                ),
            )
            header_row = row
            row += 1

            def _sites_map(struct: Dict[str, Any]) -> Dict[str, Any]:
                lookup: Dict[str, Any] = {}
                for site in struct.get("sites", []) or []:
                    name = site.get("site") or "—"
                    if name not in lookup:
                        lookup[name] = site
                return lookup

            table_rows: List[List[str]] = []
            general_overall = general_struct.get("overall") or {}
            pistol_overall = pistol_struct.get("overall") or {}
            table_rows.append(
                [
                    "All",
                    format_count(general_overall.get("team_plants")),
                    format_rate(general_overall.get("post_plant")),
                    format_count(general_overall.get("opponent_plants")),
                    format_rate(general_overall.get("retake_win")),
                    format_count(pistol_overall.get("team_plants")),
                    format_rate(pistol_overall.get("post_plant")),
                    format_count(pistol_overall.get("opponent_plants")),
                    format_rate(pistol_overall.get("retake_win")),
                ]
            )

            general_sites = _sites_map(general_struct or {})
            pistol_sites = _sites_map(pistol_struct or {})
            site_order: List[str] = []
            for name in general_sites.keys():
                if name not in site_order:
                    site_order.append(name)
            for name in pistol_sites.keys():
                if name not in site_order:
                    site_order.append(name)

            for name in site_order:
                general_site = general_sites.get(name) or {}
                pistol_site = pistol_sites.get(name) or {}
                table_rows.append(
                    [
                        name,
                        format_count(general_site.get("team_plants")),
                        format_rate(general_site.get("post_plant")),
                        format_count(general_site.get("opponent_plants")),
                        format_rate(general_site.get("retake_win")),
                        format_count(pistol_site.get("team_plants")),
                        format_rate(pistol_site.get("post_plant")),
                        format_count(pistol_site.get("opponent_plants")),
                        format_rate(pistol_site.get("retake_win")),
                    ]
                )

            body_end_row = header_row
            if table_rows:
                body_end_row = row + len(table_rows) - 1
                body_range = f"{start_letter}{row}:{end_letter}{body_end_row}"
                map_buffer.update(body_range, table_rows)
                format_cell_range(
                    map_sheet,
                    body_range,
                    CellFormat(
                        horizontalAlignment="LEFT",
                        textFormat=TextFormat(
                            foregroundColor=Color(1.0, 1.0, 1.0),
                            fontSize=9,
                            bold=False,
                        ),
                    ),
                )
                format_cell_range(
                    map_sheet,
                    f"{_col_letter(start_col+1)}{row}:{_col_letter(start_col+4)}{body_end_row}",
                    CellFormat(backgroundColor=Color(0.047, 0.204, 0.239)),
                )
                format_cell_range(
                    map_sheet,
                    f"{_col_letter(start_col+5)}{row}:{end_letter}{body_end_row}",
                    CellFormat(backgroundColor=Color(0.976, 0.451, 0.086)),
                )
                format_cell_range(
                    map_sheet,
                    f"{start_letter}{row}:{start_letter}{body_end_row}",
                    CellFormat(
                        backgroundColor=Color(0.26, 0.26, 0.26),
                        textFormat=TextFormat(
                            foregroundColor=Color(1.0, 1.0, 1.0),
                            fontSize=9,
                            bold=False,
                        ),
                    ),
                )
            current_row_ref[0] = body_end_row + 2
            return title_row

        current_row = [6 + (len(final) if final else 0) + 2]
        postplant_section_row = write_combined_postplant_section(
            post_plants_struct,
            pistol_struct,
            start_col=1,
            current_row_ref=current_row,
        )

        def_pos_times = [10, 20, 30]
        defence_positions: List[Dict[str, Any]] = []
        defence_heading_row = current_row[0]
        defence_heading_range = f"A{defence_heading_row}:L{defence_heading_row}"
        map_buffer.update(defence_heading_range, [["Defensive early team positioning"]])
        _merge_cells(map_sheet, defence_heading_range)
        format_cell_range(
            map_sheet,
            defence_heading_range,
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
        current_row[0] += 1
        image_start_row = current_row[0]
        image_height = 18
        col_starts = [1, 5, 9]  # columns A, E, I
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
            _ensure_public_asset(drive_service, def_pos_link)
            image_url = _drive_image_url(def_pos_link)
            defence_positions.append({"seconds": seconds, "image_url": image_url})
            start_col_idx = col_starts[idx_time]
            end_col_idx = start_col_idx + 3
            start_letter = _col_letter(start_col_idx)
            end_letter = _col_letter(end_col_idx)
            target_cell = f"{start_letter}{image_start_row}"
            try:
                _add_image(
                    sheets_service,
                    spreadsheet_id,
                    map_sheet.id,
                    target_cell,
                    image_url,
                )
            except Exception:
                map_buffer.update(
                    target_cell,
                    [[f'=IMAGE("{image_url}")']],
                    value_input_option="USER_ENTERED",
                )
            _merge_cells(
                map_sheet,
                f"{start_letter}{image_start_row}:{end_letter}{image_start_row + image_height - 1}"
            )

        current_row[0] = image_start_row + image_height + 2

        get_sniper_kills(
            map_name, "def", maps_stats[map_name][3], map_data, basic_info, "plots/def_sniper.png"
        )
        def_sniper_link = get_image_link(
            "def_sniper.png", "plots/def_sniper.png", creds
        )
        _ensure_public_asset(drive_service, def_sniper_link)
        def_sniper_url = _drive_image_url(def_sniper_link)
        sniper_height = 16
        def_sniper_row = current_row[0]
        map_buffer.update(
            f"A{def_sniper_row}",
            [["Defending sniper kills"]],
        )
        _merge_cells(map_sheet, f"A{def_sniper_row}:D{def_sniper_row}")
        format_cell_range(
            map_sheet,
            f"A{def_sniper_row}:D{def_sniper_row}",
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
        try:
            _add_image(
                sheets_service,
                spreadsheet_id,
                map_sheet.id,
                f"A{def_sniper_row+1}",
                def_sniper_url,
            )
        except Exception:
            map_buffer.update(
                f"A{def_sniper_row+1}",
                [[f'=IMAGE("{def_sniper_url}")']],
                value_input_option="USER_ENTERED",
            )
        _merge_cells(
            map_sheet,
            f"A{def_sniper_row+1}:D{def_sniper_row+sniper_height}"
        )

        _notify("Atk positioning loading", progress_callback)

        atk_pos_times = [10, 20, 30]
        attack_positions: List[Dict[str, Any]] = []
        atk_heading_row = def_sniper_row + sniper_height + 2
        map_buffer.update(
            f"A{atk_heading_row}",
            [["Attacking early team positioning"]],
        )
        _merge_cells(map_sheet, f"A{atk_heading_row}:L{atk_heading_row}")
        format_cell_range(
            map_sheet,
            f"A{atk_heading_row}:L{atk_heading_row}",
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
        atk_image_start = atk_heading_row + 1
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
            _ensure_public_asset(drive_service, atk_pos_link)
            atk_image_url = _drive_image_url(atk_pos_link)
            attack_positions.append({"seconds": seconds, "image_url": atk_image_url})
            start_col_idx = col_starts[idx_time]
            end_col_idx = start_col_idx + 3
            start_letter = _col_letter(start_col_idx)
            end_letter = _col_letter(end_col_idx)
            target_cell = f"{start_letter}{atk_image_start}"
            try:
                _add_image(
                    sheets_service,
                    spreadsheet_id,
                    map_sheet.id,
                    target_cell,
                    atk_image_url,
                )
            except Exception:
                map_buffer.update(
                    target_cell,
                    [[f'=IMAGE("{atk_image_url}")']],
                    value_input_option="USER_ENTERED",
                )
            _merge_cells(
                map_sheet,
                f"{start_letter}{atk_image_start}:{end_letter}{atk_image_start + image_height - 1}"
            )

        current_row[0] = atk_image_start + image_height + 2

        atk_sniper_row = current_row[0]
        map_buffer.update(
            f"A{atk_sniper_row}",
            [["Attacking sniper kills"]],
        )
        _merge_cells(map_sheet, f"A{atk_sniper_row}:D{atk_sniper_row}")
        format_cell_range(
            map_sheet,
            f"A{atk_sniper_row}:D{atk_sniper_row}",
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
        get_sniper_kills(
            map_name, "atk", maps_stats[map_name][3], map_data, basic_info, "plots/atk_sniper.png"
        )
        atk_sniper_link = get_image_link(
            "atk_sniper.png", "plots/atk_sniper.png", creds
        )
        _ensure_public_asset(drive_service, atk_sniper_link)
        atk_sniper_url = _drive_image_url(atk_sniper_link)
        try:
            _add_image(
                sheets_service,
                spreadsheet_id,
                map_sheet.id,
                f"A{atk_sniper_row+1}",
                atk_sniper_url,
            )
        except Exception:
            map_buffer.update(
                f"A{atk_sniper_row+1}",
                [[f'=IMAGE("{atk_sniper_url}")']],
                value_input_option="USER_ENTERED",
            )
        _merge_cells(
            map_sheet,
            f"A{atk_sniper_row+1}:D{atk_sniper_row+sniper_height}"
        )
        current_row[0] = atk_sniper_row + sniper_height + 2

        map_detail.update(
            {
                "compositions": sorted(
                    compositions_struct,
                    key=lambda entry: (entry["winrate"] or -1, entry["played"]),
                    reverse=True,
                ),
                "post_plants": post_plants_struct,
                "pistol_plants": pistol_struct,
                "visuals": {
                    "def_positions": defence_positions,
                    "atk_positions": attack_positions,
                    "sniper": {"defence": def_sniper_url, "attack": atk_sniper_url},
                },
            }
        )
        map_buffer.flush()
        sleep_fn(1)
        map_details.append(map_detail)

        header_rows = [
            postplant_section_row,
            defence_heading_row,
            atk_heading_row,
            def_sniper_row,
            atk_sniper_row,
        ]

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

        for header_row in header_rows:
            if not header_row:
                continue
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

    return map_performance_data, map_details


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
    _set_sleep_fn(sleep_fn)
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
    _format_overall_sheet(
        sheet,
        sheets_service,
        spreadsheet.id,
        team_context,
        image_url,
        basic_info,
        maps_stats,
        sleep_fn,
    )

    _notify("Creating per-map tabs…", progress_callback)
    map_performance_data, map_details = _populate_map_tabs(
        spreadsheet,
        sheets_service,
        spreadsheet.id,
        team_context,
        maps_stats,
        data_matches,
        basic_info,
        creds,
        sleep_fn,
        progress_callback,
        drive_service,
    )
    if map_performance_data:
        summary_start = 6 + len(basic_info["matches"])
        _format_map_summary(sheet, summary_start, maps_stats, map_performance_data, sleep_fn)

    report_payload = _build_report_payload(
        team_context,
        image_url,
        match_ids,
        basic_info,
        data_matches,
        map_details,
    )

    recipient = share_email or DEFAULT_SHARE_EMAIL
    if recipient:
        _share_spreadsheet(spreadsheet, recipient)

    try:
        _notify("Ensuring the spreadsheet is viewable by link…", progress_callback)
        _ensure_public_view_permission(drive_service, spreadsheet.id)
    except AnalyticalReportError as exc:
        _notify(f"Unable to grant public viewer access automatically: {exc}", progress_callback)

    gid: Optional[int] = None
    try:
        gid = _get_gid(sheets_service, spreadsheet.id, sheet.title)
    except AnalyticalReportError as exc:
        _notify(f"Unable to resolve sheet tab for embedding: {exc}", progress_callback)

    published_link = ""
    try:
        _notify("Publishing spreadsheet for embedding…", progress_callback)
        published_link = _ensure_published(drive_service, spreadsheet.id)
        sleep_fn(0.5)
    except AnalyticalReportError as exc:
        _notify(f"Unable to publish spreadsheet automatically: {exc}", progress_callback)

    embed_url = ""
    view_url = ""
    csv_url = ""
    if published_link:
        embed_url, view_url, csv_url = _compose_published_urls(published_link, spreadsheet.id, gid)
    else:
        base_embed = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}/htmlview?rm=embedded"
        if gid is not None:
            base_embed += f"&gid={int(gid)}"
        embed_url = base_embed
        view_url = spreadsheet.url
        csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}/export?format=csv"
        if gid is not None:
            csv_url += f"&gid={int(gid)}"

    _notify("Spreadsheet ready for embedding.", progress_callback)
    _notify(f"Spreadsheet created! URL: {embed_url}", progress_callback)
    return {
        "spreadsheet_url": embed_url,              # ← use this in your iframe
        "spreadsheet_view_url": view_url,          # non-embed published page
        "spreadsheet_csv_url": csv_url,            # optional
        "spreadsheet_edit_url": spreadsheet.url,   # editor link
        "spreadsheet_id": spreadsheet.id,
        "team_tag": team_context.tag,
        "team_name": team_context.name,
        "match_count": str(len(match_ids)),
        "report_payload": report_payload,
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
