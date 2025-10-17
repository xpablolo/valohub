from __future__ import annotations

from typing import Any, Dict, Optional

import requests

from .config import get_settings

DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; ValoHub/1.0; +https://valohub)"
REQUEST_TIMEOUT = 20


def _valolytics_headers() -> Dict[str, str]:
    settings = get_settings()
    return {
        "user-agent": DEFAULT_USER_AGENT,
        "x-api-key": settings.valolytics_key,
    }


def _valorant_headers() -> Dict[str, str]:
    return {
        "user-agent": DEFAULT_USER_AGENT,
    }


def _request_json(method: str, url: str, headers: Dict[str, str]) -> Any:
    response = requests.request(
        method,
        url,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def get_match_by_match_id(match_id: str, region: str) -> Any:
    url = f"https://api.valolytics.gg/api/matches/{region}/{match_id}"
    return _request_json("GET", url, _valolytics_headers())


def get_puuid_by_riotid(game_name: str, tag_line: str, region: str) -> Any:
    url = (
        "https://api.valolytics.gg/api/riot/account/v1/"
        f"accounts/by-riot-id/{region}/{game_name}/{tag_line}"
    )
    return _request_json("GET", url, _valolytics_headers())


def get_matchlist_by_puuid(puuid: str, region: str) -> Any:
    url = f"https://api.valolytics.gg/api/riot/match/v1/matchlists/by-puuid/{region}/{puuid}"
    return _request_json("GET", url, _valolytics_headers())


def get_riotid_by_puuid(puuid: str, region: str) -> Any:
    url = f"https://api.valolytics.gg/api/riot/account/v1/accounts/by-puuid/{region}/{puuid}"
    return _request_json("GET", url, _valolytics_headers())


def get_playerlocations_by_id(identifier: str, region: str) -> Any:
    url = f"https://api.valolytics.gg/api/stats/playerlocations/{region}/{identifier}"
    return _request_json("GET", url, _valolytics_headers())


def get_playerstats_by_id(identifier: str, region: str) -> Any:
    url = f"https://api.valolytics.gg/api/stats/playerstats/{region}/{identifier}"
    return _request_json("POST", url, _valolytics_headers())


def get_teamstats_by_id(identifier: str, region: str) -> Any:
    url = f"https://api.valolytics.gg/api/stats/teamstats/{region}/{identifier}"
    return _request_json("POST", url, _valolytics_headers())


def get_minimap_by_uuid(uuid: str) -> Any:
    url = f"https://api.valolytics.gg/api/stats/minimap/{uuid}"
    return _request_json("POST", url, _valolytics_headers())


def get_teams() -> Any:
    url = "https://api.valolytics.gg/teams"
    return _request_json("GET", url, _valolytics_headers())


def get_team_by_id(identifier: str) -> Any:
    url = f"https://api.valolytics.gg/teams/{identifier}"
    return _request_json("GET", url, _valolytics_headers())


def get_agent_by_puuid(puuid: str) -> Any:
    url = f"https://valorant-api.com/v1/agents/{puuid}"
    return _request_json("GET", url, _valorant_headers())


def get_weapon_by_puuid(puuid: str) -> Any:
    url = f"https://valorant-api.com/v1/weapons/{puuid}"
    return _request_json("GET", url, _valorant_headers())


def get_maps() -> Any:
    url = "https://valorant-api.com/v1/maps"
    return _request_json("GET", url, _valorant_headers())


def get_map_by_id(map_identifier: str) -> Optional[str]:
    maps = get_maps().get("data", [])
    for mapa in maps:
        if mapa.get("mapUrl") == map_identifier:
            return mapa.get("displayName")
    return None


__all__ = [
    "get_match_by_match_id",
    "get_puuid_by_riotid",
    "get_matchlist_by_puuid",
    "get_riotid_by_puuid",
    "get_playerlocations_by_id",
    "get_playerstats_by_id",
    "get_teamstats_by_id",
    "get_minimap_by_uuid",
    "get_teams",
    "get_team_by_id",
    "get_agent_by_puuid",
    "get_weapon_by_puuid",
    "get_maps",
    "get_map_by_id",
]
