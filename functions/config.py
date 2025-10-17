from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SETTINGS_PATH = PROJECT_ROOT / "api_keys" / "settings.json"


@dataclass(frozen=True)
class Settings:
    """Container for API credentials and runtime configuration."""

    riot_api_key: str
    valolytics_key: str
    openai_key: str | None = None
    additional: Dict[str, Any] | None = None


def _load_raw_settings(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(
            f"Settings file not found at {path}. "
            "Ensure api_keys/settings.json is available."
        )
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


@lru_cache()
def get_settings(path: Path | None = None) -> Settings:
    """Load API keys and other secrets from the shared settings file."""
    settings_path = Path(path) if path else DEFAULT_SETTINGS_PATH
    raw = _load_raw_settings(settings_path)
    return Settings(
        riot_api_key=raw.get("riot_api_key", ""),
        valolytics_key=raw.get("valolytics_key", ""),
        openai_key=raw.get("openai_key"),
        additional={k: v for k, v in raw.items() if k not in {
            "riot_api_key",
            "valolytics_key",
            "openai_key",
        }},
    )


__all__ = ["Settings", "get_settings", "PROJECT_ROOT", "DEFAULT_SETTINGS_PATH"]
