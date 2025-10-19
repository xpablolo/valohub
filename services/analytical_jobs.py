from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

import redis

DEFAULT_REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
LOG_LENGTH_SOFT_LIMIT = 500


def utc_now_iso() -> str:
    """Return a compact ISO-8601 timestamp in UTC with trailing 'Z'."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def get_redis_connection(url: Optional[str] = None) -> redis.Redis:
    """Create a Redis connection usable both by Flask and RQ workers."""
    return redis.from_url(url or DEFAULT_REDIS_URL)


@dataclass
class AnalyticalJobKeys:
    base: str
    log: str
    meta: str
    channel: str
    input: str


class AnalyticalJobStore:
    """Persist and stream analytical report job progress via Redis."""

    def __init__(self, redis_conn: redis.Redis, *, log_limit: int = LOG_LENGTH_SOFT_LIMIT) -> None:
        self.redis = redis_conn
        self.log_limit = log_limit

    def keys(self, job_id: str) -> AnalyticalJobKeys:
        base = f"analytical:jobs:{job_id}"
        return AnalyticalJobKeys(
            base=base,
            log=f"{base}:log",
            meta=f"{base}:meta",
            channel=f"{base}:stream",
            input=f"{base}:input",
        )

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------
    def bootstrap(
        self,
        job_id: str,
        *,
        team_tag: str,
        match_count: Optional[int],
        share_email: Optional[str],
        created_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Reset job state and store initial metadata."""
        keys = self.keys(job_id)
        created_at = utc_now_iso()
        meta: Dict[str, Any] = {
            "job_id": job_id,
            "status": "queued",
            "team_tag": team_tag,
            "match_count": match_count,
            "share_email": share_email,
            "created_at": created_at,
            "updated_at": created_at,
        }
        if created_by:
            meta["created_by"] = created_by

        pipe = self.redis.pipeline()
        pipe.delete(keys.log, keys.input, keys.meta)
        pipe.hset(keys.meta, mapping=self._encode_meta(meta))
        pipe.execute()
        self.append_event(job_id, "status", {"status": "queued", "meta": meta})
        return meta

    def update_status(self, job_id: str, status: str, **extra: Any) -> Dict[str, Any]:
        """Persist and broadcast a status transition."""
        timestamp = utc_now_iso()
        payload = {"status": status, "updated_at": timestamp}
        payload.update(extra)
        meta_update = payload.copy()

        keys = self.keys(job_id)
        self.redis.hset(keys.meta, mapping=self._encode_meta(meta_update))
        self.append_event(job_id, "status", payload)
        return payload

    # ------------------------------------------------------------------
    # Logging & streaming
    # ------------------------------------------------------------------
    def append_event(
        self,
        job_id: str,
        event_type: str,
        payload: Dict[str, Any],
        *,
        origin: str = "system",
    ) -> Dict[str, Any]:
        keys = self.keys(job_id)
        message = {
            "type": event_type,
            "origin": origin,
            "payload": payload,
            "timestamp": utc_now_iso(),
        }
        raw = json.dumps(message, ensure_ascii=True)
        pipe = self.redis.pipeline()
        pipe.rpush(keys.log, raw)
        pipe.ltrim(keys.log, -self.log_limit, -1)
        pipe.publish(keys.channel, raw)
        pipe.execute()
        return message

    def log_lines(self, job_id: str) -> Iterable[Dict[str, Any]]:
        keys = self.keys(job_id)
        raw_entries = self.redis.lrange(keys.log, 0, -1)
        for raw in raw_entries:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            try:
                yield json.loads(raw)
            except Exception:
                yield {
                    "type": "progress",
                    "origin": "system",
                    "payload": {"message": raw},
                    "timestamp": utc_now_iso(),
                }

    # ------------------------------------------------------------------
    # Meta helpers
    # ------------------------------------------------------------------
    def get_meta(self, job_id: str) -> Dict[str, Any]:
        keys = self.keys(job_id)
        stored = self.redis.hgetall(keys.meta)
        decoded: Dict[str, Any] = {}
        for raw_key, raw_value in stored.items():
            key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else raw_key
            value = raw_value.decode("utf-8") if isinstance(raw_value, bytes) else raw_value
            decoded[key] = self._decode_meta_value(value)
        return decoded

    def merge_meta(self, job_id: str, data: Dict[str, Any]) -> None:
        keys = self.keys(job_id)
        encoded = self._encode_meta(data)
        self.redis.hset(keys.meta, mapping=encoded)

    # ------------------------------------------------------------------
    # Prompt utilities
    # ------------------------------------------------------------------
    def push_user_input(
        self,
        job_id: str,
        message: str,
        *,
        author: Optional[str] = None,
        prompt_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = {"message": message}
        if author:
            payload["author"] = author
        if prompt_id:
            payload["prompt_id"] = prompt_id
        event = self.append_event(job_id, "user_message", payload, origin="user")
        keys = self.keys(job_id)
        queue_payload = {"message": message, "author": author, "prompt_id": prompt_id}
        self.redis.rpush(keys.input, json.dumps(queue_payload, ensure_ascii=True))
        return event

    def pop_user_input(self, job_id: str, timeout: int = 0) -> Optional[Dict[str, Any]]:
        keys = self.keys(job_id)
        raw = self.redis.blpop(keys.input, timeout=timeout)
        if not raw:
            return None
        _, value = raw
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        try:
            return json.loads(value)
        except Exception:
            return {"message": value}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _encode_meta(self, data: Dict[str, Any]) -> Dict[str, str]:
        encoded: Dict[str, str] = {}
        for key, value in data.items():
            if value is None:
                encoded[key] = ""
            elif isinstance(value, (dict, list)):
                encoded[key] = json.dumps(value, ensure_ascii=True)
            else:
                encoded[key] = str(value)
        return encoded

    def _decode_meta_value(self, value: str) -> Any:
        if value == "":
            return None
        for loader in (json.loads,):
            try:
                parsed = loader(value)
                if isinstance(parsed, (dict, list)):
                    return parsed
            except Exception:
                continue
        return value

    def _build_event(self, event_type: str, payload: Dict[str, Any]) -> str:
        message = {
            "type": event_type,
            "origin": "system",
            "payload": payload,
            "timestamp": utc_now_iso(),
        }
        return json.dumps(message, ensure_ascii=True)
