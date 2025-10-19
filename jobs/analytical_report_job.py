from __future__ import annotations

import traceback
from typing import Optional
import time
from uuid import uuid4

from analytical_rep import AnalyticalReportError, generate_analytical_report
from services.analytical_jobs import AnalyticalJobStore, get_redis_connection, utc_now_iso


def run_analytical_report_job(
    job_id: str,
    *,
    team_tag: str,
    match_count: Optional[int],
    share_email: Optional[str],
    credentials_path: Optional[str],
    spreadsheet_title: Optional[str] = None,
) -> dict:
    """Background task that orchestrates the analytical report generation."""
    redis_conn = get_redis_connection()
    store = AnalyticalJobStore(redis_conn)
    store.update_status(job_id, "started", team_tag=team_tag, match_count=match_count)
    store.append_event(
        job_id,
        "progress",
        {"message": f"Job {job_id} started for team {team_tag}."},
    )

    def emit_progress(message: str) -> None:
        store.append_event(job_id, "progress", {"message": message})

    def prompt_user(spec: dict) -> str:
        prompt_id = spec.get("id") or uuid4().hex
        payload = {"id": prompt_id}
        for key in ("title", "message", "hint", "options", "default"):
            if spec.get(key) is not None:
                payload[key] = spec[key]
        store.append_event(job_id, "prompt", payload)

        while True:
            user_message = store.pop_user_input(job_id, timeout=5)
            if not user_message:
                continue

            response = (user_message.get("message") or "").strip()
            if not response and spec.get("default") is not None:
                response = str(spec["default"])
            if not response and spec.get("default") is None:
                emit_progress("Please provide a response to continue.")
                continue

            store.append_event(
                job_id,
                "prompt_resolved",
                {"id": prompt_id, "response": response},
            )
            return response

    try:
        result = generate_analytical_report(
            team_tag,
            match_count,
            share_email=share_email,
            spreadsheet_title=spreadsheet_title,
            credentials_path=credentials_path,
            sleep_fn=lambda seconds: time.sleep(min(seconds, 0.6)),
            progress_callback=emit_progress,
            prompt_handler=prompt_user,
        )
        store.merge_meta(job_id, {"result": result, "completed_at": utc_now_iso()})
        store.append_event(
            job_id,
            "completed",
            {"message": "Analytical report generated successfully.", "result": result},
        )
        store.update_status(job_id, "finished")
        return result
    except AnalyticalReportError as exc:
        message = str(exc)
        store.append_event(job_id, "error", {"message": message})
        store.update_status(job_id, "failed", error=message)
        raise
    except Exception as exc:  # pragma: no cover - defensive logging
        tb = traceback.format_exc()
        message = f"Unexpected error: {exc}"
        store.append_event(job_id, "error", {"message": message, "traceback": tb})
        store.update_status(job_id, "failed", error=message)
        raise
