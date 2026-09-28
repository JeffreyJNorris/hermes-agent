from __future__ import annotations

from gateway.session_context import (
    clear_session_vars,
    get_session_env,
    reset_session_vars,
    set_session_vars,
)


def test_cron_job_id_is_task_local_and_cleared() -> None:
    reset_session_vars()
    tokens = set_session_vars(
        cron_session="1",
        cron_job_id="job-123",
        async_delivery=False,
    )
    try:
        assert get_session_env("HERMES_CRON_SESSION") == "1"
        assert get_session_env("HERMES_CRON_JOB_ID") == "job-123"
    finally:
        clear_session_vars(tokens)

    assert get_session_env("HERMES_CRON_JOB_ID") == ""
