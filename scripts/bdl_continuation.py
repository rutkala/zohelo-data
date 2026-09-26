#!/usr/bin/env python3
"""Fail-closed decision for a successor BDL Actions run."""
from __future__ import annotations
import json
from pathlib import Path
import sys


def should_continue(summary: dict) -> bool:
    if not isinstance(summary, dict):
        return False
    return (
        summary.get('status') == 'interrupted'
        and summary.get('run_stop_reason') == 'runtime_budget_reached'
        and summary.get('checkpoint_saved') is True
        and summary.get('writer_released') is True
        and summary.get('progress_counts_scope') == 'durable_checkpoint_plus_current_run'
        and summary.get('resumable_work') is True
        and summary.get('source_errors_require_review') is False
        and summary.get('pass_complete') is False
        and summary.get('load_complete') is False
    )


def main() -> int:
    path = Path(sys.argv[1])
    try:
        summary = json.loads(path.read_text(encoding='utf-8'))
        approved = should_continue(summary)
    except (OSError, ValueError, TypeError):
        approved = False
    print('CONTINUE=true' if approved else 'CONTINUE=false')
    return 0


if __name__ == '__main__':
    sys.exit(main())
