"""Run consecutive bounded collection batches and publish each accepted increment."""
from __future__ import annotations

from copy import deepcopy
import time

from ingestion.source_campaign import run_campaign


def run_collection_session(store, adapter, settings, *, today_factory, publish,
                           max_cycles=3, max_seconds=900, history_enabled=True,
                           code_sha="unknown", fetcher=None, on_cycle=None,
                           clock=time.monotonic, collector=run_campaign):
    """Continue useful work immediately; stop on quota, failure, capacity or budget.

    Each batch retains the collector's durable provider quotas and lane fairness.
    Publication follows every batch with accepted state, including partial failures.
    The session budget is checked between operations; in-flight persistence is never
    cancelled merely to meet a wall-clock target.
    """
    if type(max_cycles) is not int or not 1 <= max_cycles <= 6:
        raise ValueError("Session cycles must be between 1 and 6")
    if type(max_seconds) is not int or not 60 <= max_seconds <= 1800:
        raise ValueError("Session seconds must be between 60 and 1800")
    started = clock()
    reports = []
    reason = "cycle_budget"
    for cycle in range(1, max_cycles + 1):
        remaining = int(max_seconds - (clock() - started))
        if remaining <= 0:
            reason = "session_time_budget"
            break
        batch_settings = deepcopy(settings)
        batch_settings["max_run_seconds"] = min(settings["max_run_seconds"], remaining)
        options = {"history_enabled": history_enabled, "code_sha": code_sha}
        if fetcher is not None:
            options["fetcher"] = fetcher
        report = collector(store, adapter, today_factory(), batch_settings, **options)
        report["cycle"] = cycle
        # Older accepted data remains publishable even if this batch is quota-bound
        # or the provider returns an error. A publication failure propagates and
        # retains the previous validated pointer; it is never recorded as success.
        if report.get("total_accepted_responses", 0):
            report["landing_publication"] = publish(store, adapter, code_sha)
        reports.append(report)
        if on_cycle is not None:
            on_cycle(report)
        if report.get("failed_requests", 0):
            reason = "source_error"
            break
        if report["reason"] not in {"request_budget", "time_budget"}:
            reason = report["reason"]
            break
    return {
        "source_id": adapter.SOURCE_ID,
        "reason": reason,
        "cycles_completed": len(reports),
        "requests": sum(r["requests"] for r in reports),
        "accepted_responses": sum(r["accepted_responses"] for r in reports),
        "failed_requests": sum(r["failed_requests"] for r in reports),
        "elapsed_seconds": round(clock() - started, 2),
        "coverage_status": "incomplete",
        "publication_layer": "01_landing",
        "cycles": reports,
    }
