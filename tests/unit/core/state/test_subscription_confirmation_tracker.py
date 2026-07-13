import asyncio
import logging

import pytest

from core.state.subscription_confirmation_tracker import (
    DEFAULT_CONFIRMATION_TIMEOUT_SECONDS,
    SubscriptionConfirmationTracker,
)


@pytest.mark.asyncio
async def test_begin_expectation_and_record_confirmation_completes_early():
    completed: list[set[str]] = []
    tracker = SubscriptionConfirmationTracker(
        timeout_seconds=2.0,
        on_complete=lambda confirmed: completed.append(confirmed),
    )
    tracker.begin_expectation(["BTCUSDT", "ETHUSDT"])
    assert tracker.get_expected_symbols() == ["BTCUSDT", "ETHUSDT"]
    assert tracker.get_missing_symbols() == ["BTCUSDT", "ETHUSDT"]

    assert tracker.record_confirmation("BTCUSDT") is True
    assert tracker.record_confirmation("BTCUSDT") is False  # duplicate
    assert tracker.record_confirmation("XRPUSDT") is False  # unexpected

    assert tracker.record_confirmation("ETHUSDT") is True
    await asyncio.sleep(0)  # let cancel settle
    assert completed == [{"BTCUSDT", "ETHUSDT"}]
    assert tracker.get_expected_symbols() == []
    assert tracker.is_expectation_active() is False


@pytest.mark.asyncio
async def test_partial_confirmation_warns_after_timeout(caplog):
    tracker = SubscriptionConfirmationTracker(timeout_seconds=0.05)
    tracker.begin_expectation(["BTCUSDT", "ETHUSDT", "XRPUSDT"])
    tracker.record_confirmation("XRPUSDT")

    with caplog.at_level(logging.WARNING):
        await asyncio.sleep(0.12)

    assert any(
        "Confirmation partielle d'abonnement" in record.message
        and "XRPUSDT" in record.message
        and "BTCUSDT" in record.message
        for record in caplog.records
    )
    assert tracker.get_missing_symbols() == []
    assert tracker.is_expectation_active() is False


@pytest.mark.asyncio
async def test_complete_confirmation_logs_requested_vs_confirmed(caplog):
    tracker = SubscriptionConfirmationTracker(timeout_seconds=0.05)
    tracker.begin_expectation(["XRPUSDT"])

    with caplog.at_level(logging.INFO):
        tracker.record_confirmation("XRPUSDT")
        await asyncio.sleep(0)

    assert any(
        "Abonnements confirmés après reconnect" in record.message
        and "XRPUSDT" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_begin_expectation_cancels_previous_window():
    partial_calls: list[tuple] = []
    tracker = SubscriptionConfirmationTracker(
        timeout_seconds=0.2,
        on_partial=lambda expected, confirmed, missing: partial_calls.append(
            (expected, confirmed, missing)
        ),
    )
    tracker.begin_expectation(["OLD"])
    tracker.begin_expectation(["NEW"])
    tracker.record_confirmation("NEW")
    await asyncio.sleep(0.25)
    assert partial_calls == []


@pytest.mark.asyncio
async def test_cancel_clears_pending_without_evaluation():
    partial_calls: list = []
    complete_calls: list = []
    tracker = SubscriptionConfirmationTracker(
        timeout_seconds=0.05,
        on_partial=lambda *args: partial_calls.append(args),
        on_complete=lambda *args: complete_calls.append(args),
    )
    tracker.begin_expectation(["BTCUSDT"])
    tracker.cancel()
    await asyncio.sleep(0.1)
    assert partial_calls == []
    assert complete_calls == []
    assert tracker.get_expected_symbols() == []


def test_tracker_contracts():
    with pytest.raises(TypeError, match="timeout_seconds must be a number"):
        SubscriptionConfirmationTracker(timeout_seconds="slow")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="timeout_seconds must be > 0"):
        SubscriptionConfirmationTracker(timeout_seconds=0)

    tracker = SubscriptionConfirmationTracker()
    assert tracker.timeout_seconds == DEFAULT_CONFIRMATION_TIMEOUT_SECONDS

    with pytest.raises(TypeError, match="symbols must be a list"):
        tracker.begin_expectation("BTC")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="symbols must be strings"):
        tracker.begin_expectation(["BTC", 1])  # type: ignore[list-item]
    with pytest.raises(ValueError, match="symbols must be non-empty strings"):
        tracker.begin_expectation([""])

    with pytest.raises(TypeError, match="symbol must be a string"):
        tracker.record_confirmation(123)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="symbol cannot be empty"):
        tracker.record_confirmation("")


@pytest.mark.asyncio
async def test_begin_expectation_empty_is_noop():
    tracker = SubscriptionConfirmationTracker(timeout_seconds=0.05)
    tracker.begin_expectation([])
    assert tracker.is_expectation_active() is False
    await asyncio.sleep(0.08)
    assert tracker.get_confirmed_symbols() == []


@pytest.mark.asyncio
async def test_evaluate_timeout_generation_mismatch_is_noop():
    tracker = SubscriptionConfirmationTracker(timeout_seconds=0.05)
    tracker.begin_expectation(["BTCUSDT"])
    await tracker._SubscriptionConfirmationTracker__evaluate_after_timeout(0)
    assert tracker.is_expectation_active() is True
    tracker.cancel()


@pytest.mark.asyncio
async def test_evaluate_timeout_complete_and_partial_callbacks(caplog):
    completed = []
    partial = []
    tracker = SubscriptionConfirmationTracker(
        timeout_seconds=0.01,
        on_complete=lambda confirmed: completed.append(set(confirmed)),
        on_partial=lambda expected, confirmed, missing: partial.append(
            (set(expected), set(confirmed), set(missing))
        ),
    )
    tracker._SubscriptionConfirmationTracker__generation = 1
    tracker._SubscriptionConfirmationTracker__expected = {"BTCUSDT"}
    tracker._SubscriptionConfirmationTracker__confirmed = {"BTCUSDT"}
    await tracker._SubscriptionConfirmationTracker__evaluate_after_timeout(1)
    assert completed and completed[0] == {"BTCUSDT"}

    tracker2 = SubscriptionConfirmationTracker(
        timeout_seconds=0.01,
        on_partial=lambda expected, confirmed, missing: partial.append(
            (set(expected), set(confirmed), set(missing))
        ),
    )
    tracker2.begin_expectation(["BTCUSDT", "ETHUSDT"])
    await asyncio.sleep(0.05)
    assert partial
    assert partial[-1][2] == {"BTCUSDT", "ETHUSDT"} or "ETHUSDT" in partial[-1][2]

    tracker3 = SubscriptionConfirmationTracker(timeout_seconds=0.01)
    tracker3._SubscriptionConfirmationTracker__generation = 7
    tracker3._SubscriptionConfirmationTracker__expected = {"XRPUSDT"}
    tracker3._SubscriptionConfirmationTracker__confirmed = {"XRPUSDT"}
    with caplog.at_level(logging.INFO):
        await tracker3._SubscriptionConfirmationTracker__evaluate_after_timeout(7)
    assert any("Abonnements confirmés" in r.message for r in caplog.records)
