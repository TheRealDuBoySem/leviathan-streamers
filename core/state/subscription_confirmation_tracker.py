"""Tracks expected vs confirmed subscription acknowledgements after resubscribe.

Pattern: State — pending confirmation window with timeout evaluation.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Callable, List, Optional, Set

logger = logging.getLogger(__name__)

DEFAULT_CONFIRMATION_TIMEOUT_SECONDS = 5.0


class SubscriptionConfirmationTracker:
    """
    Observes subscription acknowledgements for a known expected-symbol set.

    Invariants:
        - Confirmed symbols are always a subset of the last expected set (or empty
          when no expectation is active).
        - At most one timeout watchdog task is scheduled at a time.
    """

    def __init__(
        self,
        timeout_seconds: float = DEFAULT_CONFIRMATION_TIMEOUT_SECONDS,
        *,
        on_partial: Optional[Callable[[Set[str], Set[str], Set[str]], None]] = None,
        on_complete: Optional[Callable[[Set[str]], None]] = None,
    ) -> None:
        if not isinstance(timeout_seconds, (int, float)):
            raise TypeError("timeout_seconds must be a number")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")

        self.__timeout_seconds = float(timeout_seconds)
        self.__expected: Set[str] = set()
        self.__confirmed: Set[str] = set()
        self.__watchdog_task: Optional[asyncio.Task] = None
        self.__on_partial = on_partial
        self.__on_complete = on_complete
        self.__generation = 0

    @property
    def timeout_seconds(self) -> float:
        """Return the confirmation timeout window in seconds."""
        return self.__timeout_seconds

    def get_expected_symbols(self) -> List[str]:
        """Return a sorted copy of symbols expected to be acknowledged."""
        return sorted(self.__expected)

    def get_confirmed_symbols(self) -> List[str]:
        """Return a sorted copy of symbols confirmed so far."""
        return sorted(self.__confirmed)

    def get_missing_symbols(self) -> List[str]:
        """Return symbols still awaiting confirmation."""
        return sorted(self.__expected - self.__confirmed)

    def is_expectation_active(self) -> bool:
        """True while a confirmation window is open."""
        return bool(self.__expected) and self.__watchdog_task is not None

    def begin_expectation(self, symbols: List[str]) -> None:
        """
        Start a new confirmation window for ``symbols``.

        Cancels any previous pending window. Empty ``symbols`` clears state.
        """
        if not isinstance(symbols, list):
            raise TypeError("symbols must be a list")
        for symbol in symbols:
            if not isinstance(symbol, str):
                raise TypeError("symbols must be strings")
            if not symbol:
                raise ValueError("symbols must be non-empty strings")

        self.cancel()
        if not symbols:
            return

        self.__generation += 1
        generation = self.__generation
        self.__expected = set(symbols)
        self.__confirmed = set()
        self.__watchdog_task = asyncio.create_task(
            self.__evaluate_after_timeout(generation),
            name="subscription-confirmation-watchdog",
        )

    def record_confirmation(self, symbol: str) -> bool:
        """
        Record an acknowledgement for ``symbol``.

        Returns True if the symbol was expected and newly confirmed.
        Completes early (and cancels the watchdog) when all expected symbols
        are confirmed.
        """
        if not isinstance(symbol, str):
            raise TypeError("symbol must be a string")
        if not symbol:
            raise ValueError("symbol cannot be empty")

        if symbol not in self.__expected:
            return False
        if symbol in self.__confirmed:
            return False

        self.__confirmed.add(symbol)
        if self.__confirmed >= self.__expected:
            self.__complete_successfully()
        return True

    def cancel(self) -> None:
        """Cancel any pending confirmation window without evaluating."""
        self.__generation += 1
        task = self.__watchdog_task
        self.__watchdog_task = None
        self.__expected = set()
        self.__confirmed = set()
        if task is not None and not task.done():
            task.cancel()

    def __complete_successfully(self) -> None:
        confirmed = set(self.__confirmed)
        self.__generation += 1
        task = self.__watchdog_task
        self.__watchdog_task = None
        self.__expected = set()
        self.__confirmed = set()
        if task is not None and not task.done():
            task.cancel()
        if self.__on_complete is not None:
            self.__on_complete(confirmed)
        else:
            logger.info(
                "Abonnements confirmés après reconnect: demandés=%s, confirmés=%s",
                sorted(confirmed),
                sorted(confirmed),
            )

    async def __evaluate_after_timeout(self, generation: int) -> None:
        try:
            await asyncio.sleep(self.__timeout_seconds)
        except asyncio.CancelledError:
            raise

        if generation != self.__generation:
            return

        expected = set(self.__expected)
        confirmed = set(self.__confirmed)
        missing = expected - confirmed
        self.__watchdog_task = None
        self.__expected = set()
        self.__confirmed = set()

        if not missing:
            if self.__on_complete is not None:
                self.__on_complete(confirmed)
            else:
                logger.info(
                    "Abonnements confirmés après reconnect: demandés=%s, confirmés=%s",
                    sorted(expected),
                    sorted(confirmed),
                )
            return

        if self.__on_partial is not None:
            self.__on_partial(expected, confirmed, missing)
        else:
            logger.warning(
                "Confirmation partielle d'abonnement après reconnect: "
                "demandés=%s, confirmés=%s, manquants=%s (timeout=%.1fs)",
                sorted(expected),
                sorted(confirmed),
                sorted(missing),
                self.__timeout_seconds,
            )
