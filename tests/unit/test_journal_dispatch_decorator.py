import pytest

from leviathan_common.models.trade_tick import TradeTick
from core.interfaces.base import IDispatchStrategy
from core.journal.journal_dispatch_decorator import JournalDispatchDecorator
from core.journal.tick_journal import TickJournal
from core.routing.async_queue_dispatcher import AsyncQueueDispatcher


def _tick(trade_id: str, ts: int = 1000) -> TradeTick:
    return TradeTick(
        inst_id="BTCUSDT",
        ts=ts,
        price=100.0,
        size=1.0,
        side="buy",
        trade_id=trade_id,
    )


class _StubDispatchStrategy(IDispatchStrategy):
    def __init__(self) -> None:
        self.dispatched: list[TradeTick] = []
        self.marked_processed = False

    async def dispatch(self, tick: TradeTick) -> None:
        self.dispatched.append(tick)

    async def wait_for_next_tick(self) -> TradeTick:
        raise NotImplementedError

    def mark_tick_as_processed(self) -> None:
        self.marked_processed = True


@pytest.mark.asyncio
async def test_journal_dispatch_decorator_persists_and_forwards(tmp_path):
    journal = TickJournal(str(tmp_path))
    inner = AsyncQueueDispatcher()
    decorator = JournalDispatchDecorator(inner, journal)
    tick = _tick("t1")

    await decorator.dispatch(tick)

    assert journal.latest_seq() == 1
    replay = list(journal.tail_from(1))
    assert replay[0][1].trade_id == "t1"

    retrieved = await decorator.wait_for_next_tick()
    assert retrieved.trade_id == "t1"
    decorator.mark_tick_as_processed()
    assert inner.empty()


def test_journal_dispatch_decorator_contracts(tmp_path):
    journal = TickJournal(str(tmp_path))
    inner = AsyncQueueDispatcher()

    with pytest.raises(TypeError, match="inner must be a IDispatchStrategy instance"):
        JournalDispatchDecorator(None, journal)  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="inner must be a IDispatchStrategy instance"):
        JournalDispatchDecorator(object(), journal)  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="journal must be a TickJournal instance"):
        JournalDispatchDecorator(inner, object())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_journal_dispatch_decorator_dispatch_contract(tmp_path):
    journal = TickJournal(str(tmp_path))
    decorator = JournalDispatchDecorator(AsyncQueueDispatcher(), journal)

    with pytest.raises(TypeError, match="Expected TradeTick"):
        await decorator.dispatch("not a tick")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_journal_dispatch_decorator_delegates_consumption(mocker, tmp_path):
    journal = TickJournal(str(tmp_path))
    inner = _StubDispatchStrategy()
    decorator = JournalDispatchDecorator(inner, journal)
    tick = _tick("t2")

    mock_wait = mocker.patch.object(
        inner,
        "wait_for_next_tick",
        new=mocker.AsyncMock(return_value=tick),
    )
    mock_mark = mocker.patch.object(inner, "mark_tick_as_processed")

    result = await decorator.wait_for_next_tick()
    decorator.mark_tick_as_processed()

    assert result is tick
    mock_wait.assert_awaited_once()
    mock_mark.assert_called_once()


def test_journal_dispatch_decorator_properties(tmp_path):
    journal = TickJournal(str(tmp_path))
    inner = AsyncQueueDispatcher()
    decorator = JournalDispatchDecorator(inner, journal)

    assert decorator.inner is inner
    assert decorator.journal is journal

    with pytest.raises(AttributeError):
        decorator.inner = inner  # type: ignore[misc]
    with pytest.raises(AttributeError):
        decorator.journal = journal  # type: ignore[misc]
