import pytest

from core.journal.journal_file_lock import JournalFileLock


def test_journal_file_lock_context_manager_acquires_and_releases(tmp_path):
    lock_path = str(tmp_path / "tick_journal.lock")
    with JournalFileLock(lock_path) as lock:
        assert lock.is_acquired()
        assert lock.lock_path == lock_path
    assert not lock.is_acquired()


def test_journal_file_lock_double_acquire_raises(tmp_path):
    lock = JournalFileLock(str(tmp_path / "tick_journal.lock"))
    lock.acquire()
    try:
        with pytest.raises(RuntimeError, match="already acquired"):
            lock.acquire()
    finally:
        lock.release()


def test_journal_file_lock_release_is_idempotent(tmp_path):
    lock = JournalFileLock(str(tmp_path / "tick_journal.lock"))
    lock.release()
    with lock:
        pass
    lock.release()


def test_journal_file_lock_blocks_until_holder_releases(tmp_path):
    lock_path = str(tmp_path / "tick_journal.lock")
    holder = JournalFileLock(lock_path)
    holder.acquire()
    try:
        waiter = JournalFileLock(lock_path, timeout_seconds=0.2)
        with pytest.raises(TimeoutError, match="Timed out acquiring journal lock"):
            waiter.acquire()
    finally:
        holder.release()


def test_journal_file_lock_rejects_invalid_lock_path():
    with pytest.raises(ValueError, match="lock_path must be a non-empty string"):
        JournalFileLock("")
    with pytest.raises(ValueError, match="lock_path must be a non-empty string"):
        JournalFileLock("   ")


def test_journal_file_lock_rejects_non_positive_timeout(tmp_path):
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        JournalFileLock(str(tmp_path / "tick_journal.lock"), timeout_seconds=0)
