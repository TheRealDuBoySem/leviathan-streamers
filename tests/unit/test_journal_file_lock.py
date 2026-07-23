from unittest.mock import patch

import pytest

from core.journal.journal_file_lock import (
    JournalFileLock,
    _exclusive_lock_file_descriptor,
    _release_file_descriptor_lock,
)


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


def test_journal_file_lock_acquire_retries_on_busy_lock(mocker, tmp_path):
    lock_path = str(tmp_path / "tick_journal.lock")
    handle = mocker.MagicMock()
    handle.fileno.return_value = 7
    mocker.patch("builtins.open", return_value=handle)
    mocker.patch(
        "core.journal.journal_file_lock._try_acquire_exclusive_lock",
        side_effect=[BlockingIOError(), None],
    )
    mocker.patch("time.sleep")
    lock = JournalFileLock(lock_path, timeout_seconds=1.0)
    lock.acquire()
    assert lock.is_acquired()


def test_journal_file_lock_rejects_invalid_lock_path():
    with pytest.raises(ValueError, match="lock_path must be a non-empty string"):
        JournalFileLock("")
    with pytest.raises(ValueError, match="lock_path must be a non-empty string"):
        JournalFileLock("   ")


def test_journal_file_lock_rejects_non_positive_timeout(tmp_path):
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        JournalFileLock(str(tmp_path / "tick_journal.lock"), timeout_seconds=0)


def test_journal_file_lock_unix_branch(mocker, tmp_path):
    mock_fcntl = mocker.MagicMock()
    mock_fcntl.LOCK_EX = 2
    mock_fcntl.LOCK_NB = 4
    mock_fcntl.LOCK_UN = 8
    mocker.patch("core.journal.journal_file_lock.sys.platform", "linux")
    mocker.patch.dict("sys.modules", {"fcntl": mock_fcntl})
    _exclusive_lock_file_descriptor(1)
    _release_file_descriptor_lock(1)
    assert mock_fcntl.flock.call_count == 2
    assert mock_fcntl.flock.call_args_list[0].args == (1, 6)


def test_journal_file_lock_acquire_retries_then_timeout(mocker, tmp_path):
    lock_path = str(tmp_path / "tick_journal.lock")
    mocker.patch("time.time", side_effect=[0.0, 0.0, 100.0])
    mocker.patch("time.sleep")
    with patch("builtins.open", side_effect=OSError("busy")):
        with pytest.raises(TimeoutError, match="Timed out acquiring journal lock"):
            JournalFileLock(lock_path, timeout_seconds=1.0).acquire()


def test_journal_file_lock_release_close_oserror_is_ignored(tmp_path, mocker):
    lock = JournalFileLock(str(tmp_path / "tick_journal.lock"))
    handle = mocker.MagicMock()
    handle.fileno.return_value = 42
    handle.close.side_effect = OSError("close failed")
    lock._JournalFileLock__handle = handle
    mocker.patch(
        "core.journal.journal_file_lock._release_file_descriptor_lock",
    )
    lock.release()


def test_journal_file_lock_acquire_closes_handle_on_lock_failure(mocker, tmp_path):
    handle = mocker.MagicMock()
    handle.fileno.side_effect = OSError("lock failed")
    handle.close.side_effect = OSError("close failed")
    mocker.patch("builtins.open", return_value=handle)
    mocker.patch("time.time", side_effect=[0.0, 100.0])
    mocker.patch("time.sleep")
    with pytest.raises(TimeoutError, match="Timed out acquiring journal lock"):
        JournalFileLock(str(tmp_path / "tick_journal.lock"), timeout_seconds=1.0).acquire()
    handle.close.assert_called()
