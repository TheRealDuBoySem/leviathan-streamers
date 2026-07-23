"""Unit tests for SymbolDedupBucket."""

from core.journal.symbol_dedup_bucket import SymbolDedupBucket


def test_symbol_dedup_bucket_ignores_duplicate_trade_id():
    bucket = SymbolDedupBucket(max_size=3)
    bucket.add("dup")
    bucket.add("dup")
    assert bucket.to_list() == ["dup"]
    assert bucket.contains("dup") is True


def test_symbol_dedup_bucket_from_list():
    bucket = SymbolDedupBucket.from_list(["a", "b", "c"], max_size=2)
    assert bucket.to_list() == ["b", "c"]


def test_symbol_dedup_bucket_evicts_oldest_on_overflow():
    bucket = SymbolDedupBucket(max_size=2)
    bucket.add("a")
    bucket.add("b")
    bucket.add("c")
    assert bucket.to_list() == ["b", "c"]
    assert bucket.contains("a") is False
