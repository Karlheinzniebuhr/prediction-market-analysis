"""Tests for adaptive trades chunk sizing."""

from src.indexers.polymarket.adaptive_chunks import AdaptiveChunkSizer


def test_dense_region_shrinks_span():
    sizer = AdaptiveChunkSizer(initial_blocks=100, target_logs=8500, min_blocks=10, max_blocks=400)
    start, end = sizer.plan(1_000, 9_999)
    assert end - start + 1 == 100

    sizer.observe(100, 20_000)
    start, end = sizer.plan(1_100, 9_999)
    assert end - start + 1 < 100
    assert end - start + 1 >= 10


def test_sparse_region_grows_span():
    sizer = AdaptiveChunkSizer(initial_blocks=100, target_logs=8500, min_blocks=10, max_blocks=400)
    sizer.observe(100, 5)
    start, end = sizer.plan(5_000, 9_999)
    assert end - start + 1 > 100
    assert end - start + 1 <= 400


def test_plan_respects_chain_end():
    sizer = AdaptiveChunkSizer(initial_blocks=400, max_blocks=400)
    start, end = sizer.plan(9_950, 9_999)
    assert start == 9_950
    assert end == 9_999
