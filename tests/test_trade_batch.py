"""Tests for columnar trade batch export."""

from __future__ import annotations

from datetime import datetime

import pyarrow.parquet as pq

from src.indexers.polymarket.trade_batch import TradeBatch


def test_extend_merges_without_copying_rows():
    fetched_at = datetime.utcnow()
    left = TradeBatch.from_trades(
        [
            type(
                "Trade",
                (),
                {
                    "block_number": 1,
                    "transaction_hash": "0x1",
                    "log_index": 0,
                    "order_hash": "0xa",
                    "maker": "0x01",
                    "taker": "0x02",
                    "maker_asset_id": 0,
                    "taker_asset_id": 1,
                    "maker_amount": 1,
                    "taker_amount": 1,
                    "fee": 0,
                    "timestamp": None,
                },
            )()
        ],
        contract_name="CTF Exchange",
        fetched_at=fetched_at,
    )
    right = TradeBatch.from_trades(
        [
            type(
                "Trade",
                (),
                {
                    "block_number": 2,
                    "transaction_hash": "0x2",
                    "log_index": 1,
                    "order_hash": "0xb",
                    "maker": "0x03",
                    "taker": "0x04",
                    "maker_asset_id": 0,
                    "taker_asset_id": 1,
                    "maker_amount": 2,
                    "taker_amount": 2,
                    "fee": 0,
                    "timestamp": None,
                },
            )()
        ],
        contract_name="NegRisk CTF Exchange",
        fetched_at=fetched_at,
    )

    left.extend(right)

    assert len(left) == 2
    assert left.block_number == [1, 2]


def test_write_parquet_round_trip(tmp_path):
    batch = TradeBatch.from_trades(
        [
            type(
                "Trade",
                (),
                {
                    "block_number": 42,
                    "transaction_hash": "0xabc",
                    "log_index": 0,
                    "order_hash": "0xdef",
                    "maker": "0x0000000000000000000000000000000000000001",
                    "taker": "0x0000000000000000000000000000000000000002",
                    "maker_asset_id": 0,
                    "taker_asset_id": 1,
                    "maker_amount": 10,
                    "taker_amount": 20,
                    "fee": 1,
                    "timestamp": None,
                },
            )()
        ],
        contract_name="CTF Exchange",
        fetched_at=datetime.utcnow(),
    )

    path = tmp_path / "trades_1_1.parquet"
    assert batch.write_parquet(path) == 1

    table = pq.read_table(path)
    assert table.num_rows == 1
    assert table.column("block_number").to_pylist() == [42]
    assert table.column("maker_asset_id").to_pylist() == ["0"]
