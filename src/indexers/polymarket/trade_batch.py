"""Columnar trade batches for fast parquet export."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from eth_abi import decode as abi_decode
from web3 import Web3

TRADE_PARQUET_SCHEMA = pa.schema(
    [
        ("block_number", pa.int64()),
        ("transaction_hash", pa.string()),
        ("log_index", pa.int64()),
        ("order_hash", pa.string()),
        ("maker", pa.string()),
        ("taker", pa.string()),
        ("maker_asset_id", pa.string()),
        ("taker_asset_id", pa.string()),
        ("maker_amount", pa.int64()),
        ("taker_amount", pa.int64()),
        ("fee", pa.int64()),
        ("timestamp", pa.int64()),
        ("_fetched_at", pa.timestamp("us")),
        ("_contract", pa.string()),
    ]
)


@dataclass
class TradeBatch:
    """Append-only column store for OrderFilled rows."""

    block_number: list[int]
    transaction_hash: list[str]
    log_index: list[int]
    order_hash: list[str]
    maker: list[str]
    taker: list[str]
    maker_asset_id: list[str]
    taker_asset_id: list[str]
    maker_amount: list[int]
    taker_amount: list[int]
    fee: list[int]
    timestamp: list[Optional[int]]
    _fetched_at: list[datetime]
    _contract: list[str]

    @classmethod
    def empty(cls) -> TradeBatch:
        return cls(
            block_number=[],
            transaction_hash=[],
            log_index=[],
            order_hash=[],
            maker=[],
            taker=[],
            maker_asset_id=[],
            taker_asset_id=[],
            maker_amount=[],
            taker_amount=[],
            fee=[],
            timestamp=[],
            _fetched_at=[],
            _contract=[],
        )

    def __len__(self) -> int:
        return len(self.block_number)

    def extend(self, other: TradeBatch) -> None:
        """Merge another batch in-place without copying rows."""
        self.block_number.extend(other.block_number)
        self.transaction_hash.extend(other.transaction_hash)
        self.log_index.extend(other.log_index)
        self.order_hash.extend(other.order_hash)
        self.maker.extend(other.maker)
        self.taker.extend(other.taker)
        self.maker_asset_id.extend(other.maker_asset_id)
        self.taker_asset_id.extend(other.taker_asset_id)
        self.maker_amount.extend(other.maker_amount)
        self.taker_amount.extend(other.taker_amount)
        self.fee.extend(other.fee)
        self.timestamp.extend(other.timestamp)
        self._fetched_at.extend(other._fetched_at)
        self._contract.extend(other._contract)

    def append_log(self, log: dict, *, contract_name: str, fetched_at: datetime) -> None:
        topics = log["topics"]
        tx_hash = log["transactionHash"]
        order_hash = topics[1]
        maker_asset_id, taker_asset_id, maker_amount, taker_amount, fee = abi_decode(
            ["uint256", "uint256", "uint256", "uint256", "uint256"],
            log["data"],
        )
        self.block_number.append(int(log["blockNumber"]))
        self.transaction_hash.append(tx_hash.hex() if hasattr(tx_hash, "hex") else str(tx_hash))
        self.log_index.append(int(log["logIndex"]))
        self.order_hash.append(order_hash.hex() if hasattr(order_hash, "hex") else str(order_hash))
        self.maker.append(Web3.to_checksum_address("0x" + topics[2].hex()[-40:]))
        self.taker.append(Web3.to_checksum_address("0x" + topics[3].hex()[-40:]))
        self.maker_asset_id.append(str(maker_asset_id))
        self.taker_asset_id.append(str(taker_asset_id))
        self.maker_amount.append(int(maker_amount))
        self.taker_amount.append(int(taker_amount))
        self.fee.append(int(fee))
        self.timestamp.append(None)
        self._fetched_at.append(fetched_at)
        self._contract.append(contract_name)

    @classmethod
    def from_trades(cls, trades: list[Any], *, contract_name: str, fetched_at: datetime) -> TradeBatch:
        batch = cls.empty()
        for trade in trades:
            batch.block_number.append(int(trade.block_number))
            batch.transaction_hash.append(str(trade.transaction_hash))
            batch.log_index.append(int(trade.log_index))
            batch.order_hash.append(str(trade.order_hash))
            batch.maker.append(str(trade.maker))
            batch.taker.append(str(trade.taker))
            batch.maker_asset_id.append(str(trade.maker_asset_id))
            batch.taker_asset_id.append(str(trade.taker_asset_id))
            batch.maker_amount.append(int(trade.maker_amount))
            batch.taker_amount.append(int(trade.taker_amount))
            batch.fee.append(int(trade.fee))
            batch.timestamp.append(getattr(trade, "timestamp", None))
            batch._fetched_at.append(fetched_at)
            batch._contract.append(contract_name)
        return batch

    def to_arrow_table(self) -> pa.Table:
        return pa.Table.from_pydict(
            {
                "block_number": self.block_number,
                "transaction_hash": self.transaction_hash,
                "log_index": self.log_index,
                "order_hash": self.order_hash,
                "maker": self.maker,
                "taker": self.taker,
                "maker_asset_id": self.maker_asset_id,
                "taker_asset_id": self.taker_asset_id,
                "maker_amount": self.maker_amount,
                "taker_amount": self.taker_amount,
                "fee": self.fee,
                "timestamp": self.timestamp,
                "_fetched_at": self._fetched_at,
                "_contract": self._contract,
            },
            schema=TRADE_PARQUET_SCHEMA,
        )

    def write_parquet(self, path: Path) -> int:
        if not self.block_number:
            return 0
        pq.write_table(self.to_arrow_table(), path, compression="snappy")
        return len(self)
