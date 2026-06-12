"""Fetch Polymarket trades directly from the Polygon blockchain."""

import concurrent.futures
import os
import time
from collections.abc import Generator
from dataclasses import dataclass
from typing import Callable, Optional, TypeVar

from dotenv import load_dotenv
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

load_dotenv()

# Contract addresses
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEGRISK_CTF_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

# OrderFilled event signature
ORDER_FILLED_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

# ABI for OrderFilled event
ORDER_FILLED_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "orderHash", "type": "bytes32"},
        {"indexed": True, "name": "maker", "type": "address"},
        {"indexed": True, "name": "taker", "type": "address"},
        {"indexed": False, "name": "makerAssetId", "type": "uint256"},
        {"indexed": False, "name": "takerAssetId", "type": "uint256"},
        {"indexed": False, "name": "makerAmountFilled", "type": "uint256"},
        {"indexed": False, "name": "takerAmountFilled", "type": "uint256"},
        {"indexed": False, "name": "fee", "type": "uint256"},
    ],
    "name": "OrderFilled",
    "type": "event",
}

# Public Polygon RPC
POLYGON_RPC = os.getenv("POLYGON_RPC", "")
RPC_MAX_RETRIES = int(os.getenv("POLYGON_RPC_MAX_RETRIES", "6"))
RPC_RETRY_BASE_SEC = float(os.getenv("POLYGON_RPC_RETRY_BASE_SEC", "2.0"))

T = TypeVar("T")


def _rpc_call_with_retry(fn: Callable[[], T], label: str) -> T:
    last_err: Exception | None = None
    for attempt in range(RPC_MAX_RETRIES):
        try:
            return fn()
        except Exception as e:
            last_err = e
            err = str(e).lower()
            retryable = any(
                token in err
                for token in (
                    "429",
                    "too many requests",
                    "timeout",
                    "timed out",
                    "connection",
                    "reset",
                    "502",
                    "503",
                    "504",
                    "gateway",
                    "rate limit",
                )
            )
            if retryable:
                time.sleep(min(60.0, RPC_RETRY_BASE_SEC * (2 ** attempt)))
                continue
            raise
    raise last_err  # type: ignore[misc]


@dataclass
class BlockchainTrade:
    """A trade decoded from the blockchain."""

    block_number: int
    transaction_hash: str
    log_index: int
    order_hash: str
    maker: str
    taker: str
    maker_asset_id: int
    taker_asset_id: int
    maker_amount: int  # In smallest units (6 decimals for USDC)
    taker_amount: int
    fee: int
    timestamp: Optional[int] = None  # Block timestamp

    @property
    def is_buy(self) -> bool:
        """True if maker is providing USDC (buying outcome tokens)."""
        return self.maker_asset_id == 0

    @property
    def price(self) -> float:
        """Calculate price in USDC per token."""
        if self.is_buy:
            # Maker gives USDC, receives tokens
            if self.taker_amount > 0:
                return self.maker_amount / self.taker_amount
        else:
            # Maker gives tokens, receives USDC
            if self.maker_amount > 0:
                return self.taker_amount / self.maker_amount
        return 0.0

    @property
    def size(self) -> float:
        """Number of tokens traded (in token units, 6 decimals)."""
        if self.is_buy:
            return self.taker_amount / 1e6
        return self.maker_amount / 1e6

    @property
    def side(self) -> str:
        """BUY or SELL from taker's perspective."""
        return "BUY" if self.is_buy else "SELL"

    @property
    def condition_id(self) -> str:
        """Get condition ID from asset ID (first 32 bytes of position ID)."""
        asset_id = self.taker_asset_id if self.is_buy else self.maker_asset_id
        if asset_id == 0:
            return ""
        # The condition ID is embedded in the position ID
        # Position ID = keccak256(collateral, conditionId, partition)
        # We store the full asset ID as a hex string for now
        return hex(asset_id)


class PolygonClient:
    """Client for fetching Polymarket trades from Polygon blockchain."""

    def __init__(self, rpc_url: Optional[str] = None):
        self.rpc_url = rpc_url or POLYGON_RPC
        self.w3 = Web3(Web3.HTTPProvider(self.rpc_url, request_kwargs={"timeout": 30}))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        # Create contract instances for decoding
        self.ctf_exchange = self.w3.eth.contract(address=Web3.to_checksum_address(CTF_EXCHANGE), abi=[ORDER_FILLED_ABI])
        self.negrisk_exchange = self.w3.eth.contract(
            address=Web3.to_checksum_address(NEGRISK_CTF_EXCHANGE),
            abi=[ORDER_FILLED_ABI],
        )

    def get_block_number(self) -> int:
        """Get current block number."""
        return _rpc_call_with_retry(lambda: self.w3.eth.block_number, "get_block_number")

    def get_block_timestamp(self, block_number: int) -> int:
        """Get timestamp for a block."""
        block = _rpc_call_with_retry(
            lambda: self.w3.eth.get_block(block_number),
            f"get_block_timestamp({block_number})",
        )
        return block["timestamp"]

    def _decode_order_filled(self, log: dict, contract) -> BlockchainTrade:
        """Decode an OrderFilled event log."""
        decoded = contract.events.OrderFilled().process_log(log)
        args = decoded["args"]

        return BlockchainTrade(
            block_number=log["blockNumber"],
            transaction_hash=log["transactionHash"].hex(),
            log_index=log["logIndex"],
            order_hash=args["orderHash"].hex(),
            maker=args["maker"],
            taker=args["taker"],
            maker_asset_id=args["makerAssetId"],
            taker_asset_id=args["takerAssetId"],
            maker_amount=args["makerAmountFilled"],
            taker_amount=args["takerAmountFilled"],
            fee=args["fee"],
        )

    @staticmethod
    def _should_split_log_range(exc: Exception) -> bool:
        err = str(exc).lower()
        return any(
            token in err
            for token in (
                "too large",
                "limit exceeded",
                "query returned more than",
                "bad request",
                "400 client error",
                "413",
                "response size",
            )
        )

    def get_trades(
        self,
        from_block: int,
        to_block: int,
        contract_address: str = CTF_EXCHANGE,
    ) -> list[BlockchainTrade]:
        """Fetch OrderFilled events from a block range."""
        contract = self.ctf_exchange if contract_address.lower() == CTF_EXCHANGE.lower() else self.negrisk_exchange

        try:
            logs = _rpc_call_with_retry(
                lambda: self.w3.eth.get_logs(
                    {
                        "address": Web3.to_checksum_address(contract_address),
                        "topics": [ORDER_FILLED_TOPIC],
                        "fromBlock": from_block,
                        "toBlock": to_block,
                    }
                ),
                f"get_logs({from_block}-{to_block})",
            )
        except Exception as exc:
            if self._should_split_log_range(exc) and to_block > from_block:
                mid = (from_block + to_block) // 2
                return self.get_trades(from_block, mid, contract_address) + self.get_trades(
                    mid + 1, to_block, contract_address
                )
            raise

        trades = []
        for log in logs:
            try:
                trade = self._decode_order_filled(log, contract)
                trades.append(trade)
            except Exception as e:
                print(f"Error decoding log: {e}")

        return trades

    def _fetch_chunk(self, start: int, end: int, contract_address: str) -> tuple[list[BlockchainTrade], int, int]:
        """Fetch a single chunk of trades. Used by thread pool."""
        try:
            trades = self.get_trades(start, end, contract_address)
            return trades, start, end
        except Exception as e:
            if "too large" in str(e).lower():
                # Split into two halves and fetch sequentially
                mid = (start + end) // 2
                t1, _, _ = self._fetch_chunk(start, mid, contract_address)
                t2, _, _ = self._fetch_chunk(mid + 1, end, contract_address)
                return t1 + t2, start, end
            else:
                print(f"Error fetching blocks {start}-{end}: {e}")
                return [], start, end

    def iter_trades(
        self,
        from_block: int,
        to_block: Optional[int] = None,
        chunk_size: int = 1000,
        contract_address: str = CTF_EXCHANGE,
        max_workers: int = 5,
    ) -> Generator[tuple[list[BlockchainTrade], int, int], None, None]:
        """Iterate through trades in chunks of blocks using parallel fetching.

        Args:
            from_block: Starting block number
            to_block: Ending block number (default: latest)
            chunk_size: Number of blocks per query
            contract_address: CTF_EXCHANGE or NEGRISK_CTF_EXCHANGE
            max_workers: Number of parallel threads

        Yields:
            Tuples of (trades, chunk_start, chunk_end)
        """
        if to_block is None:
            to_block = self.get_block_number()

        # Build list of chunk ranges
        ranges = []
        current = from_block
        while current <= to_block:
            end = min(current + chunk_size - 1, to_block)
            ranges.append((current, end))
            current = end + 1

        # Process in batches of max_workers
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for batch_start in range(0, len(ranges), max_workers):
                batch = ranges[batch_start : batch_start + max_workers]
                futures = {
                    executor.submit(self._fetch_chunk, start, end, contract_address): (start, end)
                    for start, end in batch
                }

                # Yield results in order
                results = {}
                for future in concurrent.futures.as_completed(futures):
                    trades, start, end = future.result()
                    results[(start, end)] = trades

                for start, end in batch:
                    yield results[(start, end)], start, end


# Polymarket CTF Exchange created at block 33605403
POLYMARKET_START_BLOCK = int(os.getenv("POLYMARKET_START_BLOCK", "33605403"))


def get_deployment_block() -> int:
    """Get approximate block when Polymarket CTF Exchange was deployed."""
    return POLYMARKET_START_BLOCK
