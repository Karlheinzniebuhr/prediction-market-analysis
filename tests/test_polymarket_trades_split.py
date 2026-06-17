"""Regression tests for Polymarket trade range splitting."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from requests import Response
from requests.exceptions import HTTPError

from src.indexers.polymarket.blockchain import BlockchainTrade, CTF_EXCHANGE, PolygonClient


def _make_trade(block_number: int) -> BlockchainTrade:
    return BlockchainTrade(
        block_number=block_number,
        transaction_hash="0x" + "11" * 32,
        log_index=0,
        order_hash="0x" + "22" * 32,
        maker="0x0000000000000000000000000000000000000001",
        taker="0x0000000000000000000000000000000000000002",
        maker_asset_id=0,
        taker_asset_id=1,
        maker_amount=1,
        taker_amount=1,
        fee=0,
    )


def test_get_trades_splits_on_bad_request(monkeypatch: pytest.MonkeyPatch):
    from src.indexers.polymarket import blockchain as mod

    calls: list[tuple[int, int]] = []

    def fake_get_logs(filter_params):
        start = int(filter_params["fromBlock"])
        end = int(filter_params["toBlock"])
        calls.append((start, end))
        if end - start + 1 > 3:
            response = Response()
            response.status_code = 400
            response.url = "https://polygon.drpc.org/"
            raise HTTPError(
                "400 Client Error: Bad Request for url: https://polygon.drpc.org/",
                response=response,
            )
        return [{"blockNumber": block} for block in range(start, end + 1)]

    monkeypatch.setattr(mod, "RPC_MAX_RETRIES", 1)
    monkeypatch.setattr(mod.time, "sleep", lambda *_args, **_kwargs: None)

    client = PolygonClient.__new__(PolygonClient)
    client.w3 = SimpleNamespace(eth=SimpleNamespace(get_logs=fake_get_logs))
    client._decode_order_filled = lambda log: _make_trade(log["blockNumber"])

    trades = PolygonClient.get_trades(client, 1, 8, CTF_EXCHANGE)

    assert len(trades) == 8
    assert calls[0] == (1, 8)
    assert any((end - start + 1) < 8 for start, end in calls)
    assert max(end - start + 1 for start, end in calls) == 8


def test_fetch_chunk_stops_on_single_block_bad_request(monkeypatch: pytest.MonkeyPatch):
    from src.indexers.polymarket import blockchain as mod

    calls: list[tuple[int, int]] = []

    def fake_get_trades(start: int, end: int, contract_address: str):
        calls.append((start, end))
        response = Response()
        response.status_code = 400
        response.url = "https://polygon.drpc.org/"
        raise HTTPError(
            "400 Client Error: Bad Request for url: https://polygon.drpc.org/",
            response=response,
        )

    monkeypatch.setattr(mod, "RPC_MAX_RETRIES", 1)
    monkeypatch.setattr(mod.time, "sleep", lambda *_args, **_kwargs: None)

    client = PolygonClient.__new__(PolygonClient)
    client.get_trades = fake_get_trades

    trades, start, end = PolygonClient._fetch_chunk(client, 123, 123, CTF_EXCHANGE)

    assert trades == []
    assert (start, end) == (123, 123)
    assert calls == [(123, 123)]
