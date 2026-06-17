"""Regression tests for Polymarket trade range splitting."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest
from requests import Response
from requests.exceptions import HTTPError

from src.indexers.polymarket.blockchain import CTF_EXCHANGE, PolygonClient
from src.indexers.polymarket.trade_batch import TradeBatch


def _fake_append_log(batch: TradeBatch, log: dict, *, contract_name: str, fetched_at: datetime) -> None:
    block_number = int(log["blockNumber"])
    batch.block_number.append(block_number)
    batch.transaction_hash.append("0xabc")
    batch.log_index.append(0)
    batch.order_hash.append("0xdef")
    batch.maker.append("0x0000000000000000000000000000000000000001")
    batch.taker.append("0x0000000000000000000000000000000000000002")
    batch.maker_asset_id.append("0")
    batch.taker_asset_id.append("1")
    batch.maker_amount.append(1)
    batch.taker_amount.append(1)
    batch.fee.append(0)
    batch.timestamp.append(None)
    batch._fetched_at.append(fetched_at)
    batch._contract.append(contract_name)


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
    monkeypatch.setattr(TradeBatch, "append_log", _fake_append_log)

    client = PolygonClient.__new__(PolygonClient)
    client.w3 = SimpleNamespace(eth=SimpleNamespace(get_logs=fake_get_logs))

    batch = PolygonClient.get_trades(client, 1, 8, CTF_EXCHANGE)

    assert len(batch) == 8
    assert calls[0] == (1, 8)
    assert any((end - start + 1) < 8 for start, end in calls)
    assert max(end - start + 1 for start, end in calls) == 8


def test_fetch_chunk_stops_on_single_block_bad_request(monkeypatch: pytest.MonkeyPatch):
    from src.indexers.polymarket import blockchain as mod

    calls: list[tuple[int, int]] = []

    def fake_get_trades(start: int, end: int, contract_address: str, **kwargs):
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

    batch, start, end = PolygonClient._fetch_chunk(client, 123, 123, CTF_EXCHANGE)

    assert len(batch) == 0
    assert (start, end) == (123, 123)
    assert calls == [(123, 123)]
