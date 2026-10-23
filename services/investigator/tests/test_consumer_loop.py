"""
The investigator's Kafka loop (app.main.consume_cases) with aiokafka's consumer
replaced by an in-memory fake: messages become cases, a malformed message is
skipped (and committed, so it can't block the partition), and the offset is
only committed once the case row exists. That a redelivered message doesn't
duplicate a case is the case store's job — see test_api.py.
"""

import asyncio
import json
from types import SimpleNamespace

import aiokafka
import pytest

from app import main as inv_main

from .fakes import case_msg


class FakeConsumer:
    messages, log, stop = [], None, None

    def __init__(self, *topics, **config):
        pass

    async def start(self):
        pass

    async def stop(self):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not FakeConsumer.messages:
            FakeConsumer.stop.set()
            raise StopAsyncIteration
        return FakeConsumer.messages.pop(0)

    async def commit(self):
        FakeConsumer.log.append("commit")


class RecordingService:
    """Stands in for CaseService.open; records the order of opens and commits."""
    def __init__(self, log):
        self.log, self.cases = log, {}

    def open(self, msg):
        merchant = msg["merchant_id"]
        is_new = merchant not in self.cases
        self.cases.setdefault(merchant, set()).add(msg["payment_id"])
        self.log.append(("open", msg["payment_id"]))
        return f"case-{merchant}", is_new


def kmsg(body):
    return SimpleNamespace(value=body if isinstance(body, bytes) else json.dumps(body).encode())


@pytest.fixture
def run(monkeypatch):
    log, stop = [], asyncio.Event()
    FakeConsumer.log, FakeConsumer.stop = log, stop
    monkeypatch.setattr(aiokafka, "AIOKafkaConsumer", FakeConsumer)

    async def go(messages):
        FakeConsumer.messages = list(messages)
        service = RecordingService(log)
        await asyncio.wait_for(inv_main.consume_cases(service, stop), 10)
        return service, log
    return go


async def test_each_message_is_committed_only_after_its_case_is_opened(run):
    service, log = await run([kmsg(case_msg("burst", "p1")), kmsg(case_msg("burst", "p2")),
                              kmsg(case_msg("spike", "p9", reasons=["AMOUNT_SPIKE"]))])
    assert log == [("open", "p1"), "commit", ("open", "p2"), "commit", ("open", "p9"), "commit"]
    assert service.cases == {"burst": {"p1", "p2"}, "spike": {"p9"}}


async def test_malformed_messages_are_skipped_and_still_committed(run):
    # Not committing a poison message would redeliver it forever and block the partition.
    service, log = await run([kmsg(b"{not json"), kmsg({"merchant_id": "burst"}), kmsg(case_msg("burst", "p1"))])
    assert [e for e in log if e != "commit"] == [("open", "p1")]
    assert log.count("commit") == 3
