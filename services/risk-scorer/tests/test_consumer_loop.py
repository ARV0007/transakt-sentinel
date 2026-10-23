"""
The Kafka consumer loop (app.main.consume_forever) with the aiokafka client
classes replaced by in-memory fakes. No broker — what this checks is the loop's
own logic: batched polls, time order across partitions, cases sent before the
offset is committed, malformed messages skipped, and a crash mid-batch losing
nothing.
"""

import asyncio
import json
from types import SimpleNamespace

import aiokafka
import pytest

from app import main as scorer_main
from app.model import RiskModel
from app.scoring import Scorer
from app.store import ScoreStore
from app.train import train


class Log:
    """Everything the fakes saw, in order."""
    def __init__(self):
        self.events = []          # ("send", key, case) | ("flush",) | ("commit", n_polled_so_far)


class FakeConsumer:
    polls = []                    # list of {partition: [msgs]}, consumed one per getmany
    log = None
    stop = None
    redeliver_on_restart = None   # a poll to hand out again after a crash, like Kafka would

    def __init__(self, *topics, **config):
        self.config = config

    async def start(self):
        if FakeConsumer.redeliver_on_restart is not None:
            FakeConsumer.polls.insert(0, FakeConsumer.redeliver_on_restart)
            FakeConsumer.redeliver_on_restart = None

    async def stop(self):
        pass

    async def getmany(self, timeout_ms=0, max_records=None):
        if not FakeConsumer.polls:
            FakeConsumer.stop.set()
            return {}
        self.current = FakeConsumer.polls.pop(0)
        assert max_records == scorer_main.MAX_BATCH
        return self.current

    async def commit(self):
        FakeConsumer.log.events.append(("commit",))


class FakeProducer:
    """Like the real one: send() only buffers; a message reaches the broker (the
    log) when flush() succeeds. A failed flush loses whatever was buffered."""
    log = None
    fail_next_flush = False

    def __init__(self, **config):
        self.buffer = []

    async def start(self):
        pass

    async def stop(self):
        self.buffer = []          # a stopped producer's unsent buffer is gone

    async def send(self, topic, value, key=None):
        self.buffer.append(("send", key.decode(), json.loads(value)))

    async def flush(self):
        if FakeProducer.fail_next_flush:
            FakeProducer.fail_next_flush = False
            self.buffer = []
            raise ConnectionError("broker went away mid-batch")
        FakeProducer.log.events.extend(self.buffer)
        self.buffer = []
        FakeProducer.log.events.append(("flush",))


def msg(offset, ts_ms, payment, merchant, amount):
    body = json.dumps({"paymentId": payment, "merchantId": merchant, "amountPaise": amount,
                       "currency": "INR", "status": "CAPTURED"}).encode()
    return SimpleNamespace(value=body, timestamp=ts_ms, offset=offset)


@pytest.fixture
def harness(monkeypatch, tmp_path):
    log, stop = Log(), asyncio.Event()
    FakeConsumer.polls, FakeConsumer.log, FakeConsumer.stop = [], log, stop
    FakeConsumer.redeliver_on_restart = None
    FakeProducer.log, FakeProducer.fail_next_flush = log, False
    monkeypatch.setattr(aiokafka, "AIOKafkaConsumer", FakeConsumer)
    monkeypatch.setattr(aiokafka, "AIOKafkaProducer", FakeProducer)

    async def no_wait(_):         # the loop backs off 2 s after an error; not in tests
        return None
    monkeypatch.setattr(scorer_main.asyncio, "sleep", no_wait)

    class Dir:
        async def created_ts(self, merchant_id):
            return 0.0
    bundle = train()
    bundle["source"] = "test"
    scorer = Scorer(RiskModel(bundle), ScoreStore(str(tmp_path / "s.db")), Dir())
    return SimpleNamespace(log=log, stop=stop, scorer=scorer)


def burst_polls():
    """A quiet shop, then a 30-payment burst, split across two partitions and two polls."""
    quiet = [msg(i, 1_000_000 + i * 60_000, f"q{i}", "shop", 80_000) for i in range(5)]
    burst = [msg(10 + i, 1_400_000 + i * 2_000, f"b{i}", "shop", 80_000) for i in range(30)]
    first, second = quiet + burst[:15], burst[15:]
    # partitions interleave: deliver each poll as two partitions, out of time order
    return [{0: first[1::2], 1: first[0::2]}, {0: second[1::2], 1: second[0::2]}]


async def test_cases_are_sent_and_flushed_before_the_offset_is_committed(harness):
    FakeConsumer.polls = burst_polls()
    await asyncio.wait_for(scorer_main.consume_forever(harness.scorer, harness.stop), 30)
    kinds = [e[0] for e in harness.log.events]
    sends = [e for e in harness.log.events if e[0] == "send"]
    assert len(sends) >= 18 and all(e[1] == "shop" for e in sends)        # keyed by merchant
    assert kinds.count("commit") == 2                                      # one per poll
    # Every commit comes straight after a flush: the offset never moves past a case
    # that hasn't reached the broker.
    assert all(kinds[i - 1] == "flush" for i, k in enumerate(kinds) if k == "commit")


async def test_time_order_across_partitions_is_restored(harness):
    # Same traffic, partitions interleaved: had the burst been scored out of order, its
    # 2-minute counts would differ. Compare with the traffic scored in true order.
    FakeConsumer.polls = burst_polls()
    await asyncio.wait_for(scorer_main.consume_forever(harness.scorer, harness.stop), 30)
    via_loop = {e[2]["payment_id"]: e[2]["features"]["count_2m"] for e in harness.log.events if e[0] == "send"}

    class Dir:
        async def created_ts(self, merchant_id):
            return 0.0
    bundle = train()
    bundle["source"] = "test"
    ordered = Scorer(RiskModel(bundle), ScoreStore(":memory:"), Dir())
    msgs = sorted((m for poll in burst_polls() for part in poll.values() for m in part), key=lambda m: m.timestamp)
    expected = {}
    for m in msgs:
        c = await ordered.handle(json.loads(m.value), m.timestamp / 1000)
        if c:
            expected[c["payment_id"]] = c["features"]["count_2m"]
    assert via_loop == expected


async def test_a_malformed_message_is_skipped_and_the_rest_scored(harness):
    good = burst_polls()[0]
    good[0] = good[0] + [SimpleNamespace(value=b"{not json", timestamp=1_500_000, offset=99),
                         SimpleNamespace(value=b'{"merchantId": "shop"}', timestamp=1_500_001, offset=100)]
    FakeConsumer.polls = [good]
    await asyncio.wait_for(scorer_main.consume_forever(harness.scorer, harness.stop), 30)
    assert any(e[0] == "send" for e in harness.log.events)
    assert [e[0] for e in harness.log.events][-1] == "commit"


async def test_a_crash_mid_batch_loses_no_case(harness):
    # The broker fails while the batch's cases are being flushed: no commit happens,
    # the consumer restarts, Kafka redelivers the same poll — and every flagged
    # payment's case still goes out (re-sent from the stored score).
    polls = burst_polls()
    FakeConsumer.polls = [polls[0]]
    FakeConsumer.redeliver_on_restart = polls[0]
    FakeProducer.fail_next_flush = True
    await asyncio.wait_for(scorer_main.consume_forever(harness.scorer, harness.stop), 30)
    # Only what a successful flush delivered counts; the failed attempt delivered nothing.
    sent_ids = {e[2]["payment_id"] for e in harness.log.events if e[0] == "send"}
    flagged = {r["payment_id"] for r in harness.scorer.store.list(flagged=True, limit=1000)}
    assert flagged and flagged <= sent_ids
    assert harness.scorer.store.stats()["scored"] == 20            # scored once, despite the redelivery
    assert [e[0] for e in harness.log.events].count("commit") == 1
