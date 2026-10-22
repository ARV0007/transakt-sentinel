"""
Runs cases through the graph and keeps the case table in step with the
graph's checkpoints.

Every case is a LangGraph thread whose id is the case id. That one choice is
what makes the system durable: the checkpointer saves the case file after
every node, so a case can be paused for a human, survive a restart, or be
resumed after a crash mid-investigation, all from the same saved state.
"""

import asyncio
import logging

from langgraph.types import Command

log = logging.getLogger("investigator.service")


class CaseError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


class CaseService:
    def __init__(self, store, graph, max_concurrent=4):
        self.store = store
        self.graph = graph
        self.sem = asyncio.Semaphore(max_concurrent)
        self.tasks = set()

    @staticmethod
    def _cfg(case_id):
        return {"configurable": {"thread_id": case_id},
                "run_name": f"investigate-{case_id}", "tags": ["fraud-case"],
                "metadata": {"case_id": case_id}}

    # ---------------------------------------------------------------- intake

    def open(self, msg):
        """Open a case for a flagged payment, or attach it to the merchant's open one."""
        case_id, is_new = self.store.open_or_attach(msg)
        if is_new:
            self.spawn(case_id)
        return case_id, is_new

    def spawn(self, case_id):
        t = asyncio.create_task(self.run(case_id))
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)

    async def recover(self):
        """At startup: anything still INVESTIGATING was interrupted by a crash or
        restart mid-run. Resume it from its last checkpoint."""
        stuck = self.store.list(status="INVESTIGATING")
        for case in stuck:
            log.info("resuming %s after restart", case["id"])
            self.spawn(case["id"])
        return len(stuck)

    # ---------------------------------------------------------------- running

    def _initial_state(self, case):
        t = case["trigger"]
        return {"case_id": case["id"], "merchant_id": case["merchant_id"],
                "payment": {"id": t["payment_id"], "amount_paise": t["amount_paise"],
                            "currency": t.get("currency", "INR"), "status": t.get("status")},
                "score": t.get("score"), "reasons": t.get("reasons", []),
                "features": t.get("features", {})}

    async def run(self, case_id):
        case = self.store.get(case_id)
        cfg = self._cfg(case_id)
        async with self.sem:
            try:
                snap = await self.graph.aget_state(cfg)
                # No checkpoint yet -> start fresh. A checkpoint -> continue from it.
                await self.graph.ainvoke(None if snap.values else self._initial_state(case), cfg)
            except Exception as e:
                log.exception("case %s failed", case_id)
                self.store.update(case_id, status="ERROR", error=str(e)[:500])
                return
        await self.sync(case_id)

    async def sync(self, case_id):
        """Copy what the checkpoint says into the case table."""
        snap = await self.graph.aget_state(self._cfg(case_id))
        v = snap.values
        waiting = any(t.interrupts for t in snap.tasks)
        status = "AWAITING_REVIEW" if waiting else ("CLOSED" if not snap.next else "INVESTIGATING")
        self.store.update(case_id, status=status, recommendation=v.get("recommendation"),
                          findings=v.get("findings"), decision=v.get("decision"), error=None)
        return status

    # ---------------------------------------------------------------- decisions

    async def decide(self, case_id, decision, note="", analyst="analyst"):
        case = self.store.get(case_id)
        if not case:
            raise CaseError(404, "case not found")
        if not self.store.claim_for_decision(case_id):
            raise CaseError(409, f"case is {case['status']}, not AWAITING_REVIEW")
        try:
            await self.graph.ainvoke(
                Command(resume={"decision": decision, "note": note, "analyst": analyst}),
                self._cfg(case_id))
        except ValueError as e:
            self.store.update(case_id, status="AWAITING_REVIEW")
            raise CaseError(422, str(e))
        except Exception as e:
            self.store.update(case_id, status="AWAITING_REVIEW")
            raise CaseError(500, f"could not resume case: {e}")
        return await self.sync(case_id)

    async def retry(self, case_id):
        case = self.store.get(case_id)
        if not case:
            raise CaseError(404, "case not found")
        if case["status"] != "ERROR":
            raise CaseError(409, f"case is {case['status']}; only ERROR cases can be retried")
        self.store.update(case_id, status="INVESTIGATING", error=None)
        self.spawn(case_id)
