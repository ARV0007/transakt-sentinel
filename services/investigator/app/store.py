"""
The case table: what an analyst's list view needs, kept beside the graph's own
checkpoints. The checkpoint is the source of truth for a case's progress; this
table is an index over it.

One merchant, one open case. A 30-payment velocity burst produces dozens of
flags, and an analyst wants ONE case that says "this merchant, these payments",
not thirty. Later flags attach to the open case instead of opening new ones.
"""

import json
import sqlite3
import threading
import time
import uuid

OPEN = ("INVESTIGATING", "AWAITING_REVIEW", "DECIDING")

SCHEMA = """
CREATE TABLE IF NOT EXISTS cases (
    id              TEXT PRIMARY KEY,
    merchant_id     TEXT NOT NULL,
    status          TEXT NOT NULL,
    opened_at       REAL NOT NULL,
    updated_at      REAL NOT NULL,
    score           REAL,
    reasons         TEXT NOT NULL,
    trigger         TEXT NOT NULL,
    payment_ids     TEXT NOT NULL,
    recommendation  TEXT,
    findings        TEXT,
    decision        TEXT,
    error           TEXT
);
CREATE INDEX IF NOT EXISTS idx_cases_merchant ON cases(merchant_id, status);
CREATE INDEX IF NOT EXISTS idx_cases_status ON cases(status, opened_at);
-- Which case each flagged payment belongs to. The primary key makes a redelivered
-- Kafka message a lookup instead of a duplicate case.
CREATE TABLE IF NOT EXISTS case_payments (
    payment_id  TEXT PRIMARY KEY,
    case_id     TEXT NOT NULL REFERENCES cases(id)
);
"""

JSON_COLS = ("reasons", "trigger", "payment_ids", "recommendation", "findings", "decision")


class CaseStore:
    def __init__(self, path, dedup_window_s=30 * 60):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        self.dedup_window_s = dedup_window_s
        with self.lock:
            self.conn.executescript(SCHEMA)

    def _row(self, r):
        if r is None:
            return None
        d = dict(r)
        for c in JSON_COLS:
            d[c] = json.loads(d[c]) if d[c] else None
        return d

    def open_or_attach(self, msg):
        """Returns (case_id, is_new). Safe to call twice with the same message."""
        now = time.time()
        with self.lock, self.conn:
            seen = self.conn.execute("SELECT case_id FROM case_payments WHERE payment_id=?",
                                     (msg["payment_id"],)).fetchone()
            if seen:
                return seen["case_id"], False
            rows = self.conn.execute(
                f"SELECT * FROM cases WHERE merchant_id=? AND status IN ({','.join('?' * len(OPEN))}) "
                "AND opened_at > ? ORDER BY opened_at DESC LIMIT 1",
                (msg["merchant_id"], *OPEN, now - self.dedup_window_s)).fetchall()
            if rows:
                case = self._row(rows[0])
                ids = case["payment_ids"] + [msg["payment_id"]]
                reasons = sorted(set(case["reasons"]) | set(msg.get("reasons", [])))
                self.conn.execute(
                    "UPDATE cases SET payment_ids=?, reasons=?, score=MAX(score, ?), updated_at=? WHERE id=?",
                    (json.dumps(ids), json.dumps(reasons), msg.get("score", 0), now, case["id"]))
                self.conn.execute("INSERT INTO case_payments VALUES (?,?)", (msg["payment_id"], case["id"]))
                return case["id"], False
            case_id = "case_" + uuid.uuid4().hex[:12]
            self.conn.execute(
                "INSERT INTO cases (id, merchant_id, status, opened_at, updated_at, score, reasons, trigger, payment_ids) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (case_id, msg["merchant_id"], "INVESTIGATING", now, now, msg.get("score"),
                 json.dumps(msg.get("reasons", [])), json.dumps(msg), json.dumps([msg["payment_id"]])))
            self.conn.execute("INSERT INTO case_payments VALUES (?,?)", (msg["payment_id"], case_id))
            return case_id, True

    def get(self, case_id):
        with self.lock:
            return self._row(self.conn.execute("SELECT * FROM cases WHERE id=?", (case_id,)).fetchone())

    def list(self, status=None, limit=200):
        q, args = "SELECT * FROM cases", []
        if status:
            q += " WHERE status=?"
            args.append(status)
        q += " ORDER BY opened_at DESC LIMIT ?"
        args.append(limit)
        with self.lock:
            return [self._row(r) for r in self.conn.execute(q, args).fetchall()]

    def update(self, case_id, **fields):
        fields["updated_at"] = time.time()
        for c in JSON_COLS:
            if c in fields and fields[c] is not None:
                fields[c] = json.dumps(fields[c])
        cols = ", ".join(f"{k}=?" for k in fields)
        with self.lock, self.conn:
            self.conn.execute(f"UPDATE cases SET {cols} WHERE id=?", (*fields.values(), case_id))

    def claim_for_decision(self, case_id):
        """
        Atomic check-and-set. Two analysts clicking at the same moment both send
        a decision; exactly one UPDATE matches the AWAITING_REVIEW row, and the
        other gets a 409 — the same race Transakt's idempotency table settles.
        """
        with self.lock, self.conn:
            cur = self.conn.execute(
                "UPDATE cases SET status='DECIDING', updated_at=? WHERE id=? AND status='AWAITING_REVIEW'",
                (time.time(), case_id))
            return cur.rowcount == 1

    def stats(self):
        with self.lock:
            rows = self.conn.execute("SELECT status, COUNT(*) FROM cases GROUP BY status").fetchall()
        return {r[0]: r[1] for r in rows}
