"""
Every score the service produces, kept in SQLite so the eval script and the
API can see what was decided and why. SQLite is enough: one writer, one file,
and the volume survives container restarts.
"""

import json
import math
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS scores (
    payment_id   TEXT PRIMARY KEY,   -- a redelivered Kafka event cannot double-count
    merchant_id  TEXT NOT NULL,
    amount_paise INTEGER NOT NULL,
    status       TEXT,
    score        REAL NOT NULL,
    flagged      INTEGER NOT NULL,
    reasons      TEXT NOT NULL,
    features     TEXT NOT NULL,
    event_ts     REAL NOT NULL,
    scored_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_scores_flagged ON scores(flagged, scored_at);
"""


def _clean(feats):
    # JSON has no NaN; store "unknown" as null
    return {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in feats.items()}


class ScoreStore:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        with self.lock:
            self.conn.executescript(SCHEMA)

    def get(self, payment_id):
        with self.lock:
            r = self.conn.execute("SELECT * FROM scores WHERE payment_id=?", (payment_id,)).fetchone()
        if r is None:
            return None
        d = dict(r)
        d["flagged"], d["reasons"], d["features"] = bool(d["flagged"]), json.loads(d["reasons"]), json.loads(d["features"])
        return d

    def seen(self, payment_id):
        with self.lock:
            return self.conn.execute("SELECT 1 FROM scores WHERE payment_id=?", (payment_id,)).fetchone() is not None

    def save(self, payment_id, merchant_id, amount_paise, status, score, flagged, reasons, feats, event_ts):
        self.save_many([(payment_id, merchant_id, amount_paise, status, score, flagged, reasons, feats, event_ts)])

    def save_many(self, rows):
        """All rows in one transaction: one disk sync per batch, not per payment."""
        now = time.time()
        with self.lock, self.conn:
            self.conn.executemany(
                "INSERT OR IGNORE INTO scores VALUES (?,?,?,?,?,?,?,?,?,?)",
                [(pid, mid, amt, st, score, int(flagged), json.dumps(reasons), json.dumps(_clean(feats)), ts, now)
                 for pid, mid, amt, st, score, flagged, reasons, feats, ts in rows])

    def list(self, flagged=None, limit=200):
        q, args = "SELECT * FROM scores", []
        if flagged is not None:
            q += " WHERE flagged=?"
            args.append(int(flagged))
        q += " ORDER BY scored_at DESC LIMIT ?"
        args.append(limit)
        with self.lock:
            rows = self.conn.execute(q, args).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["flagged"] = bool(d["flagged"])
            d["reasons"] = json.loads(d["reasons"])
            d["features"] = json.loads(d["features"])
            out.append(d)
        return out

    def stats(self):
        with self.lock:
            total, flagged = self.conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(flagged),0) FROM scores").fetchone()
        return {"scored": total, "flagged": flagged,
                "flag_rate": round(flagged / total, 4) if total else 0.0}
