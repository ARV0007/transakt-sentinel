"""
A stand-in for Transakt that speaks the same HTTP API, for end-to-end tests
of Sentinel without Java, Postgres or Kafka. NOT a Transakt replacement:
it implements only the endpoints Sentinel and the simulator touch.

Settled payments are kept in order and exposed at GET /_events, which the
e2e runner replays into the scorer the way Kafka would deliver them.
"""

import random
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, Header, HTTPException, Request

app = FastAPI()
MERCHANTS, KEYS, TOKENS, PAYMENTS, EVENTS = {}, {}, {}, [], []
rng = random.Random(3)


def now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def caller(authorization, api_key):
    if api_key and api_key in KEYS:
        return MERCHANTS[KEYS[api_key]]
    if authorization and authorization.startswith("Bearer ") and authorization[7:] in TOKENS:
        return MERCHANTS[TOKENS[authorization[7:]]]
    raise HTTPException(403)


def public(m):
    return {k: v for k, v in m.items() if k not in ("password",)}


@app.get("/api/v1/health")
def health():
    return {"status": "UP", "service": "fake-transakt"}


@app.post("/api/v1/merchants")
async def signup(req: Request):
    b = await req.json()
    if any(m["email"] == b["email"] for m in MERCHANTS.values()):
        raise HTTPException(500, "duplicate email")
    mid, key = str(uuid.uuid4()), "tk_" + uuid.uuid4().hex
    MERCHANTS[mid] = {"id": mid, "name": b["name"], "email": b["email"], "businessName": b.get("businessName"),
                      "password": b["password"], "role": "MERCHANT", "createdAt": now(), "apiKeyPrefix": key[:8]}
    KEYS[key] = mid
    return {**public(MERCHANTS[mid]), "apiKey": key}


@app.post("/_promote/{email}")
def promote(email: str):
    for m in MERCHANTS.values():
        if m["email"] == email:
            m["role"] = "ADMIN"
            return {"ok": True}
    raise HTTPException(404)


@app.post("/api/v1/auth/login")
async def login(req: Request):
    b = await req.json()
    for m in MERCHANTS.values():
        if m["email"] == b.get("email") and m["password"] == b.get("password"):
            tok = uuid.uuid4().hex
            TOKENS[tok] = m["id"]
            return {"token": tok, "merchantId": m["id"], "email": m["email"]}
    raise HTTPException(401)


@app.post("/api/v1/payments")
async def pay(req: Request, x_api_key: str = Header(None), authorization: str = Header(None)):
    m = caller(authorization, x_api_key)
    b = await req.json()
    p = {"id": str(uuid.uuid4()), "merchantId": m["id"], "amountPaise": b["amountPaise"],
         "currency": b["currency"], "status": "FAILED" if rng.random() < 0.1 else "CAPTURED", "createdAt": now()}
    PAYMENTS.append(p)
    EVENTS.append({"paymentId": p["id"], "merchantId": p["merchantId"], "status": p["status"],
                   "amountPaise": p["amountPaise"], "currency": p["currency"], "_ts": p["createdAt"]})
    return p


def admin(authorization):
    m = caller(authorization, None)
    if m["role"] != "ADMIN":
        raise HTTPException(403)


@app.get("/api/v1/payments")
def list_payments(page: int = 0, size: int = 20, authorization: str = Header(None), sort: str = ""):
    admin(authorization)
    rows = sorted(PAYMENTS, key=lambda p: p["createdAt"], reverse=True)
    size = min(size, 100)
    chunk = rows[page * size:(page + 1) * size]
    return {"content": chunk, "page": {"size": size, "number": page, "totalElements": len(rows),
                                       "totalPages": (len(rows) + size - 1) // size}}


@app.get("/api/v1/payments/{pid}")
def get_payment(pid: str, authorization: str = Header(None)):
    admin(authorization)
    for p in PAYMENTS:
        if p["id"] == pid:
            return p
    raise HTTPException(404)


@app.get("/api/v1/payments/{pid}/ledger")
def ledger(pid: str, authorization: str = Header(None)):
    p = get_payment(pid, authorization)
    if p["status"] != "CAPTURED":
        return []
    return [{"paymentId": pid, "account": "merchant:" + p["merchantId"], "direction": "CREDIT", "amountPaise": p["amountPaise"]},
            {"paymentId": pid, "account": "gateway:incoming", "direction": "DEBIT", "amountPaise": p["amountPaise"]}]


@app.get("/api/v1/merchants/{mid}")
def get_merchant(mid: str, authorization: str = Header(None)):
    admin(authorization)
    if mid not in MERCHANTS:
        raise HTTPException(404)
    return public(MERCHANTS[mid])


@app.get("/_events")
def events():
    return EVENTS
