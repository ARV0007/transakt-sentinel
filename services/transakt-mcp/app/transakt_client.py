"""
A small admin client for Transakt's REST API.

The fraud system needs to look ACROSS merchants, and Transakt already allows
exactly that for an ADMIN caller: GET /api/v1/payments returns every merchant's
payments when the JWT carries ROLE_ADMIN. So this client logs in as an admin and
nothing in Transakt had to change.

Login is lazy and retried: at startup the admin account may not exist yet (the
bootstrap step creates it), and crashing the service over that would turn a
two-second ordering problem into a restart loop. JWTs expire after an hour, so
the token is refreshed early and also re-fetched on any 401/403.
"""

import time
from datetime import datetime

import httpx

TOKEN_TTL_S = 50 * 60      # Transakt issues 1-hour JWTs; refresh 10 minutes early
PAGE_SIZE = 100            # Transakt's max-page-size
MAX_PAGES = 50             # hard ceiling on one window scan (5,000 payments)


class TransaktError(RuntimeError):
    pass


def parse_ts(value):
    """Transakt serialises Instants as ISO-8601 strings ending in Z."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class TransaktClient:
    def __init__(self, base_url, email, password, http=None):
        self.base_url = base_url.rstrip("/")
        self.email = email
        self.password = password
        self.http = http or httpx.AsyncClient(timeout=10.0)
        self._token = None
        self._token_at = 0.0

    async def _login(self):
        r = await self.http.post(f"{self.base_url}/api/v1/auth/login",
                                 json={"email": self.email, "password": self.password})
        if r.status_code != 200:
            raise TransaktError(f"admin login failed ({r.status_code}); has bootstrap run?")
        self._token = r.json()["token"]
        self._token_at = time.monotonic()

    async def _get(self, path, params=None):
        if not self._token or time.monotonic() - self._token_at > TOKEN_TTL_S:
            await self._login()
        for attempt in (1, 2):
            r = await self.http.get(f"{self.base_url}{path}", params=params,
                                    headers={"Authorization": f"Bearer {self._token}"})
            # Transakt answers an expired JWT with an empty 403, not a 401.
            if r.status_code in (401, 403) and attempt == 1:
                await self._login()
                continue
            if r.status_code == 404:
                return None
            if r.status_code != 200:
                raise TransaktError(f"GET {path} -> {r.status_code}")
            return r.json()

    async def get_payment(self, payment_id):
        return await self._get(f"/api/v1/payments/{payment_id}")

    async def get_ledger(self, payment_id):
        return await self._get(f"/api/v1/payments/{payment_id}/ledger")

    async def get_merchant(self, merchant_id):
        return await self._get(f"/api/v1/merchants/{merchant_id}")

    async def payments_since(self, since, merchant_id=None):
        """
        Every payment created at or after `since`, newest first.

        Transakt's admin list has no merchant filter, so this walks pages sorted
        newest-first and stops as soon as it passes `since`. The cost grows with
        TOTAL traffic in the window, not one merchant's — fine for a demo, and the
        documented v2 fix is an admin `?merchantId=` filter in Transakt.
        """
        out = []
        for page in range(MAX_PAGES):
            body = await self._get("/api/v1/payments", {
                "page": page, "size": PAGE_SIZE, "sort": "createdAt,desc"})
            content = (body or {}).get("content", [])
            for p in content:
                if p.get("createdAt") is None:
                    continue
                if parse_ts(p["createdAt"]) < since:
                    return out
                if merchant_id is None or p.get("merchantId") == merchant_id:
                    out.append(p)
            if len(content) < PAGE_SIZE:
                return out
        return out
