"""Stand-ins for the MCP server and the LLM, so the graph can be tested offline."""

from app.toolbox import ToolError

# merchant id -> what the MCP tools would report about it
MERCHANTS = {
    "burst": {"activity30": {"payments": 31, "failed": 2, "max_in_any_2_minutes": 30, "avg_per_minute": 1.03},
              "activity1440": {"payments": 36, "failed": 3, "max_in_any_2_minutes": 30, "avg_per_minute": 0.03},
              "profile": {"payments": 35, "median_paise": 80_000, "p95_paise": 120_000, "max_paise": 130_000},
              "merchant": {"id": "burst", "name": "Shop 03", "account_age_minutes": 2900.0}},
    "spike": {"activity30": {"payments": 2, "failed": 0, "max_in_any_2_minutes": 1, "avg_per_minute": 0.07},
              "activity1440": {"payments": 9, "failed": 0, "max_in_any_2_minutes": 2, "avg_per_minute": 0.01},
              "profile": {"payments": 8, "median_paise": 60_000, "p95_paise": 80_000, "max_paise": 85_000},
              "merchant": {"id": "spike", "name": "Shop 07", "account_age_minutes": 5000.0}},
    "calm": {"activity30": {"payments": 1, "failed": 0, "max_in_any_2_minutes": 1, "avg_per_minute": 0.03},
             "activity1440": {"payments": 20, "failed": 1, "max_in_any_2_minutes": 2, "avg_per_minute": 0.01},
             "profile": {"payments": 19, "median_paise": 150_000, "p95_paise": 300_000, "max_paise": 320_000},
             "merchant": {"id": "calm", "name": "Shop 11", "account_age_minutes": 9000.0}},
}


# payment id -> what get_payment returns
PAYMENTS = {
    "11111111-2222-3333-4444-555555555555": {"id": "11111111-2222-3333-4444-555555555555", "merchantId": "burst",
                                             "amountPaise": 80_000, "currency": "INR", "status": "CAPTURED"},
}


class FakeTools:
    def __init__(self, broken=()):
        self.calls = []
        self.broken = set(broken)

    async def call(self, name, **args):
        self.calls.append((name, args))
        if name in self.broken or "*" in self.broken:
            raise ToolError(f"{name} failed: connection refused")
        if name == "get_payment":
            if args["payment_id"] not in PAYMENTS:
                raise ToolError(f"payment {args['payment_id']} not found")
            return dict(PAYMENTS[args["payment_id"]])
        m = MERCHANTS[args["merchant_id"]]
        if name == "merchant_activity":
            return dict(m["activity30" if args["window_minutes"] <= 30 else "activity1440"],
                        window_minutes=args["window_minutes"])
        if name == "merchant_amount_profile":
            return dict(m["profile"])
        if name == "get_merchant":
            return dict(m["merchant"])
        raise ToolError(f"no tool {name}")


class Reply:
    def __init__(self, content):
        self.content = content


class FakeLLM:
    def __init__(self, content=None, error=None):
        self.content, self.error, self.calls = content, error, 0

    async def ainvoke(self, messages, config=None):
        self.calls += 1
        if self.error:
            raise self.error
        return Reply(self.content)


def case_msg(merchant, payment="p1", amount=80_000, reasons=("VELOCITY",), score=0.97):
    return {"payment_id": payment, "merchant_id": merchant, "amount_paise": amount, "currency": "INR",
            "status": "CAPTURED", "score": score, "reasons": list(reasons), "features": {}}
