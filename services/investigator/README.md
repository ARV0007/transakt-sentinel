# investigator

LangGraph agent that investigates risky payments and **pauses for an analyst's decision**.

```
supervisor -> [velocity | amount | merchant-profile analysts, in parallel, via MCP tools]
           -> synthesize (LLM, with rules fallback + grounding check)
           -> human_review (interrupt: waits for the analyst) -> finalize
```

- Consumes `fraud.cases` (from risk-scorer). One open case per merchant; later flags attach to it.
- Checkpoints every step to SQLite, so a paused case survives restarts and crashes resume.
- LLM: any OpenAI-compatible endpoint. Default is Hugging Face Inference Providers
  (`LLM_BASE_URL=https://router.huggingface.co/v1`, `LLM_API_KEY=<HF token>`). With no key,
  deterministic rules write the recommendation and everything still works.
- LangSmith tracing: set `LANGSMITH_TRACING=true` and `LANGSMITH_API_KEY`.

Review page: http://localhost:8003

API: `GET /cases?status=AWAITING_REVIEW`, `GET /cases/{id}`,
`POST /cases/{id}/decision {"decision": "CONFIRM_FRAUD"|"DISMISS", "note": "..."}`

Tests: `pip install -r requirements-dev.txt && pytest`
