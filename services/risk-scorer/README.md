# risk-scorer

Fast ML risk scoring for every settled payment. **No LLM on this path.**

- Consumes `payment.settled` (Transakt's outbox topic) as consumer group `risk-scorer`.
- Builds streaming features per merchant (velocity, amount vs own median, account age…).
- Scores with a gradient-boosted classifier (scikit-learn), loaded from the Hugging Face
  Hub when `HF_MODEL_REPO` is set, otherwise trained into the image at build time.
- Publishes payments at or above `RISK_THRESHOLD` (default 0.5) to `fraud.cases`, keyed by merchant.

API (port 8002): `GET /health`, `GET /stats`, `GET /scores?flagged=true&limit=100`

Train / publish the model yourself:

```bash
python -m app.train --out model/model.joblib
HF_TOKEN=... python -m app.train --push-to-hub <your-hf-username>/transakt-sentinel-risk
```

Tests: `pip install -r requirements-dev.txt && pytest`
