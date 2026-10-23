"""
Trains the risk model and (optionally) publishes it to the Hugging Face Hub.

    python -m app.train --out model/model.skops
    python -m app.train --out model/model.skops --push-to-hub <user>/transakt-sentinel-risk

Why a gradient-boosted tree and not an LLM: real fraud teams score every
transaction with fast tabular models. This one answers in well under a
millisecond and costs nothing per call. The LLM is reserved for the handful of
cases that need reasoning — the cost argument for the whole architecture.

Where the training data comes from, honestly: the rows below are SYNTHETIC,
generated from the same fraud patterns the simulator plants. That makes the
headline accuracy optimistic — a model graded on the distribution it was
trained on. The eval script measures it against a live simulator run with its
own random seed, and the documented next step is mixing in a public labelled
dataset such as PaySim.
"""

import argparse
import json
import math
import os
import random

import numpy as np
import skops.io as sio
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split

from .features import FEATURE_ORDER

MODEL_VERSION = "2026.10-synthetic-v1"


def _poisson(rng, lam):
    # Knuth's method; fine for the small rates used here
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= L:
            return k
        k += 1


def _row(rng, kind):
    """One synthetic payment's features. kind is 'normal' or a fraud pattern."""
    median_paise = 10 ** rng.uniform(4, 6.7)           # Rs 100 .. Rs 50,000 shops
    history = int(10 ** rng.uniform(0, 2.5)) - 1        # 0 .. ~315, skewed young
    age = 10 ** rng.uniform(0, 5) if rng.random() > 0.1 else math.nan
    ratio = rng.lognormvariate(0, 0.4)
    c2 = 1 + _poisson(rng, rng.uniform(0.3, 3))
    c10 = c2 + _poisson(rng, rng.uniform(1, 8))
    failed = _poisson(rng, 0.1 * c10) / c10

    # Keep normal rows internally consistent with the account's age. A shop that
    # signed up minutes ago cannot have hundreds of past payments, and genuine
    # newcomers start small — legit high-ticket newcomers are the separate
    # "legit_new_big" rows below, and they are hours to days old. Without this,
    # "minutes old, moving Rs 30,000" was generated as normal half the time and
    # the model learned to shrug at the exact pattern it exists to catch.
    if age == age and age < 60:
        history = min(history, int(age))
        median_paise = min(median_paise, 10 ** rng.uniform(4, 5.7))   # <= ~Rs 5,000

    if kind == "velocity_burst":
        c2 = rng.randint(8, 40)
        c10 = c2 + rng.randint(0, 20)
        history = max(history, rng.randint(2, 60))
    elif kind == "amount_spike":
        ratio = rng.uniform(12, 90)
        history = max(history, rng.randint(2, 60))
    elif kind == "new_merchant_blitz":
        history = rng.randint(0, 14)
        age = rng.uniform(0, 45)
        median_paise = rng.uniform(1_200_000, 6_000_000)   # Rs 12k .. Rs 60k
        ratio = rng.lognormvariate(0, 0.3)
        c2 = rng.randint(1, 13)
        c10 = c2 + rng.randint(0, 10)
    elif kind == "legit_new_big":
        # Hard negatives: a genuinely new shop with big, steady tickets
        # (furniture, electronics). Without these, "new + expensive" alone
        # would be learned as fraud.
        history = rng.randint(0, 14)
        age = rng.uniform(60, 10_000)
        median_paise = rng.uniform(1_000_000, 4_000_000)
        c2 = 1 + _poisson(rng, 0.3)
        c10 = c2 + _poisson(rng, 1)

    amount = median_paise * ratio
    return {
        "count_2m": c2,
        "count_10m": c10,
        "history_count": history,
        "amount_ratio": ratio if history >= 2 else math.nan,
        "log_amount": math.log10(max(amount, 1)),
        "merchant_age_min": age,
        "failed_ratio_10m": failed,
    }


def make_dataset(n=40_000, seed=13):
    rng = random.Random(seed)
    mix = [("normal", 0.86), ("legit_new_big", 0.04),
           ("velocity_burst", 0.04), ("amount_spike", 0.03), ("new_merchant_blitz", 0.03)]
    kinds, weights = zip(*mix)
    X, y = [], []
    for _ in range(n):
        kind = rng.choices(kinds, weights)[0]
        f = _row(rng, kind)
        X.append([f[c] for c in FEATURE_ORDER])
        y.append(0 if kind in ("normal", "legit_new_big") else 1)
    return np.array(X, dtype=float), np.array(y)


# ---------------------------------------------------------------- analyst feedback
#
# Every case an analyst decides is a label a human checked: the most valuable
# training data the system ever sees, and the rarest. Each one is weighted as
# FEEDBACK_WEIGHT synthetic rows so a few dozen decisions can move the model,
# especially on the false alarms (DISMISS) that synthetic data never predicted.

FEEDBACK_WEIGHT = 25

# The gate. A retrained model only ships if it earns it on decisions it never saw:
# the newest HOLDOUT_SHARE of analyst decisions are held back as an exam. The
# newest, because the question is "does this model agree with what analysts are
# deciding NOW", and a random split would leak tomorrow into yesterday.
HOLDOUT_SHARE = 0.25
MIN_HOLDOUT = 4              # fewer than 2 x this many decisions: too few to judge, skip the gate
MAX_RECALL_DROP = 0.02       # may not forget more than 2 points of synthetic fraud recall


def load_feedback(path):
    """Rows from the investigator's GET /feedback, oldest decision first. A
    missing file means none yet."""
    if not path or not os.path.exists(path):
        return np.empty((0, len(FEATURE_ORDER))), np.empty(0, dtype=int)
    with open(path) as f:
        rows = sorted(json.load(f), key=lambda r: r.get("decided_at") or 0)
    X = [[math.nan if r["features"].get(c) is None else float(r["features"][c]) for c in FEATURE_ORDER]
         for r in rows]
    y = [int(r["label"]) for r in rows]
    return np.array(X, dtype=float).reshape(-1, len(FEATURE_ORDER)), np.array(y, dtype=int)


def agreement(model, X, y):
    if not len(y):
        return None
    return int(((model.predict_proba(X)[:, 1] >= 0.5).astype(int) == y).sum())


def train(seed=13, feedback_path=None):
    X, y = make_dataset(seed=seed)
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, random_state=seed, stratify=y)
    X_fb, y_fb = load_feedback(feedback_path)

    def fit(X_extra=None, y_extra=None):
        # NaN-aware: "merchant age unknown" and "too little history for a ratio"
        # are information, not errors, and this model handles missing values natively.
        m = HistGradientBoostingClassifier(max_iter=200, learning_rate=0.1, random_state=seed)
        if y_extra is not None and len(y_extra):
            m.fit(np.vstack([X_tr, X_extra]), np.concatenate([y_tr, y_extra]),
                  sample_weight=np.concatenate([np.ones(len(y_tr)), np.full(len(y_extra), FEEDBACK_WEIGHT)]))
        else:
            m.fit(X_tr, y_tr)
        return m

    def synthetic_recall(m):
        return float(recall_score(y_te, (m.predict_proba(X_te)[:, 1] >= 0.5).astype(int)))

    baseline = fit()
    gate = {"gate": "no feedback"}
    model = baseline
    if len(y_fb) >= 2 * MIN_HOLDOUT:
        n_hold = max(MIN_HOLDOUT, int(round(len(y_fb) * HOLDOUT_SHARE)))
        X_old, y_old, X_new, y_new = X_fb[:-n_hold], y_fb[:-n_hold], X_fb[-n_hold:], y_fb[-n_hold:]
        candidate = fit(X_old, y_old)
        base_agree, cand_agree = agreement(baseline, X_new, y_new), agreement(candidate, X_new, y_new)
        base_rec, cand_rec = synthetic_recall(baseline), synthetic_recall(candidate)
        gate = {"holdout_decisions": n_hold,
                "holdout_agreement_before": f"{base_agree}/{n_hold}",
                "holdout_agreement_candidate": f"{cand_agree}/{n_hold}",
                "synthetic_recall_before": round(base_rec, 4),
                "synthetic_recall_candidate": round(cand_rec, 4)}
        if cand_agree < base_agree:
            gate["gate"] = (f"REJECTED: agrees with {cand_agree}/{n_hold} of the newest analyst decisions, "
                            f"the current model {base_agree}/{n_hold}")
        elif cand_rec < base_rec - MAX_RECALL_DROP:
            gate["gate"] = (f"REJECTED: synthetic fraud recall would fall from {base_rec:.3f} to {cand_rec:.3f}")
        else:
            gate["gate"] = "passed"
            # Validated on the old decisions; now learn from all of them, newest included.
            model = fit(X_fb, y_fb)
    elif len(y_fb):
        gate = {"gate": f"skipped: {len(y_fb)} decisions, need {2 * MIN_HOLDOUT} to hold some back"}
        model = fit(X_fb, y_fb)

    proba = model.predict_proba(X_te)[:, 1]
    pred = (proba >= 0.5).astype(int)
    metrics = {
        "roc_auc": round(float(roc_auc_score(y_te, proba)), 4),
        "average_precision": round(float(average_precision_score(y_te, proba)), 4),
        "precision_at_0.5": round(float(precision_score(y_te, pred)), 4),
        "recall_at_0.5": round(float(recall_score(y_te, pred)), 4),
        "test_rows": int(len(y_te)),
        "fraud_share": round(float(y.mean()), 4),
        "feedback_rows": int(len(y_fb)),
        **gate,
    }
    used_feedback = model is not baseline
    if len(y_fb):
        # In-sample for the shipped model, so optimistic; the holdout figures above are the honest ones.
        metrics["agrees_with_analysts_before"] = f"{agreement(baseline, X_fb, y_fb)}/{len(y_fb)}"
        metrics["agrees_with_analysts_after"] = f"{agreement(model, X_fb, y_fb)}/{len(y_fb)}"
    version = MODEL_VERSION + (f"+fb{len(y_fb)}" if used_feedback else "")
    return {"model": model, "feature_order": FEATURE_ORDER, "threshold": 0.5,
            "version": version, "metrics": metrics}


MODEL_CARD = """---
library_name: sklearn
tags: [fraud-detection, tabular-classification, payments]
---
# Transakt Sentinel — payment risk scorer

A gradient-boosted classifier (scikit-learn `HistGradientBoostingClassifier`) that
scores payments from the Transakt payment gateway for merchant fraud.

**Features:** {features}

**Training data:** synthetic, generated from the same fraud patterns the project's
simulator plants (velocity bursts, amount spikes, new-merchant blitzes) plus hard
negatives. Metrics on a held-out synthetic split are therefore optimistic.

**Held-out metrics:** `{metrics}`

**Format:** [skops](https://skops.readthedocs.io), not pickle. Loading checks every
type in the file against an explicit allow-list, so a tampered file cannot run code.

Version `{version}`.
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="model/model.skops")
    ap.add_argument("--push-to-hub", metavar="REPO_ID", help="e.g. yourname/transakt-sentinel-risk")
    ap.add_argument("--feedback", metavar="JSON", help="analyst decisions from the investigator's GET /feedback")
    args = ap.parse_args()

    bundle = train(feedback_path=args.feedback)
    if str(bundle["metrics"].get("gate", "")).startswith("REJECTED"):
        print(f"!! retrained model NOT shipped — {bundle['metrics']['gate']}. Keeping the current model.")
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    sio.dump(bundle, args.out)
    print(f"saved {args.out}  metrics={json.dumps(bundle['metrics'])}")

    if args.push_to_hub:
        from huggingface_hub import HfApi
        api = HfApi()   # reads HF_TOKEN from the environment
        api.create_repo(args.push_to_hub, exist_ok=True)
        card = MODEL_CARD.format(features=", ".join(FEATURE_ORDER),
                                 metrics=json.dumps(bundle["metrics"]), version=bundle["version"])
        api.upload_file(path_or_fileobj=args.out, path_in_repo="model.skops", repo_id=args.push_to_hub)
        api.upload_file(path_or_fileobj=card.encode(), path_in_repo="README.md", repo_id=args.push_to_hub)
        print(f"pushed to https://huggingface.co/{args.push_to_hub}")


if __name__ == "__main__":
    main()
