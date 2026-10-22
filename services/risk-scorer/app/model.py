"""
Loads the risk model: from the Hugging Face Hub when HF_MODEL_REPO is set,
otherwise from the copy trained into the image at build time.

Falling back instead of failing is deliberate. The Hub being unreachable is a
reason to log a warning, not a reason for fraud scoring to stop.
"""

import logging
import os

import joblib

from .features import FEATURE_ORDER, vector

log = logging.getLogger("risk-scorer.model")


def load_bundle():
    local = os.environ.get("MODEL_PATH", "model/model.joblib")
    repo = os.environ.get("HF_MODEL_REPO")
    path, source = local, "local"
    if repo:
        try:
            from huggingface_hub import hf_hub_download
            path = hf_hub_download(repo_id=repo, filename="model.joblib",
                                   revision=os.environ.get("HF_MODEL_REVISION", "main"))
            source = f"hf://{repo}"
        except Exception as e:  # network, auth, missing file — all mean "use local"
            log.warning("Could not fetch %s from the Hub (%s); using %s", repo, e, local)
    # Security: joblib files are pickles and unpickling runs code. Only point
    # HF_MODEL_REPO at a repository you control.
    bundle = joblib.load(path)
    if bundle.get("feature_order") != FEATURE_ORDER:
        raise RuntimeError(f"model from {source} expects features {bundle.get('feature_order')}, "
                           f"code provides {FEATURE_ORDER}")
    bundle["source"] = source
    log.info("Loaded risk model %s from %s", bundle.get("version"), source)
    return bundle


class RiskModel:
    def __init__(self, bundle):
        self.model = bundle["model"]
        self.threshold = float(os.environ.get("RISK_THRESHOLD", bundle.get("threshold", 0.5)))
        self.version = bundle.get("version")
        self.source = bundle.get("source")

    def score(self, feats):
        return float(self.model.predict_proba([vector(feats)])[0, 1])
