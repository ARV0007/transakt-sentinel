"""
Loads the risk model: from the Hugging Face Hub when HF_MODEL_REPO is set,
otherwise from the copy trained into the image at build time.

Falling back instead of failing is deliberate. The Hub being unreachable is a
reason to log a warning, not a reason for fraud scoring to stop.

The file is skops, not pickle. Unpickling a file runs whatever code it
contains, so a pickle from the Hub is only as safe as everyone who can push to
that repository. skops stores data, and on load it compares every type in the
file with TRUSTED below; anything else is refused before it is built.
"""

import logging
import os

import skops.io as sio

from .features import FEATURE_ORDER, vector

log = logging.getLogger("risk-scorer.model")

# The only non-builtin type a HistGradientBoostingClassifier needs beyond
# skops's own defaults. Adding a type here is a security decision, not a fix
# for a load error: read what it is first.
TRUSTED = ["sklearn.ensemble._hist_gradient_boosting.predictor.TreePredictor"]


class UntrustedModelError(RuntimeError):
    pass


def load_file(path):
    unknown = [t for t in sio.get_untrusted_types(file=path) if t not in TRUSTED]
    if unknown:
        raise UntrustedModelError(f"{path} contains types outside the allow-list: {unknown}")
    return sio.load(path, trusted=TRUSTED)


def load_bundle():
    local = os.environ.get("MODEL_PATH", "model/model.skops")
    repo = os.environ.get("HF_MODEL_REPO")
    bundle, source = None, "local"
    if repo:
        try:
            from huggingface_hub import hf_hub_download
            path = hf_hub_download(repo_id=repo, filename="model.skops",
                                   revision=os.environ.get("HF_MODEL_REVISION", "main"))
            bundle, source = load_file(path), f"hf://{repo}"
        except UntrustedModelError:
            raise          # a tampered model is an alarm, not a reason to quietly fall back
        except Exception as e:  # network, auth, missing file — all mean "use local"
            log.warning("Could not fetch %s from the Hub (%s); using %s", repo, e, local)
    if bundle is None:
        bundle = load_file(local)
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
        self.gate = (bundle.get("metrics") or {}).get("gate", "no feedback")

    def score(self, feats):
        return self.score_many([feats])[0]

    def score_many(self, feats_list):
        """One model call for the whole batch: the call's fixed overhead (~0.7 ms)
        dwarfs the per-row cost, so this is what makes batching pay."""
        return [float(p) for p in self.model.predict_proba([vector(f) for f in feats_list])[:, 1]]
