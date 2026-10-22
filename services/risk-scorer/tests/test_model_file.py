"""The model file is loaded through an allow-list, never unpickled."""

import pickle

import pytest
import skops.io as sio
from sklearn.ensemble import HistGradientBoostingClassifier

from app.model import UntrustedModelError, load_bundle, load_file
from app.train import train


@pytest.fixture(scope="module")
def saved(tmp_path_factory):
    path = tmp_path_factory.mktemp("m") / "model.skops"
    sio.dump(train(), str(path))
    return path


def test_round_trip_gives_identical_scores(saved):
    original = train()["model"]
    loaded = load_file(str(saved))
    row = [[9, 12, 40, 1.1, 5.9, 3000.0, 0.0]]
    assert loaded["model"].predict_proba(row)[0, 1] == original.predict_proba(row)[0, 1]


def test_types_outside_the_allow_list_are_refused(tmp_path):
    class Payload:                       # stands in for anything a tampered file might carry
        pass

    path = tmp_path / "evil.skops"
    sio.dump({"model": HistGradientBoostingClassifier(), "extra": Payload()}, str(path))
    with pytest.raises(UntrustedModelError, match="Payload"):
        load_file(str(path))


def test_a_pickle_is_not_accepted_at_all(tmp_path):
    path = tmp_path / "model.skops"
    path.write_bytes(pickle.dumps({"model": None}))
    with pytest.raises(Exception):
        load_file(str(path))


def test_load_bundle_uses_the_local_file(saved, monkeypatch):
    monkeypatch.setenv("MODEL_PATH", str(saved))
    monkeypatch.delenv("HF_MODEL_REPO", raising=False)
    b = load_bundle()
    assert b["source"] == "local" and b["version"]


def test_analyst_feedback_teaches_the_model(tmp_path):
    import json
    import math
    from app.train import train
    # A pattern the synthetic data calls fraud: a velocity burst from an established shop.
    # Suppose analysts keep dismissing it (say, a flash sale every evening).
    flash_sale = {"count_2m": 14, "count_10m": 20, "history_count": 120, "amount_ratio": 1.0,
                  "log_amount": 4.7, "merchant_age_min": 50_000.0, "failed_ratio_10m": 0.0}
    path = tmp_path / "feedback.json"
    path.write_text(json.dumps([{"features": flash_sale, "label": 0}] * 12))
    base, tuned = train(), train(feedback_path=str(path))
    row = [[flash_sale[k] for k in base["feature_order"]]]
    assert base["model"].predict_proba(row)[0, 1] > 0.5           # flagged before
    assert tuned["model"].predict_proba(row)[0, 1] < 0.5          # learned from the analysts
    assert tuned["metrics"]["feedback_rows"] == 12 and tuned["version"].endswith("+fb12")
    assert tuned["metrics"]["agrees_with_analysts_after"] == "12/12"
    # ...without forgetting real fraud: a new merchant moving Rs 30,000 a time
    blitz = [[3, 3, 2, math.nan, 6.48, 4.0, 0.0]]
    assert tuned["model"].predict_proba(blitz)[0, 1] > 0.5


def test_missing_feedback_file_means_none(tmp_path):
    from app.train import train
    assert train(feedback_path=str(tmp_path / "nope.json"))["metrics"]["feedback_rows"] == 0
