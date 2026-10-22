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
