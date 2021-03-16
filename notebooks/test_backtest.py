import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier
from backtest import (
    frame_to_signals,
    noise_to_belief,
    signals_to_positions,
    position_to_margin,
)


def prepare_test_data():
    raw = pd.read_parquet("../data/train-beta.1.parquet")
    gvkeys = (
        raw[(raw.mcap > 1e6) & (raw.prccd > 5)].index.get_level_values("gvkey").unique()
    )
    universe = raw.loc[gvkeys, ["open", "high", "low", "close"]]
    test_start = "2010-01-01"
    test_end = "2016-01-01"
    return universe.xs(slice(test_start, test_end), level="date", drop_level=False)


def prepare_test_multi():
    test_data = prepare_test_data()
    features = test_data[["open", "high", "low", "close"]]
    labels = pd.Series(
        np.random.randint(low=-1, high=2, size=len(features)), index=features.index
    )
    return features, labels


def test_frame_to_signals():
    X, y = prepare_test_multi()
    # long_clf only predict 1
    long_clf = DummyClassifier(strategy="constant", constant=1)
    long_clf.fit(X, y)
    short_signal, long_signal = frame_to_signals(X, long_clf)
    assert short_signal.unique() == np.array([0.0])
    assert long_signal.unique() == np.array([1.0])
    # short_clf only predict -1
    short_clf = DummyClassifier(strategy="constant", constant=-1)
    short_clf.fit(X, y)
    short_signal, long_signal = frame_to_signals(X, short_clf)
    assert short_signal.unique() == np.array([-1.0])
    assert long_signal.unique() == np.array([0.0])


def test_noise_to_belief():
    X, y = prepare_test_multi()
    # dummy_clf generates predictions at random.
    dummy_clf = DummyClassifier(strategy="uniform")
    dummy_clf.fit(X, y)
    short_signal, long_signal = frame_to_signals(X, dummy_clf)
    # signals will be added some random float
    short_signal -= np.random.random(size=len(short_signal))
    long_signal += np.random.random(size=len(long_signal))
    # short_belief and long_belief should be sorted by its abs value
    short_belief = noise_to_belief(short_signal, 10)
    long_belief = noise_to_belief(long_signal, 10)
    short_belief_1m = short_belief.xs("2010-01-31", level="date")
    long_belief_1m = long_belief.xs("2010-01-31", level="date")
    assert short_belief_1m[0] < short_belief_1m[-1]
    assert long_belief_1m[0] > long_belief_1m[-1]


test_frame_to_signals()
test_noise_to_belief()
