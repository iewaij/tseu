import numpy as np
import pandas as pd


def scale_features(features, method="rank"):
    if method == "rank":
        features = features.groupby("date").rank(pct=True).fillna(0.5)
    return features


def build_features(universe, strategy, scale=None, **kwargs):
    features = strategy(universe, **kwargs)
    if scale is not None:
        features = scale_features(features, method=scale)
    return features


def build_labels(universe, months=3, relative=True):
    log_ret = (
        universe.close.groupby("gvkey")
        .transform(lambda x: np.log(x.shift(-months)) - np.log(x))
        .dropna()
    )

    if relative:
        y = log_ret.groupby("date").transform(lambda x: (x - x.median()))
    else:
        y = log_ret

    return y


def to_multi_lables(y, thres):
    multi = pd.Series(0, index=y.index)
    multi.where(y > -thres, -1, inplace=True)
    multi.where(y < thres, 1, inplace=True)
    return multi


def build_train_test(
    features,
    labels,
    train_start="2002-01-01",
    train_end="2012-01-01",
    test_start="2012-01-01",
    test_end="2016-01-01",
    method="multi",
    thres=0.1,
    extreme=False,
    lower=0.2,
    upper=0.8,
):
    """
    extreme: retain the 20% values that are the smallest and the 20% that are the largest.
    """
    idx = labels.index.intersection(features.index)
    y = labels.reindex(idx)
    y_test = y.xs(slice(test_start, test_end), level="date", drop_level=False)
    y_train = y.xs(slice(train_start, train_end), level="date", drop_level=False)

    if extreme:
        y_train = y_train.groupby("date", group_keys=False).apply(
            lambda x: x[(x < x.quantile(lower)) | (x > x.quantile(upper))]
        )

    X_test = features.reindex(y_test.index)
    X_train = features.reindex(y_train.index)

    if method == "binary":
        y_test = (y_test >= 0).astype("int")
        y_train = (y_train >= 0).astype("int")
    elif method == "multi":
        y_test = to_multi_lables(y_test, thres)
        y_train = to_multi_lables(y_train, thres)
    elif method == "regression":
        pass
    else:
        raise NameError("The method doesn't exist.")
    return X_train, y_train, X_test, y_test
