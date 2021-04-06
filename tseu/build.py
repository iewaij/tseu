import numpy as np
import pandas as pd


def build_multi_label(y, thres=0.1):
    if y > thres:
        return 2
    elif y < -thres:
        return 0
    else:
        return 1


def build_train_test(
    data,
    train_start,
    train_end,
    test_start,
    test_end,
    months=3,
    relative=True,
    extreme=True,
    lower=0.2,
    upper=0.8,
    label="multi",
    multi_thres=0.1,
    rescale="rank",
):
    labels = (
        data.close.groupby("gvkey")
        .transform(lambda x: np.log(x.shift(-months)) - np.log(x))
        .dropna()
    )

    if relative:
        labels = labels.groupby("date").transform(lambda y: (y - y.median()))

    features = data.drop(columns="close")

    if rescale == "rank":
        features = features.astype("float").groupby("date").rank(pct=True).fillna(0.5)

    y_train = labels.xs(slice(train_start, train_end), level="date", drop_level=False)
    y_test = labels.xs(slice(test_start, test_end), level="date", drop_level=False)

    if extreme:
        y_train = y_train.groupby("date", group_keys=False).apply(
            lambda y: y[(y < y.quantile(lower)) | (y > y.quantile(upper))]
        )

    if label == "binary":
        y_train = (y_train >= 0).astype("int")
        y_test = (y_test >= 0).astype("int")
    elif label == "multi":
        y_train = y_train.transform(build_multi_label, 0, multi_thres)
        y_test = y_test.transform(build_multi_label, 0, multi_thres)
    elif label == "regression":
        pass
    else:
        raise NameError("The labelling method doesn't exist.")

    X_train = features.reindex(y_train.index)
    X_test = features.reindex(y_test.index)
    return X_train, y_train, X_test, y_test
