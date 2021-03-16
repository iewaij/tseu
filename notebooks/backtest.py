import numpy as np
import pandas as pd
from pandas.tseries.offsets import DateOffset


def frame_to_signals(X, estimator):
    try:
        short_array = -1 * estimator.predict_proba(X)[:, 0]
        long_array = estimator.predict_proba(X)[:, -1]
    except AttributeError:
        short_array = estimator.predict(X)
        long_array = estimator.predict(X)
    short_signal = pd.Series(short_array, X.index)
    short_signal = short_signal.where(short_signal < 0, 0)
    long_signal = pd.Series(long_array, X.index)
    long_signal = long_signal.where(long_signal > 0, 0)
    return short_signal, long_signal


def noise_to_belief(signal, n):
    return signal.groupby("date", group_keys=False).apply(
        lambda x: x.sort_values(ascending=False, key=abs).head(n)
    )


def monthly_to_daily(signal, months, weight):
    daily_signals = []
    for index, value in signal.iteritems():
        gvkey = index[0]
        predict_date = pd.to_datetime(index[1])
        start = predict_date + DateOffset(days=1)
        end = predict_date + DateOffset(months=months)
        date_range = pd.date_range(start, end, freq="D", name="date")
        daily_idx = pd.MultiIndex.from_tuples(
            [(gvkey, date) for date in date_range], names=["gvkey", "date"]
        )
        if weight == "equal":
            daily_signal = pd.Series(np.sign(value), daily_idx)
        elif weight == "value":
            daily_signal = pd.Series(value, daily_idx)
        daily_signals.append(daily_signal)
    return pd.concat(daily_signals)


def signals_to_positions(short_signal, long_signal, months=3, n=10, weight="equal"):
    short_belief = noise_to_belief(short_signal, n)
    long_belief = noise_to_belief(long_signal, n)
    short_daily = monthly_to_daily(short_belief, months, weight)
    long_daily = monthly_to_daily(long_belief, months, weight)
    short_position = short_daily.groupby(["gvkey", "date"]).agg("sum")
    long_position = long_daily.groupby(["gvkey", "date"]).agg("sum")
    neutral_position = (
        pd.concat([short_position, long_position]).groupby(["gvkey", "date"]).agg("sum")
    )
    return short_position, long_position, neutral_position


def position_to_margin(data, position, tx=-0.001, method="log"):
    position = position.rename("position").to_frame()
    merged = position.join(data)
    merged["close"] = merged.close.groupby("gvkey").fillna(method="ffill")
    merged["tx"] = np.where(position.shift(1) != position, tx, 0)

    if method == "log":
        merged["margin"] = (
            merged.close.groupby("gvkey")
            .transform(lambda x: np.log(x / x.shift(1)))
            .fillna(0)
        )
    elif method == "percent":
        merged["margin"] = merged.close.groupby("gvkey").transform(
            lambda x: x.pct_change(1)
        )

    margin = merged.position * merged.margin + merged.tx
    margin = margin.droplevel("gvkey").resample("D").mean().fillna(0)
    return margin


def idx_margin(data, margin, idx="DAX", method="log"):
    idx_close = data.loc[idx].reindex(margin.index, method="ffill").close
    if method == "log":
        idx_margin = np.log(idx_close / idx_close.shift(1))
    elif method == "percent":
        idx_margin = idx_close.pct_change(1)
    return idx_margin.fillna(0)


def sharpe_ratio(margin):
    margin = margin[margin != 0]
    return margin.mean() / margin.std() * np.sqrt(252)


def backtest_report(
    data,
    features,
    estimator,
    test_start="2012-01-01",
    test_end="2016-01-01",
    months=3,
    n=10,
    weight="equal",
    method="log",
):
    X = features.xs(slice(test_start, test_end), level="date", drop_level=False)
    short_signal, long_signal = frame_to_signals(X, estimator)
    short_position, long_position, neutral_position = signals_to_positions(
        short_signal, long_signal, n, months, weight
    )
    short_margin = position_to_margin(data, short_position, method=method)
    neutral_margin = position_to_margin(data, neutral_position, method=method)
    long_margin = position_to_margin(data, long_position, method=method)
    dax_margin = idx_margin(data, neutral_margin, idx="DAX", method=method)
    cac40_margin = idx_margin(data, neutral_margin, idx="CAC40", method=method)
    stoxx50_margin = idx_margin(data, neutral_margin, idx="STOXX50", method=method)
    cum_df = pd.DataFrame(
        {
            "Short Only": short_margin.cumsum(),
            "Short/Long": neutral_margin.cumsum(),
            "Long Only": long_margin.cumsum(),
            "DAX": dax_margin.cumsum(),
            "CAC40": cac40_margin.cumsum(),
            "STOXX50": stoxx50_margin.cumsum(),
        }
    )
    cum_df[["Short Only", "Short/Long", "Long Only"]].plot.line(
        color={
            "Short Only": "r",
            "Short/Long": "k",
            "Long Only": "g",
        }
    )
    cum_df[["Long Only", "DAX", "CAC40", "STOXX50"]].plot.line(
        color={
            "Long Only": "k",
            "DAX": "y",
            "CAC40": "b",
            "STOXX50": "m",
        }
    )
    print(f"Short Only Sharpe Ratio: {sharpe_ratio(short_margin)}")
    print(f"Short Long Sharpe Ratio: {sharpe_ratio(neutral_margin)}")
    print(f"Long Only Sharpe Ratio: {sharpe_ratio(long_margin)}")
