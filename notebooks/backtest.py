import numpy as np
import pandas as pd
from pandas.tseries.offsets import DateOffset
from sklearn.linear_model import LinearRegression
import statsmodels.api as sm


def frame_to_signals(X, estimator):
    try:
        short_array = -1 * estimator.predict_proba(X)[:, 0]
        long_array = estimator.predict_proba(X)[:, -1]
    except AttributeError:
        short_array = estimator.predict(X)
        long_array = estimator.predict(X)
    short_signal = pd.Series(short_array, X.index)
    short_signal = short_signal[short_signal < 0]
    long_signal = pd.Series(long_array, X.index)
    long_signal = long_signal[long_signal > 0]
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
    return short_position, long_position


def position_to_margin(data, position, tx=-0.001, method="percent"):
    position, close = position.align(data.close, join="inner")
    tx = np.where(position.shift(1) != position, tx, 0)

    if method == "log":
        margin = close.groupby("gvkey").transform(lambda x: np.log(x / x.shift(1)))
    elif method == "percent":
        margin = close.groupby("gvkey").transform(lambda x: x.pct_change(1))

    margin = position * margin + tx
    margin = margin.droplevel("gvkey").resample("D").mean()
    return margin


def idx_margin(data, margin, idx="DAX", method="percent"):
    idx_close = data.close.loc[idx].reindex(margin.index, method="ffill")
    if method == "log":
        idx_margin = np.log(idx_close / idx_close.shift(1))
    elif method == "percent":
        idx_margin = idx_close.pct_change(1)
    return idx_margin


def sharpe_ratio(margin):
    margin = margin.dropna()
    return margin.mean() / margin.std() * np.sqrt(252)


def max_drawdown(data, margin):
    drawdown = margin.dropna() - margin.dropna().cummax()
    drawdown_max = drawdown.min()
    return drawdown_max


def capm_report(margin, market_margin):
    r_p, r_m = margin.dropna().align(market_margin.dropna(), join="inner")
    X = sm.add_constant(r_p)
    y = r_m
    model = sm.OLS(y, X)
    results = model.fit()
    print(results.summary())


def capm(margin, market_margin):
    beta = margin.cov(market_margin) / market_margin.var()
    alpha = (margin.mean() - beta * market_margin.mean()) * 252
    return beta, alpha


def test_backtest_report(
    backtest_data,
    features,
    estimator,
    test_start="2012-01-01",
    test_end="2016-01-01",
    months=3,
    n=10,
    weight="equal",
    method="percent",
):
    X = features.xs(slice(test_start, test_end), level="date", drop_level=False)
    data = backtest_data.xs(slice(test_start, test_end), level="date", drop_level=False)
    short_signal, long_signal = frame_to_signals(X, estimator)
    short_position, long_position = signals_to_positions(
        short_signal, long_signal, n, months, weight
    )
    return short_position, long_position
    short_margin = position_to_margin(data, short_position, method=method)
    long_margin = position_to_margin(data, long_position, method=method)
    neutral_margin = short_margin + long_margin
    market_margin = idx_margin(data, long_margin, idx="STOXX600", method=method)
    return short_margin, long_margin, neutral_margin, market_margin


def backtest_report(
    backtest_data,
    features,
    estimator,
    test_start="2012-01-01",
    test_end="2016-01-01",
    months=3,
    n=10,
    weight="equal",
    method="percent",
):
    X = features.xs(slice(test_start, test_end), level="date", drop_level=False)
    data = backtest_data.xs(slice(test_start, test_end), level="date", drop_level=False)
    short_signal, long_signal = frame_to_signals(X, estimator)
    short_position, long_position = signals_to_positions(
        short_signal, long_signal, n, months, weight
    )
    short_margin = position_to_margin(data, short_position, method=method)
    long_margin = position_to_margin(data, long_position, method=method)
    neutral_margin = short_margin + long_margin
    market_margin = idx_margin(data, long_margin, idx="STOXX600", method=method)

    cum_df = pd.DataFrame(
        {
            "Short Only": short_margin.fillna(0).cumsum(),
            "Short Long": neutral_margin.fillna(0).cumsum(),
            "Long Only": long_margin.fillna(0).cumsum(),
            "STOXX600": market_margin.fillna(0).cumsum(),
        }
    )
    cum_df[["Short Only", "Short Long", "Long Only"]].plot.line(
        figsize=(12, 6),
        color={
            "Short Only": "r",
            "Short Long": "k",
            "Long Only": "g",
        },
        xlabel="Date",
        ylabel="Profit/Loss",
    )
    cum_df[["Long Only", "STOXX600"]].plot.line(
        figsize=(12, 6),
        color={
            "Long Only": "g",
            "STOXX600": "m",
        },
        xlabel="Date",
        ylabel="Profit/Loss",
    )

    s_beta, s_alpha = capm(short_margin, market_margin)
    n_beta, n_alpha = capm(neutral_margin, market_margin)
    l_beta, l_alpha = capm(long_margin, market_margin)

    print("Short Only:")
    print(f"Max Drawdown: {max_drawdown(data, short_margin)}")
    print(f"Sharpe : {sharpe_ratio(short_margin)}")
    print(f"Total Return: {short_margin.cumsum()[-1]}")
    print(f"Alpha: {s_alpha}")
    print(f"Beta: {s_beta}")
    capm_report(short_margin, market_margin)
    print("----------------------------------------")
    print("Long Only:")
    print(f"Max Drawdown: {max_drawdown(data, long_margin)}")
    print(f"Sharpe : {sharpe_ratio(long_margin)}")
    print(f"Total Return: {long_margin.cumsum()[-1]}")
    print(f"Alpha: {l_alpha}")
    print(f"Beta: {l_beta}")
    capm_report(long_margin, market_margin)
    print("----------------------------------------")
    print("Market Neutral:")
    print(f"Max Drawdown: {max_drawdown(data, neutral_margin)}")
    print(f"Sharpe : {sharpe_ratio(neutral_margin)}")
    print(f"Total Return: {neutral_margin.cumsum()[-1]}")
    print(f"Alpha: {n_alpha}")
    print(f"Beta: {n_beta}")
    capm_report(neutral_margin, market_margin)
    print("----------------------------------------")
