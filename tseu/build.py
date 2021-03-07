import numpy as np
import pandas as pd
from pandas.tseries.offsets import DateOffset
from functools import reduce


def build_gvkeys(prc, fund, cap, min_pctl=0.5):
    gvkeys_prc = prc.gvkey.unique()
    gvkeys_fund = fund["gvkey"].unique()
    gvkeys_cap = cap.loc[
        (cap.mcap_pctl > min_pctl) & (cap.date > "2000-01-01"), :
    ].gvkey.unique()
    gvkeys = reduce(np.intersect1d, (gvkeys_prc, gvkeys_fund, gvkeys_cap))
    return gvkeys


def fill_days(df):
    first_date = df["date"].iloc[0]
    last_date = df["date"].iloc[-1]
    date_index = pd.date_range(
        pd.to_datetime(first_date), pd.to_datetime(last_date) + DateOffset(years=1)
    )
    return (
        df.drop("gvkey", axis=1)
        .set_index("date")
        .sort_index()
        .reindex(date_index, method="ffill")
    )


def build_fundamental(fund):
    fund_filled = fund.groupby("gvkey").apply(fill_days)
    return fund_filled


def build_technical():
    pass


def build_frame(prc, fund, cap, min_pctl=0.5):
    gvkeys = build_gvkeys(prc, fund, cap, min_pctl)
    # update dates and ffill missing values
    # https://stackoverflow.com/questions/47454219/apply-set-index-over-groupby-object-in-order-to-apply-asfreq-per-group
    cap = (
        cap[(cap.mcap_pctl > min_pctl) & (cap.date >= "2000-01-01")]
        .groupby("gvkey")
        .apply(lambda x: x.set_index("date").resample("D").ffill())
        .drop(["gvkey"], axis=1)
    )
    fund = (
        fund[fund.gvkey.isin(gvkeys)]
        .groupby("gvkey")
        .apply(lambda x: x.set_index("date").resample("D").ffill())
        .drop(["gvkey"], axis=1)
    )
    prc = prc.set_index(["gvkey", "date"])
    df = pd.concat([cap, fund, prc], join="inner", axis=1)
    return df
