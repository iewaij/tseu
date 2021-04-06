import numpy as np
import pandas as pd
import wrds
from .config import Config
from .compute import compute_fundamental, compute_price, compute_data

config = Config()


def query_wrds(sql_stmt):
    with wrds.Connection(wrds_username=config.wrds_username) as db:
        data = db.raw_sql(sql_stmt, date_cols=["date"], index_col=["gvkey", "date"])
    return data


def query_sql(sql_path):
    with open(sql_path) as sql:
        sql_stmt = sql.read()
    data = query_wrds(sql_stmt)
    return data


def wrangle_analyst(ana):
    ana["buypct"] = ana.buypct / 100
    ana["holdpct"] = ana.holdpct / 100
    ana["sellpct"] = ana.sellpct / 100
    ana = ana.astype(
        {
            "numest": "Int64",
            "ptgdown": "Int64",
            "ptgup": "Int64",
            "numrec": "Int64",
            "recdown": "Int64",
            "recup": "Int64",
        }
    )
    return ana


def get_analyst():
    filename = config.ana_parquet
    try:
        ana = pd.read_parquet(filename)
    except FileNotFoundError:
        ana = query_sql(config.ana_sql)
        ana = wrangle_analyst(ana)
        ana.to_parquet(filename)
    return ana


def resample_analyst(ana):
    ana = ana.groupby(
        [pd.Grouper(level="gvkey"), pd.Grouper(level="date", freq="M")]
    ).agg(
        {
            "numest": "sum",
            "ptgdown": "sum",
            "ptgup": "sum",
            "meanptg": "mean",
            "ptghigh": "max",
            "ptglow": "min",
            "numrec": "sum",
            "recdown": "sum",
            "recup": "sum",
            "meanrec": "mean",
            "buypct": "last",
            "holdpct": "last",
            "sellpct": "last",
        }
    )
    return ana


def wrangle_fundamental(fund):
    fund["seq"] = fund.seq.fillna(fund.ceq + fund.pstk)
    fund["teq"] = fund.teq.fillna(fund.lse - fund.ltt)
    fund = (
        fund.fillna(0)
        .astype({"sic": "int64", "sic_2": "int64"})
        .astype({"loc": "category", "sic": "category", "sic_2": "category"})
    )
    return fund


def get_fundamental():
    filename = config.fund_parquet
    try:
        fund = pd.read_parquet(filename)
    except FileNotFoundError:
        fund = query_sql(config.fund_sql)
        fund = wrangle_fundamental(fund)
        fund.to_parquet(filename)
    return fund


def reindex_fundamental(fund):
    def reindex(gf):
        from pandas.tseries.offsets import DateOffset

        gvkey = gf.index[0][0]
        start = gf.index[0][1]
        end = gf.index[-1][1] + DateOffset(years=1)
        today = pd.to_datetime("today")
        if today < end:
            end = today
        date_range = pd.date_range(start, end, freq="M", name="date")
        monthly_idx = pd.MultiIndex.from_tuples(
            [(gvkey, date) for date in date_range], names=["gvkey", "date"]
        )
        return gf.reindex(monthly_idx, method="ffill")

    return fund.groupby("gvkey", group_keys=False).apply(reindex)


def wrangle_price(prc):
    prc = prc.astype({"cshoc": "int64", "cshtrd": "int64"})
    return prc


def get_price():
    filename = config.prc_parquet
    try:
        prc = pd.read_parquet(filename)
    except FileNotFoundError:
        prc = query_sql(config.prc_sql)
        prc = wrangle_price(prc)
        prc.to_parquet(filename)
    return prc


def resample_price(prc):
    prc = prc.groupby(
        [pd.Grouper(level="gvkey"), pd.Grouper(level="date", freq="M")]
    ).agg(
        {
            "ajexdi": "last",
            "prccd": "last",
            "prccd": "last",
            "cshoc": "last",
            "cshtrd": "sum",
        }
    )
    return prc


def get_gvkeys(fund, prc):
    gvkeys_fund = fund.index.get_level_values("gvkey").unique()
    gvkeys_prc = prc[prc.prccd > 5].index.get_level_values("gvkey").unique()
    gvkeys = np.intersect1d(gvkeys_prc, gvkeys_fund)
    return gvkeys


def filter_gvkeys(df, gvkeys):
    idx = df.index.get_level_values("gvkey").isin(gvkeys)
    return df.loc[idx, :]


def get_data():
    filename = config.data_parquet
    try:
        data = pd.read_parquet(filename)
    except FileNotFoundError:
        # extract data
        ana = get_analyst()
        fund = get_fundamental()
        prc = get_price()
        # filter gvkeys, compute features, and reindex or resample to monthly frequency
        gvkeys = get_gvkeys(fund, prc)
        ana = ana.pipe(filter_gvkeys, gvkeys).pipe(resample_analyst)
        fund = (
            fund.pipe(filter_gvkeys, gvkeys)
            .pipe(compute_fundamental)
            .pipe(reindex_fundamental)
        )
        prc = prc.pipe(filter_gvkeys, gvkeys).pipe(resample_price).pipe(compute_price)
        # merge and compute features
        data = prc.join([ana, fund]).pipe(compute_data)
        # select rows with closing price > 5 and computed columns
        col_prefixes = (
            "analyst",
            "accruals",
            "efficiency",
            "profitability",
            "intangible",
            "investment",
            "leverage",
            "liquidity",
            "market",
            "other",
            "mom",
            "ema",
            "qt",
            "scosc",
            "rsi",
            "std",
            "sh",
            "close",
        )
        data = data.loc[data.prccd > 5, data.columns.str.startswith(col_prefixes)]
        data.to_parquet(filename)
    return data


def get_X():
    data = get_data()
    return data.drop(columns="close").sort_index(axis=1)


def get_y():
    data = get_data()
    return data.close
