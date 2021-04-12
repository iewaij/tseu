import os

import numpy as np
import pandas as pd
import wrds

from .compute import compute_data, compute_fundamental, compute_price


def cache(func):
    def wrapped_func(*args, **kwargs):
        from datetime import date

        table_name = func.__name__.split("_")[-1]
        current_date = date.today().strftime("%Y%m%d")
        parquet_path = f"./data/{table_name}-{current_date}.parquet"
        try:
            table = pd.read_parquet(parquet_path)
        except FileNotFoundError:
            table = func(*args, **kwargs)
            table.to_parquet(parquet_path)
        return table

    return wrapped_func


def query(sql_stmt, wrds_username):
    with wrds.Connection(wrds_username=wrds_username) as db:
        data = db.raw_sql(sql_stmt, date_cols=["date"], index_col=["gvkey", "date"])
    return data


def wrangle_analyst(ana):
    ana["buypct"] = ana.buypct / 100
    ana["holdpct"] = ana.holdpct / 100
    ana["sellpct"] = ana.sellpct / 100
    return ana


@cache
def get_analyst(wrds_username):
    ana_sql = """
        SELECT
            gvkey,
            ptg.statpers AS date,
            numest,
            numdown1m AS ptgdown,
            numup1m AS ptgup,
            meanptg,
            ptghigh,
            ptglow,
            numrec,
            numdown AS recdown,
            numup AS recup,
            meanrec,
            buypct,
            holdpct,
            sellpct
        FROM
            ibes.ptgsumu AS ptg
            FULL JOIN ibes.recdsum AS rec ON ptg.statpers = rec.statpers AND ptg.ticker = rec.ticker
            JOIN comp_global_daily.g_security AS sec ON ptg.ticker = sec.ibtic
        WHERE
            exchg = ANY (ARRAY [104, 132, 151, 154, 171, 172, 192, 194, 201, 209, 228, 256, 257, 273, 286])
            AND curr = 'EUR';
        """
    ana = query(ana_sql, wrds_username)
    ana = wrangle_analyst(ana)
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
            "buypct": "mean",
            "holdpct": "mean",
            "sellpct": "mean",
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


@cache
def get_fundamental(wrds_username):
    fund_sql = """
        SELECT
            gvkey,
            CASE WHEN pdate < datadate + '6 months'::INTERVAL THEN
                pdate + '3 days'::INTERVAL
            ELSE
                datadate + '3 months'::INTERVAL END::DATE AS date,
            loc,
            sich AS sic,
            LEFT(to_char(sich, '9999'), 3) AS sic_2,
            -- Assets
            rect,act,che,ch,ivst,ppegt,invt,aco,intan,ao,ppent,gdwl,icapt,ivaeq,ivao,mib,mibn,mibt,at AS att,
            -- Liabilities
            lse,lct,dlc,dltt,dltr,dltis,dlcch,ap,lco,lo,txdi,lt as ltt,
            -- Equities and Others
            teq,seq,ceq,pstk,emp,
            -- Income Statement
            sale,revt,cogs,xsga,dp,xrd,ib,ebitda,ebit,nopi,spi,pi,txp,nicon,txt,xint,dvc,dvt,sstk,
            -- Cash Flow Statement and Others
            capx,oancf,fincf,ivncf,prstkc,dv
        FROM
            comp.g_funda
        WHERE
            exchg = ANY (ARRAY [104, 107, 132, 151, 154, 171, 192, 194, 201, 209, 256, 257, 273, 276, 286])
            AND curcd = 'EUR'
            AND datafmt = 'HIST_STD'
            AND consol = 'C'
            AND datadate >= '1999-01-01'
            AND at IS NOT NULL
            AND nicon IS NOT NULL
        ORDER BY
            gvkey,
            datadate;
        """
    fund = query(fund_sql, wrds_username)
    fund = wrangle_fundamental(fund)
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


@cache
def get_price(wrds_username):
    prc_sql = """
        SELECT
            prc.gvkey,
            prc.datadate AS date,
            cshoc,
            ajexdi,
            prcod,
            prchd,
            prcld,
            prccd,
            cshtrd
        FROM ( SELECT DISTINCT
                gvkey,
                iid
            FROM
                comp_global_daily.g_funda
            WHERE
                exchg = ANY (ARRAY [104, 132, 151, 154, 171, 172, 192, 194, 201, 209, 228, 256, 257, 273, 286])
                AND curcd = 'EUR') AS fund
            JOIN comp_global_daily.g_sec_dprc AS prc ON fund.gvkey = prc.gvkey AND fund.iid = prc.iid
        WHERE
            curcdd = 'EUR'
            AND cshoc >= 0
            AND cshtrd >= 0
            AND datadate >= '1999-01-01';
        """
    prc = query(prc_sql, wrds_username)
    prc = wrangle_price(prc)
    return prc


def resample_price(prc):
    prc = prc.groupby(
        [pd.Grouper(level="gvkey"), pd.Grouper(level="date", freq="M")]
    ).agg(
        {
            "cshoc": "last",
            "ajexdi": "last",
            "prcod": "first",
            "prchd": "max",
            "prcld": "min",
            "prccd": "last",
            "cshtrd": "sum",
        }
    )
    return prc


def filter_gvkeys(df, gvkeys):
    idx = df.index.get_level_values("gvkey").isin(gvkeys)
    return df.loc[idx, :]


@cache
def get_data(wrds_username=None):
    # Extract data
    ana = get_analyst(wrds_username)
    fund = get_fundamental(wrds_username)
    prc = get_price(wrds_username)
    # Filter gvkeys to firms with closing price > 5
    gvkeys = prc[prc.prccd > 5].index.get_level_values("gvkey").unique()
    # Compute features and reindex or resample to monthly frequency
    ana = ana.pipe(filter_gvkeys, gvkeys).pipe(resample_analyst)
    fund = (
        fund.pipe(filter_gvkeys, gvkeys)
        .pipe(compute_fundamental)
        .pipe(reindex_fundamental)
    )
    prc = prc.pipe(filter_gvkeys, gvkeys).pipe(resample_price).pipe(compute_price)
    # Merge and compute features
    data = prc.join([ana, fund]).pipe(compute_data)
    # Select rows with closing price > 5, market cap > 1 million and computed columns
    data = data.loc[
        (data.prccd > 5) & (data.market_cap > 1e6),
        data.columns.str.startswith(
            (
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
                "open",
                "high",
                "low",
                "close",
                "volume",
            )
        ),
    ]
    return data


def get_X():
    data = get_data()
    return data.drop(
        columns=[
            "open",
            "high",
            "low",
            "close",
        ]
    ).sort_index(axis=1)


def get_y():
    data = get_data()
    return data[
        [
            "open",
            "high",
            "low",
            "close",
        ]
    ]
