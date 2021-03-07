import wrds
import pandas as pd
import numpy as np
from pandas.tseries.offsets import DateOffset
from build import build_frame
from config import Config

config = Config()


def query_wrds(sql_stmt):
    with wrds.Connection(wrds_username=config.wrds_username) as db:
        data = db.raw_sql(
            sql_stmt,
            date_cols=["date", "datadate"],
        )
    return data


def query_sql(sql_path):
    with open(sql_path) as sql:
        sql_stmt = sql.read()
    data = query_wrds(sql_stmt)
    return data


def query_price():
    prc_sql = config.prc_sql
    prc = query_sql(prc_sql)
    return prc


def query_fundamental():
    fund_sql = config.fund_sql
    fund = query_sql(fund_sql).astype(
        {"gvkey": "object", "country": "category", "industry": "category"}
    )
    return fund


def query_market_cap():
    cap_sql = config.cap_sql
    cap = query_sql(cap_sql)
    return cap


def query_price_target():
    prctg_sql = config.prctg_sql
    prctg = query_sql(prctg_sql)
    return prctg


def query_surprise():
    surp_sql = config.surp_sql
    surp = query_sql(surp_sql)
    return surp


def get_price():
    filename = config.prc_parquet
    try:
        prc = pd.read_parquet(filename)
    except FileNotFoundError:
        prc = query_price()
        prc.to_parquet(filename)
    return prc


def get_fundamental():
    filename = config.fund_parquet
    try:
        fund = pd.read_parquet(filename)
    except FileNotFoundError:
        fund = query_fundamental()
        fund.to_parquet(filename)
    return fund


def get_market_cap():
    filename = config.cap_parquet
    try:
        cap = pd.read_parquet(filename)
    except FileNotFoundError:
        cap = query_market_cap()
        cap.to_parquet(filename)
    return cap


def get_price_target():
    filename = config.prctgt_parquet
    try:
        prctgt = pd.read_parquet(filename)
    except FileNotFoundError:
        prctgt = query_price_target()
        prctgt.to_parquet(filename)
    return prctgt


def get_surprise():
    filename = config.surp_parquet
    try:
        surp = pd.read_parquet(filename)
    except FileNotFoundError:
        surp = query_surprise()
        surp.to_parquet(filename)
    return surp


def build_data(min_pctl):
    prc = get_price()
    fund = get_fundamental()
    cap = get_market_cap()
    data = build_frame(prc, fund, cap, min_pctl)
    return data


def get_data(min_pctl=0.5):
    filename = config.data_parquet
    try:
        data = pd.read_parquet(filename)
    except FileNotFoundError:
        data = build_data()
        data.to_parquet(filename)
    return data
