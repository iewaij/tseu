import os


class Config(object):
    wrds_username = os.environ.get("WRDS_USERNAME") or "iewaij"
    base_dir = os.path.abspath(os.path.dirname(__file__))
    fund_sql = base_dir + "/sql/fund.sql"
    prc_sql = base_dir + "/sql/prc.sql"
    ana_sql = base_dir + "/sql/ana.sql"
    gvkeys_pickle = "./data/gvkeys.pkl"
    fund_parquet = "./data/fund.parquet"
    prc_parquet = "./data/prc.parquet"
    ana_parquet = "./data/ana.parquet"
    data_parquet = "./data/data.parquet"
    backtest_parquet = "./data/backtest.parquet"
