import os


class Config(object):
    wrds_username = os.environ.get("WRDS_USERNAME") or "iewaij"
    base_dir = os.path.abspath(os.path.dirname(__file__))
    fund_sql = "sql/fund.sql"
    cap_sql = "sql/cap.sql"
    prc_sql = "sql/prc.sql"
    prctg_sql = "sql/prctg.sql"
    surp_sql = "sql/surp.sql"
    fund_parquet = base_dir + "/../data/fund.parquet"
    cap_parquet = base_dir + "/../data/cap.parquet"
    prc_parquet = base_dir + "/../data/prc.parquet"
    prctg_parquet = base_dir + "/../data/prctg.parquet"
    surp_parquet = base_dir + "/../data/surp.parquet"
    data_parquet = base_dir + "/../data/data.parquet"
