import wrds
import pandas as pd
from config import Config

config = Config()


def query_wrds(sql_stmt):
    with wrds.Connection(wrds_username=config.wrds_username) as db:
        data = db.raw_sql(
            sql_stmt,
            date_cols=["date", "datadate"],
        )
    return data


def get_price_data():
    filename = config.basedir + "/../data/prc.parquet"
    try:
        prc = pd.read_parquet(filename)
    except FileNotFoundError:
        prc_stmt = """
            SELECT
                prc.datadate AS date,
                prc.gvkey,
                prcod / ajexdi AS open,
                prchd / ajexdi AS high,
                prcld / ajexdi AS low,
                prccd / ajexdi AS close,
                cshtrd AS volume
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
                AND cshtrd IS NOT NULL;
            """
        prc = query_wrds(prc_stmt)
        prc.to_parquet(filename)
    return prc


def get_fundamental_data():
    filename = config.basedir + "/../data/fund.parquet"
    try:
        fund = pd.read_parquet(filename)
    except FileNotFoundError:
        fund_stmt = """
            SELECT
                datadate AS date,
                gvkey,
                act / NULLIF(lct, 0) AS current_ratio,
                (act - invt) / NULLIF(lct, 0) AS quick_ratio,
                chee / NULLIF(lct, 0) AS cash_ratio,
                oancf / NULLIF(lct, 0) AS opr_cashflow_ratio,
                (dlc + dltt) / NULLIF(ceq, 0) AS debt_equity_ratio,
                (dltt + dlc) / NULLIF(at, 0) debt_asset_ratio,
                at / NULLIF(ceq, 0) AS fin_leverage,
                (dltt + dlc) / NULLIF(ebitda, 0) AS debt_ebitda_ratio,
                (dltt - chee) / NULLIF(ebitda, 0) AS net_debt_ebitda_ratio,
                ebitda / NULLIF(xint, 0) AS int_coverage_ratio,
                (dltt + dlc) / NULLIF(dltt + dlc + ceq, 0) AS debt_total_cap_ratio,
                cogs / NULLIF(invt, 0) AS invt_turnover,
                COALESCE(rect, artfs) / NULLIF(invt, 0) AS rec_turnover,
                revt / NULLIF(ppegt, 0) AS fixed_asset_turnover,
                revt / NULLIF(at, 0) AS total_asset_turnover,
                sale / NULLIF(wcap, 0) AS wcap_turnover,
                nicon / NULLIF(at, 0) AS roa,
                nicon / NULLIF(ceq, 0) AS roe,
                (revt - cogs) / NULLIF(revt, 0) AS gross_profit_margin,
                ebit / NULLIF(revt, 0) AS operating_margin,
                nicon / NULLIF(revt, 0) AS net_profit_margin,
                pi / NULLIF(revt, 0) AS pre_tax_earning_margin,
                ebitda / NULLIF(revt, 0) AS ebitda_margin,
                ebit / NULLIF(revt, 0) AS ebit_margin,
                ebit * (1 - txt / NULLIF(pi, 0)) / NULLIF(icapt, 0) AS roic,
                ceq / NULLIF(cshpria, 0) AS bvps,
                COALESCE(epsexcon, nicon/NULLIF(cshpria, 0)) AS eps,
                COALESCE(epsincon, (nicon + xido)/NULLIF(cshpria, 0))  AS eps_inextra, 
                fincf + ivncf + oancf AS cashflow,
                xrd / NULLIF(revt, 0) AS rd_sales_ratio,
                xrd / NULLIF(xopr, 0) AS re_opr_exp_ratio,
                -- Others
                at AS risk_size
            FROM
                comp_global_daily.g_funda
            WHERE
                exchg = ANY (ARRAY [104, 107, 132, 151, 154, 171, 192, 194, 201, 209, 256, 257, 273, 276, 286])
                AND curcd = 'EUR';
            """
        fund = query_wrds(fund_stmt)
        fund.to_parquet(filename)
    return fund


def get_cap_data():
    filename = config.basedir + "/../data/cap.parquet"
    try:
        cap = pd.read_parquet(filename)
    except FileNotFoundError:
        cap_stmt = """
            WITH x AS (
                SELECT
                    CAST(date_trunc('month',
                            datadate::date) + interval '1 month' AS date) AS date,
                    gvkey,
                    iid,
                    COALESCE(cshoc,
                        0) * prccd AS mcap
                FROM
                    comp_global_daily.g_secd
                WHERE
                    datadate >= '2000-01-01'::date
                    AND curcdd = 'EUR'
                    AND monthend = 1
                    AND exchg = ANY (ARRAY [104, 132, 151, 154, 171, 172, 192, 194, 201, 209, 228, 256, 257, 273, 286])
            )
            SELECT
                date, gvkey, mcap, PERCENT_RANK() OVER (PARTITION BY date ORDER BY mcap) AS mcap_pctl
            FROM
                x;
            """
        cap = query_wrds(cap_stmt).drop_duplicates(
            subset=["date", "gvkey"], keep="last"
        )
        cap.to_parquet(filename)
    return cap


def get_data():
    prc = get_price_data()
    fund = get_fundamental_data()
    cap = get_cap_data()
    gvkey = prc["gvkey"].unique()
    dates = pd.date_range(start="2000-01-01", end="2021-02-28")
    index = pd.MultiIndex.from_product([gvkey, dates], names=["gvkey", "date"])
    df = (
        pd.concat(
            [prc, fund, cap],
            axis=1,
            join="outer",
            ignore_index=True,
            keys=["date", "gvkey"],
        )
        .set_index(["gvkey", "date"])
        .reindex(index)
    )
    return df


def be_technical():
    pass


def be_fundamental():
    pass
