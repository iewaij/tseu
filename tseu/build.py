import numpy as np
import pandas as pd
from pandas.tseries.offsets import DateOffset
from functools import reduce


def build_gvkeys(prc, fund, cap, min_pctl=0.5):
    gvkeys_prc = prc.gvkey.unique()
    gvkeys_fund = fund.gvkey.unique()
    gvkeys_cap = cap.loc[
        (cap.mcap_pctl > min_pctl) & (cap.date >= "2000-01-01"), :
    ].gvkey.unique()
    gvkeys = reduce(np.intersect1d, (gvkeys_prc, gvkeys_fund, gvkeys_cap))
    return gvkeys


def fill_year(df):
    first_date = df["date"].iloc[0]
    last_date = df["date"].iloc[-1]
    date_index = pd.date_range(
        pd.to_datetime(first_date),
        pd.to_datetime(last_date) + DateOffset(years=1),
        name="date",
    )
    return (
        df.drop("gvkey", axis=1)
        .set_index("date")
        .sort_index()
        .reindex(date_index, method="ffill")
    )


def fill_month(df):
    first_date = df["date"].iloc[0]
    last_date = df["date"].iloc[-1]
    date_index = pd.date_range(
        pd.to_datetime(first_date),
        pd.to_datetime(last_date) + DateOffset(months=1),
        name="date",
    )
    return (
        df.drop("gvkey", axis=1)
        .set_index("date")
        .sort_index()
        .reindex(date_index, method="ffill")
    )


def transform_fundamental(df):
    oa = df.att - df.che
    ol = df.att - df.dlc - df.dltt - df.mib - df.pstk - df.ceq
    chact = df.act - df.act.shift(1)
    chchee = df.chee - df.chee.shift(1)
    chlct = df.lct - df.lct.shift(1)
    chdlc = df.dlc - df.dlc.shift(1)
    chtxp = df.txp - df.txp.shift(1)
    chchee = df.chee - df.chee.shift(1)
    avg_at = (df.att + df.att.shift(1)) / 2
    nca = df.att - df.act - df.ivaeq
    ncl = df.ltt - df.lct - df.dltt
    ncoa = nca - ncl
    coa = df.act - df.che
    col = df.lct - df.dlc
    wc = df.act - df.che - df.lct + df.dlc
    fna = df.ivst + df.ivao
    fnl = df.dltt + df.dlc + df.pstk
    nfna = fna - fnl
    be = df.seq - df.pstk
    df = df.assign(
        # Accruals
        accruals_acc=((chact - chchee) - (chlct - chdlc - chtxp) - df.dp) / avg_at,
        accruals_chcoa=(coa - coa.shift(1)) / df.att.shift(1),
        accruals_chcol=(col - col.shift(1)) / df.att.shift(1),
        accruals_chnncwc=(wc - wc.shift(1)) / df.att.shift(1),
        accruals_chnncoa=(ncoa - ncoa.shift(1)) / df.att.shift(1),
        accruals_chncoa=(nca - nca.shift(1)) / df.att.shift(1),
        accruals_chncol=(ncl - ncl.shift(1)) / df.att.shift(1),
        accruals_chnfa=nfna - nfna.shift(1) / df.att.shift(1),
        accruals_chlti=(df.ivao - df.ivao.shift(1)) / df.att.shift(1),
        accruals_chce=(df.ceq - df.ceq.shift(1)) / df.att.shift(1),
        accruals_chfl=(
            df.dltt + df.dlc + df.pstk - (df.dltt + df.dlc + df.pstk).shift(1)
        )
        / df.att.shift(1),
        accruals_grii=(df.invt - df.invt.shift(1)) / ((df.att + df.att.shift(1)) / 2),
        accruals_ich=(df.invt - df.invt.shift(1)) / df.att.shift(1),
        accruals_igr=(df.invt - df.invt.shift(1)) / df.invt.shift(1),
        accruals_nwcch=(wc - wc.shift(1)) / df.att.shift(1),
        accruals_poa=(df.nicon - df.oancf) / abs(df.nicon),
        accruals_pta=(
            df.nicon - (-df.sstk + df.prstkc + df.dv + df.oancf + df.ivncf + df.fincf)
        )
        / abs(df.nicon),
        accruals_ta=((ncoa + wc + nfna) - (ncoa + wc + nfna).shift(1))
        / df.att.shift(1),
        # Efficiency
        efficiency_itr=df.cogs / df.invt,
        efficiency_rtr=df.revt / df.rect,
        efficiency_apr=df.cogs / df.ap,
        efficiency_dsi=365 * df.invt / df.cogs,
        efficiency_dso=365 * df.rect / df.revt,
        efficiency_dpo=365 * df.ap / df.cogs,
        # Profitablity
        profitability_at=df.revt / ((oa - ol) + (oa - ol).shift(1)) / 2,
        profitability_fat=df.revt / df.ppent,
        profitability_ct=df.revt / df.att.shift(1),
        profitability_gp=(df.revt - df.cogs) / df.att.shift(1),
        profitability_opta=(df.revt - df.cogs - df.xsga + df.xrd) / df.att,
        profitability_opte=(df.revt - df.cogs - df.xsga + df.xrd) / be,
        profitability_gpm=(df.revt - df.cogs) / df.revt,
        profitability_ebitdam=df.ebitda / df.revt,
        profitability_ebitm=df.ebit / df.revt,
        profitability_ptm=df.pi / df.revt,
        profitability_npm=df.nicon / df.revt,
        profitability_roa=df.nicon / df.att,
        profitability_roe=df.nicon / be,
        profitability_roic=(df.ebit * (df.nicon / df.pi)) / (df.dlc + df.dltt + df.teq),
        # Intangible
        intangible_rds=df.xrd / df.revt,
        # Investment
        investment_agr=df.att / df.att.shift(1),
        investment_cdi=np.log(
            (df.dltt + df.dlc) / (df.dltt.shift(5) + df.dlc.shift(5))
        ),
        investment_chnoa=(
            ((oa - ol) / df.att.shift(1)) - (((oa - ol) / df.att.shift(1)).shift(1))
        )
        / df.att.shift(1),
        investment_chppeia=(
            (df.ppegt - df.ppegt.shift(1)) + (df.invt - df.invt.shift(1))
        )
        / df.att.shift(1),
        investment_griltnoa=(
            ((oa - ol) / df.att.shift(1))
            - (((oa - ol) / df.att.shift(1)).shift(1))
            - ((chact - chchee) - (chlct - chdlc - chtxp) - df.dp) / avg_at
        ),
        investment_inv=(df.capx / df.revt)
        / (
            (
                (df.capx.shift(1) / df.revt.shift(1))
                + (df.capx.shift(2) / df.revt.shift(2))
                + (df.capx.shift(3) / df.revt.shift(3))
            )
            / 3
        ),
        investment_ndf=(df.dltis - df.dltr + df.dlcch)
        / ((df.att + df.att.shift(1)) / 2),
        investment_nef=(df.sstk - df.prstkc - df.dv) / ((df.att + df.att.shift(1)) / 2),
        investment_noa=(oa - ol) / df.att.shift(1),
        investment_noach=(ncoa - ncoa.shift(1)) / df.att,
        investment_txfin=(df.sstk - df.dv - df.prstkc + df.dltis - df.dltr) / df.att,
        # Leverage
        leverage_de=(df.dlc + df.dltt) / be,
        leverage_da=(df.dltt + df.dlc) / df.att,
        leverage_fl=df.att / be,
        leverage_deda=(df.dltt + df.dlc) / df.ebitda,
        leverage_ndeda=(df.dltt + df.dlc - df.chee) / df.ebitda,
        leverage_eic=df.ebit / df.xint,
        leverage_edaic=df.ebitda / df.xint,
        leverage_cac=df.ch / df.xint,
        leverage_dcap=(df.dltt + df.dlc) / (df.dltt + df.dlc + df.teq),
        leverage_cad=df.oancf / (df.dlc + df.dltt),
        # Liquidity
        liquid_cur=df.act / df.lct,
        liquid_qur=(df.act - df.invt) / df.lct,
        liquid_car=df.chee / df.lct,
        liquid_opr=df.oancf / df.lct,
        liquid_capxr=df.capx / df.oancf,
        # Market
        # Other
        other_ir=(df.icapt - df.icapt.shift(1)) / (df.ebit * (df.nicon / df.pi)),
        other_nopat_g=(df.icapt - df.icapt.shift(1)) / df.icapt,
        other_rev_cagr_3=((df.revt / df.revt.shift(3)) ** (1 / 3)) - 1,
        other_ebitda_cagr_3=((df.ebitda / df.ebitda.shift(3)) ** (1 / 3)) - 1,
    )
    return df


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
