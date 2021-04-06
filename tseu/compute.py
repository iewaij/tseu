import numpy as np
import pandas as pd


def parallel_apply(df, by, func):
    from multiprocessing import Pool

    with Pool() as pool:
        result = pool.map(func, [g for _, g in df.groupby(by)])
    return pd.concat(result)


def peg(prccd, eps):
    return prccd / eps / eps.pct_change(1).replace(0, np.nan).fillna(method="ffill")


def rsi(close, window):
    diff = close.diff(1)
    up_chg = 0 * diff
    down_chg = 0 * diff
    up_chg[diff > 0] = diff[diff > 0]
    down_chg[diff < 0] = diff[diff < 0]
    up_chg_avg = up_chg.ewm(com=window - 1, min_periods=window).mean()
    down_chg_avg = down_chg.ewm(com=window - 1, min_periods=window).mean()
    rs = np.abs(up_chg_avg / down_chg_avg)
    rsi = 1 - 1 / (1 + rs)
    return rsi


def compute_accruals(gf):
    gf["accruals_acc"] = (
        (gf.chact - gf.chche) - (gf.chlct - gf.chdlc - gf.chtxp) - gf.dp
    ) / gf.avatt
    gf["accruals_chcoa"] = gf.coa.diff(1) / gf.att.shift(1)
    gf["accruals_chcol"] = gf.col.diff(1) / gf.att.shift(1)
    gf["accruals_chnncwc"] = gf.wc.diff(1) / gf.att.shift(1)
    gf["accruals_chnncoa"] = gf.coa.diff(1) / gf.att.shift(1)
    gf["accruals_chncoa"] = gf.nca.diff(1) / gf.att.shift(1)
    gf["accruals_chncol"] = gf.ncl.diff(1) / gf.att.shift(1)
    gf["accruals_chnfa"] = gf.nfna.diff(1) / gf.att.shift(1)
    gf["accruals_chlti"] = gf.ivao.diff(1) / gf.att.shift(1)
    gf["accruals_chce"] = gf.ceq.diff(1) / gf.att.shift(1)
    gf["accruals_chfl"] = (gf.dltt + gf.dlc + gf.pstk).diff(1) / gf.att.shift(1)
    gf["accruals_grii"] = gf.invt.diff(1) / (gf.att.diff(1) / 2)
    gf["accruals_ich"] = gf.invt.diff(1) / gf.att.shift(1)
    gf["accruals_igr"] = gf.invt.diff(1) / gf.invt.shift(1)
    gf["accruals_nwcch"] = gf.wc.diff(1) / gf.att.shift(1)
    gf["accruals_poa"] = (gf.nicon - gf.oancf) / gf.nicon.abs()
    gf["accruals_pta"] = (
        gf.nicon - (-gf.sstk + gf.prstkc + gf.dv + gf.oancf + gf.ivncf + gf.fincf)
    ) / abs(gf.nicon)
    gf["accruals_ta"] = (gf.ncoa + gf.wc + gf.nfna).diff(1) / gf.att.shift(1)
    return gf


def compute_efficiency(gf):
    gf["efficiency_itr"] = gf.cogs / gf.invt
    gf["efficiency_rtr"] = gf.revt / gf.rect
    gf["efficiency_apr"] = gf.cogs / gf.ap
    gf["efficiency_dsi"] = 365 * gf.invt / gf.cogs
    gf["efficiency_dso"] = 365 * gf.rect / gf.revt
    gf["efficiency_dpo"] = 365 * gf.ap / gf.cogs
    gf["efficiency_dopl"] = (gf.ebit / gf.ebit.shift(1) - 1) / (
        gf.revt / gf.revt.shift(1) - 1
    )
    return gf


def compute_intangible(gf):
    gf = gf.assign(intangible_rds=gf.xrd / gf.revt)
    return gf


def compute_investment(gf):
    # ignore warnings for numpy log divided by zero and nan
    with np.errstate(divide="ignore", invalid="ignore"):
        gf["investment_cdi"] = np.log(
            (gf.dltt + gf.dlc) / (gf.dltt.shift(5) + gf.dlc.shift(5))
        )
    gf["investment_agr"] = gf.att / gf.att.shift(1)
    gf["investment_chnoa"] = (
        (gf.noa / gf.att.shift(1)) - ((gf.noa / gf.att.shift(1)).shift(1))
    ) / gf.att.shift(1)
    gf["investment_chppeia"] = (gf.ppegt.diff(1) + gf.invt.diff(1)) / gf.att.shift(1)
    gf["investment_griltnoa"] = (gf.noa / gf.att.shift(1)).diff(1) - (
        ((gf.chact - gf.chche) - (gf.chlct - gf.chdlc - gf.chtxp) - gf.dp) / gf.avatt
    )
    gf["investment_inv"] = (
        (gf.capx / gf.revt)
        / (
            (gf.capx.shift(1) / gf.revt.shift(1))
            + (gf.capx.shift(2) / gf.revt.shift(2))
            + (gf.capx.shift(3) / gf.revt.shift(3))
        )
        / 3
    )
    gf["investment_ngf"] = (gf.dltis - gf.dltr + gf.dlcch) / (
        (gf.att + gf.att.shift(1)) / 2
    )
    gf["investment_nef"] = (gf.sstk - gf.prstkc - gf.dv) / (
        (gf.att + gf.att.shift(1)) / 2
    )
    gf["investment_noa"] = gf.noa / gf.att.shift(1)
    gf["investment_noach"] = gf.ncoa.diff(1) / gf.att
    gf["investment_txfin"] = (gf.sstk - gf.dv - gf.prstkc + gf.dltis - gf.dltr) / gf.att
    return gf


def compute_leverage(gf):
    gf["leverage_de"] = (gf.dlc + gf.dltt) / gf.ceq
    gf["leverage_da"] = (gf.dltt + gf.dlc) / gf.att
    gf["leverage_fl"] = gf.att / gf.ceq
    gf["leverage_deda"] = (gf.dltt + gf.dlc) / gf.ebitda
    gf["leverage_ndeda"] = (gf.dltt + gf.dlc - gf.che) / gf.ebitda
    gf["leverage_eic"] = gf.ebit / gf.xint
    gf["leverage_edaic"] = gf.ebitda / gf.xint
    gf["leverage_cac"] = gf.ch / gf.xint
    gf["leverage_dcap"] = (gf.dltt + gf.dlc) / (gf.dltt + gf.dlc + gf.teq)
    gf["leverage_cad"] = gf.oancf / (gf.dlc + gf.dltt)
    return gf


def compute_liquidity(gf):
    gf["liquidity_cur"] = gf.act / gf.lct
    gf["liquidity_qur"] = (gf.act - gf.invt) / gf.lct
    gf["liquidity_car"] = gf.che / gf.lct
    gf["liquidity_opr"] = gf.oancf / gf.lct
    gf["liquidity_capxr"] = gf.capx / gf.oancf
    return gf


def compute_profitability(gf):
    gf["profitability_at"] = gf.revt / (gf.noa + gf.noa.shift(1)) / 2
    gf["profitability_fat"] = gf.revt / gf.ppent
    gf["profitability_ct"] = gf.revt / gf.att.shift(1)
    gf["profitability_gp"] = (gf.revt - gf.cogs) / gf.att.shift(1)
    gf["profitability_opta"] = (gf.revt - gf.cogs - gf.xsga + gf.xrd) / gf.att
    gf["profitability_opte"] = (gf.revt - gf.cogs - gf.xsga + gf.xrd) / gf.ceq
    gf["profitability_gpm"] = (gf.revt - gf.cogs) / gf.revt
    gf["profitability_ebitdam"] = gf.ebitda / gf.revt
    gf["profitability_ebitm"] = gf.ebit / gf.revt
    gf["profitability_ptm"] = gf.pi / gf.revt
    gf["profitability_npm"] = gf.nicon / gf.revt
    gf["profitability_roa"] = gf.nicon / gf.att
    gf["profitability_roe"] = gf.nicon / gf.ceq
    gf["profitability_roic"] = (gf.ebit * (gf.nicon / gf.pi)) / (
        gf.dlc + gf.dltt + gf.teq
    )
    return gf


def compute_other(gf):
    gf = gf.assign(
        other_size=gf.att,
        other_ia=gf.att / gf.att.shift(1),
        other_ir=gf.icapt.diff(1) / (gf.ebit * (gf.nicon / gf.pi)),
        other_nopatgr=gf.icapt.diff(1) / gf.icapt,
        other_rev_cagr_3=((gf.revt / gf.revt.shift(3)) ** (1 / 3)) - 1,
        other_ebitda_cagr_3=((gf.ebitda / gf.ebitda.shift(3)) ** (1 / 3)) - 1,
    )
    return gf


def compute_gf(gf):
    gf["coa"] = gf.act - gf.che
    gf["col"] = gf.lct - gf.dlc
    gf["wc"] = gf.act - gf.che - gf.lct + gf.dlc
    gf["fna"] = gf.ivst + gf.ivao
    gf["fnl"] = gf.dltt + gf.dlc + gf.pstk
    gf["nfna"] = gf.fna - gf.fnl
    gf["oa"] = gf.att - gf.che
    gf["ol"] = gf.att - gf.dlc - gf.dltt - gf.mib - gf.pstk - gf.ceq
    gf["noa"] = gf.oa - gf.ol
    gf["nca"] = gf.att - gf.act - gf.ivaeq
    gf["ncl"] = gf.ltt - gf.lct - gf.dltt
    gf["ncoa"] = gf.nca - gf.ncl
    gf["chact"] = gf.act.diff(1)
    gf["chche"] = gf.che.diff(1)
    gf["chlct"] = gf.lct.diff(1)
    gf["chdlc"] = gf.dlc.diff(1)
    gf["chtxp"] = gf.txp.diff(1)
    gf["avatt"] = (gf.att + gf.att.shift(1)) / 2
    return (
        gf.pipe(compute_accruals)
        .pipe(compute_efficiency)
        .pipe(compute_intangible)
        .pipe(compute_investment)
        .pipe(compute_leverage)
        .pipe(compute_liquidity)
        .pipe(compute_profitability)
        .pipe(compute_other)
    )


def compute_fundamental(df):
    return parallel_apply(df, "gvkey", compute_gf)


def compute_technical(gp):
    gp["close"] = gp.prccd / gp.ajexdi
    gp["mom_high_12m"] = 1 - gp.close / gp.close.rolling(12).max()
    gp["mom_1m"] = gp.close.pct_change(1)
    windows = [3, 6, 12, 24]
    for window in windows:
        std_window = gp.mom_1m.rolling(window).std()
        mean_window = gp.mom_1m.rolling(window).mean()
        min_window = gp.close.rolling(window).min()
        max_window = gp.close.rolling(window).max()
        gp[f"std_{window}m"] = std_window
        gp[f"sh_{window}m"] = mean_window / std_window
        gp[f"mom_{window}m"] = gp.close.shift(1).pct_change(window - 1)
        gp[f"ema_{window}m"] = (
            gp.close / gp.close.ewm(span=window, adjust=False).mean() - 1
        )
        gp[f"qt_{window}m"] = gp.close.rolling(window).quantile(0.75) / gp.close
        gp[f"scosc_{window}m"] = (gp.close - min_window) / (max_window - min_window)
        gp[f"rsi_{window}m"] = rsi(gp.close, window)
    return gp


def compute_price(prc):
    return parallel_apply(prc, "gvkey", compute_technical)


def compute_data(data):
    data["mom_industrial_1m"] = data.mom_1m.groupby(data.sic_2).mean()
    data["mom_industrial_3m"] = data.mom_3m.groupby(data.sic_2).mean()
    data["mom_industrial_6m"] = data.mom_6m.groupby(data.sic_2).mean()
    data["mom_industrial_12m"] = data.mom_12m.groupby(data.sic_2).mean()
    data["analyst_high"] = (data.prccd - data.ptghigh) / data.prccd
    data["analyst_mean"] = (data.prccd - data.meanptg) / data.prccd
    data["analyst_low"] = (data.prccd - data.ptglow) / data.prccd
    data["market_cap"] = data.prccd * data.cshoc
    data["market_eps"] = (data.nicon * 10 ** 6) / data.cshoc
    data["market_peg"] = data.groupby("gvkey", group_keys=False).apply(
        lambda x: peg(x.prccd, x.market_eps)
    )
    data["market_dyr"] = data.dvc * 10 ** 6 / data.cshoc / data.prccd
    data["market_pe"] = data.market_cap / (data.nicon * 10 ** 6)
    data["market_pch"] = data.market_cap / (data.oancf * 10 ** 6)
    data["market_ps"] = data.market_cap / (data.revt * 10 ** 6)
    data["market_mb"] = data.market_cap / (data.ceq * 10 ** 6)
    data["ev"] = (
        data.market_cap
        + (data.dlc + data.dltt + data.pstk + data.mib - data.che) * 10 ** 6
    )
    data["market_evs"] = data.ev / (data.revt * 10 ** 6)
    data["market_eveda"] = data.ev / (data.ebitda * 10 ** 6)
    data["market_eve"] = data.ev / (data.ebit * 10 ** 6)
    data["market_evedacpx"] = data.ev / ((data.ebitda - data.capx) * 10 ** 6)
    data["market_evocf"] = data.ev / ((data.oancf) * 10 ** 6)
    data["intangible_rdm"] = data.xrd / data.market_cap
    return data.rename(
        columns={
            "numest": "analyst_numest",
            "ptgdown": "analyst_ptgdown",
            "ptgup": "analyst_ptgup",
            "meanptg": "analyst_meanptg",
            "ptghigh": "analyst_ptghigh",
            "ptglow": "analyst_ptglow",
            "numrec": "analyst_numrec",
            "recdown": "analyst_recdown",
            "recup": "analyst_recup",
            "meanrec": "analyst_meanrec",
            "buypct": "analyst_buypct",
            "holdpct": "analyst_holdpct",
            "sellpct": "analyst_sellpct",
        }
    )
