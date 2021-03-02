SELECT
    datadate,
    conm,
    -- currency code
    curcd,
    -- current assets
    act,
    -- cash and cash qquivalents at end of year
    chee,
    -- short-term investments
    ivst,
    -- cash and short-term investments, che = chee + ivst
    che,
    -- net accounts receivable
    coalesce(rectr, rectrfs) AS rectr,
    -- other current receivables
    coalesce(recco, reccofs) AS recco,
    -- total receivables, rect = rectr + recco
    coalesce(rect, artfs) AS rect,
    -- inventories
    invt,
    -- other current assets
    coalesce(aco, acox, acofs, acoxfs) AS aco,
    -- total current assets
    act,
    -- net property, plant and equipment
    ppent,
    -- 	total intangible assets
    intan,
    -- other assets
    ao,
    -- total assets
    at,
    -- accounts payable
    ap,
    -- total current debt
    dlc,
    -- total current liabilities
    lct,
    -- long-term debt
    dltt,
    -- total liabilities
    lt,
    -- common stock
    cstk,
    -- retained earnings, sometimes 0, need to be adjusted
    re,
    -- total common equity
    ceq,
    -- total equity
    coalesce(teq, lse - lt) AS teq,
    -- total liabilities and equity
    lse,
    -- total debt
    dlc + dltt AS total_debt,
    -- net debt
    dlc + dltt - che AS net_debt,
    -- working capital
    wcap,
    -- revenue
    revt,
    -- costs of goods sold
    cogs,
    -- research and development expense
    xrd,
    -- interests expense
    xint,
    -- income tax
    txt,
    -- net income from continuing operations
    nicon,
    -- net income from extraordinary items and discontinued operations
    xido,
    -- net income
    nicon + xido AS net_income,
    -- ebit
    ebit,
    -- ebitda
    ebitda,
    -- eps excluding extraordinary items
    epsexcon,
    -- eps including extraordinary items
    epsincon,
    -- invested capital, icapt = teq + dltt
    icapt,
    -- cash from operations from continuing and discontinued operations
    oancf,
    -- CAPEX
    capx,
    -- current ratio
    act / NULLIF(lct, 0) AS current_ratio,
    -- quick ratio
    (act - invt) / NULLIF(lct, 0) AS quick_ratio,
    -- cash ratio
    chee / NULLIF(lct, 0) AS cash_ratio,
    -- operating cash flow ratio
    oancf / NULLIF(lct, 0) AS operating_cashflow_ratio,
    -- debt to equity
    (dlc + dltt) / NULLIF(teq, 0) AS d_e_ratio,
    -- debt to asset
    (dltt + dlc) / NULLIF(at, 0) d_a_ratio,
    -- debt to ebitda
    (dltt + dlc) / NULLIF(ebitda, 0) AS d_ebitda_ratio,
    -- net debt to ebitda (need clarification)
    (dltt - chee) / NULLIF(ebitda, 0) AS net_d_ebitda_ratio,
    -- interest coverage ratio
    ebitda / NULLIF(xint, 0) AS int_coverage_ratio,
    -- debt to total capitalization
    (dltt + dlc) / NULLIF(dltt + dlc + teq, 0) AS d_total_cap_ratio,
    -- inventory turnover
    cogs / NULLIF(invt, 0) AS invt_turnover,
    -- fixed assets turnover
    revt / NULLIF(ppegt, 0) AS f_asset_turnover,
    -- total asset turnover
    revt / NULLIF(at, 0) AS t_asset_turnover,
    -- gross profit margin
    (revt - cogs) / NULLIF(revt, 0) AS gross_profit_margin,
    -- operating margin
    ebit / NULLIF(revt, 0) AS operating_margin,
    -- ROA
    nicon / NULLIF(at, 0) AS roa,
    -- ROE
    nicon / NULLIF(teq, 0) AS roe,
    -- net profit margin
    nicon / NULLIF(revt, 0) AS net_profit_margin,
    -- ebitda margin
    ebitda / NULLIF(revt, 0) AS ebitda_margin,
    -- book value per share
    ceq / NULLIF(cshpria, 0) AS bvps,
    -- eps
    nicon / NULLIF(cshpria, 0) AS eps
    -- p/e
    -- p/cashflow
    -- peg
    -- z-score
    -- credit ratings
    -- investment rate
    -- r&d expense to sales
FROM
    comp_global_daily.g_funda
WHERE
    exchg = ANY (ARRAY [104, 107, 132, 151, 154, 171, 192, 194, 201, 209, 256, 257, 273, 276, 286]);
