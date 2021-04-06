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
