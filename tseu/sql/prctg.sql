SELECT
    statpers AS date,
    gvkey,
    numest,
    numdown1m,
    numup1m,
    meanptg,
    medptg,
    stdev,
    ptghigh,
    ptglow
FROM
    ibes.ptgsum AS prctg
    JOIN comp_global_daily.g_security AS sec ON prctg.ticker = sec.ibtic
WHERE
    exchg = ANY (ARRAY [104, 132, 151, 154, 171, 172, 192, 194, 201, 209, 228, 256, 257, 273, 286])
    AND curr = 'EUR';
