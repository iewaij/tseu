SELECT
    anndats AS date,
    gvkey,
    actual,
    surpmean,
    surpstdev,
    suescore
FROM
    ibes.surpsum AS surp
    JOIN comp_global_daily.g_security AS sec ON surp.ticker = sec.ibtic
WHERE
    exchg = ANY (ARRAY [104, 132, 151, 154, 171, 172, 192, 194, 201, 209, 228, 256, 257, 273, 286]);
