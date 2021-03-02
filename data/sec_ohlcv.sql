SELECT
    datadate,
    price.gvkey,
    curcdd,
    prcstd,
    prcod / ajexdi AS prcod,
    prchd / ajexdi AS prchd,
    prcld / ajexdi AS prcld,
    prccd / ajexdi AS prccd,
    cshtrd
FROM
    comp_global_daily.g_secd AS price,
    ( SELECT DISTINCT
            gvkey,
            iid
        FROM
            comp_global_daily.g_funda) AS eu
WHERE
    datadate >= '2000-01-01'::date
    AND exchg = ANY (ARRAY [104, 132, 151, 154, 171, 172, 192, 194, 201, 209, 228, 256, 257, 273, 286])
    AND curcdd = 'EUR'
    AND cshtrd IS NOT NULL
    AND price.gvkey = eu.gvkey
    AND price.iid = eu.iid;
