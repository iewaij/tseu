SELECT
    prc.datadate AS date,
    prc.gvkey,
    prcod / ajexdi AS OPEN,
    prchd / ajexdi AS high,
    prcld / ajexdi AS low,
    prccd / ajexdi AS CLOSE,
    cshtrd AS volume
FROM ( SELECT DISTINCT
        gvkey,
        iid
    FROM
        comp_global_daily.g_funda
    WHERE
        exchg = ANY (ARRAY [104, 132, 151, 154, 171, 172, 192, 194, 201, 209, 228, 256, 257, 273, 286])
        AND curcd = 'EUR') AS fund
    JOIN comp_global_daily.g_sec_dprc AS prc ON fund.gvkey = prc.gvkey
        AND fund.iid = prc.iid
WHERE
    curcdd = 'EUR'
    AND cshtrd IS NOT NULL
    AND datadate >= '1999-01-01';
