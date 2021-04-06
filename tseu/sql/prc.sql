SELECT
    prc.gvkey,
    prc.datadate AS date,
    cshoc,
    ajexdi,
    prcod,
    prchd,
    prcld,
    prccd,
    cshtrd
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
    AND cshtrd > 0
    AND cshoc > 0
    AND datadate >= '1999-01-01';
