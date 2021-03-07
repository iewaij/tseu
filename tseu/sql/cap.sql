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
        curcdd = 'EUR'
        AND monthend = 1
        AND exchg = ANY (ARRAY [104, 132, 151, 154, 171, 172, 192, 194, 201, 209, 228, 256, 257, 273, 286])
        AND datadate >= '1999-12-01')
SELECT
    date, gvkey, iid, mcap, PERCENT_RANK() OVER (PARTITION BY date ORDER BY mcap) AS mcap_pctl
FROM
    x;
