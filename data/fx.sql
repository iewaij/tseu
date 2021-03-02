SELECT
    curr.datadate,
    curr. tocurd,
    curr.exratd / eur.exratd AS exratd
FROM (
    SELECT
        datadate,
        tocurd,
        exratd
    FROM
        comp.g_exrt_dly
    WHERE
        tocurd = ANY (ARRAY ['ATS', 'AUD', 'BBD', 'BEF', 'BWP', 'CAD', 'CHF', 'CNY', 'CZK', 'DEM', 'DKK', 'EEK', 'EGP', 'ESP', 'FIM', 'FRF', 'GBP', 'GEL', 'GRD', 'HKD', 'HUF', 'IEP', 'ILS', 'INR', 'ISK', 'ITL', 'JPY', 'LTL', 'MXN', 'MYR', 'NLG', 'NOK', 'NZD', 'PGK', 'PLN', 'PTE', 'RUB', 'SAR', 'SEK', 'SGD', 'SKK', 'TRY', 'UAH', 'USD', 'XAF', 'XOF', 'ZAR', 'ZMK', 'ZMW'])) AS curr
    JOIN (
        SELECT
            datadate, exratd
        FROM
            comp.g_exrt_dly
        WHERE
            tocurd = 'EUR') AS eur ON curr.datadate = eur.datadate
ORDER BY
    curr.datadate DESC;
