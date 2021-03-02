# Data
- [Data](#data)
  - [Source](#source)
    - [Parquet](#parquet)
    - [pandas-datareader](#pandas-datareader)
    - [WRDS](#wrds)
  - [Company](#company)
  - [Price](#price)
  - [Exchange](#exchange)
  - [Fundamentals](#fundamentals)
  - [Estimates](#estimates)
  - [Currenct Exchange Rates](#currenct-exchange-rates)

## Source

### Parquet

Prepared data is availble as Apache Parquet file. To read parquet file, first install:

```
pip install pyarrow
```

Then use `pd.read_parquet()`: 

```python
df_ohlcv = pd.read_parquet("sec_ohlcv.parquet")
```

### pandas-datareader 

`pandas_datareader` is a python package to download data from multiple sources, e.g. yahoo finance, Eurostat and OECD. As the package's name suggests, the data will be directly pulled into a pandas dataframe. The documentation for this package is limited, therefore you might need use `shift + tab` in jupyter notebooks or dig into [the source code](https://github.com/pydata/pandas-datareader) to understand the available parameters.

To install `pandas_datareader`: 

```
pip install pandas_datareader
```

The following code pulls BMW historical (split and dividend) adjusted price between 1/1/2001 and 1/2/2021 from yahoo finance:

```python
import pandas_datareader as pdr
pdr.get_data_yahoo("BMW.DE", start="2001-01-01", end="2021-02-01")
```

By specifying `adjust_price=True`, `pandas_datareader` adjusts all prices (`Open`, `High`, `Low`, `Close`) based on `Adj Close` price. It also adds `Adj_Ratio` column and drops `Adj Close`.

```python
pdr.get_data_yahoo("BMW.DE", start="2001-01-01", end="2021-02-01", adjust_price=True)
```

You may also download multiple stock data at the same time, though you need some pandas kung-fu to transform the shape of the dataframe:

```python
data = pdr.get_data_yahoo(["ADS.DE", "BMW.DE"], start="2001-01-01", end="2021-02-01", adjust_price=True)
df = data.stack(level=1).reset_index(level=1)
```

Making the same request repeatedly can use a lot of bandwidth, slow down your code and may result in your IP being banned. `pandas-datareader` allows you to cache queries into a `sqlite` database using `requests_cache`. First make sure you have installed `requests_cache`:

```
pip install requests_cache
```

The following function can download and cache your requests for BMW historical price between 1/1/2001 and today from yahoo finance. `web.DataReader()` exposes limited parameters, therefore we can only get adjusted closing price:

```python
from datetime import timedelta, date
import pandas_datareader.data as web
import requests_cache

def get_data(symbols, start="1/1/2001", end=date.today()):
    session = requests_cache.CachedSession(
        cache_name="cache", backend="sqlite", expire_after=timedelta(days=1)
    )
    data = web.DataReader(symbols, "yahoo", start, end, session=session)
    return data

get_data("BMW.DE")
```

### WRDS
Wharton Research Data Services (WRDS) is online platform that provides access to analytics and historical financial and accounting data for corporations and banks, historical economic data, and tutorials on how to access them. We will count on WRDS to pull security price, fundamental, and analyst data.

After finishing [initial setup](https://wrds-www.wharton.upenn.edu/pages/support/programming-wrds/programming-python/python-from-your-computer/#initial-setup-the-pgpass-file), you can initialize a new connection by using the following code:

```python
db = wrds.Connection(wrds_username='WRDS_USERNAME')
```

You can use this connection object (`db`) to query WRDS database:

```python
db.raw_sql(
    """
    SELECT COL_1, COL_2 FROM SCHEMA.TABLE
    """,
    date_cols=["DATECOLUMN"])
```

Or directly pull the table:

```python
db.get_table(library="SCHEMA", table="TABLE", columns=["COL_1", "COL_2"], obs=10, index_col="COL_INDEX", date_cols="DATECOLUMN")
```

Always disconnect using `close()` when you exit your Python environment or complete your data query download step and want to move onto another:

```python
db.close()
```

You can also use `with` statement which automatically closes the connection when the query is finished:

```python
def get_data_wrds():
    with wrds.Connection(wrds_username="WRDS_USERNAME") as db:
        data = db.raw_sql(
            """
            SELECT COL_1, COL_2 FROM SCHEMA.TABLE
            """,
            date_cols=["DATECOLUMN"],
        )
    return data
```
## Company

You can find company specific code using [WRDS company code lookup](https://wrds-web.wharton.upenn.edu/wrds/code_search/). Note that the most standard identifiers, such as companies’ tickers and CUSIPs, tend to change over time. Therefore, using GVKEY (Global Company Key) which is a unique number assigned to each company in the Compustat is always prefered. To pull the company information of BMW from [Compustat Global Daily](https://wrds-www.wharton.upenn.edu/data-dictionary/comp_global_daily/) library:

```sql
SELECT *
FROM comp_global_daily.g_company
WHERE gvkey = '100022';
```

## Price
The following code queries against the daily prices of BMW since 1/1/2001, note that the adjusted price is PRICE/AJEXDI:

```sql
SELECT
    datadate,
    prcod / ajexdi AS adj_prcod,
    prchd / ajexdi AS adj_prchd,
    prcld / ajexdi AS adj_prcld,
    prccd / ajexdi AS adj_prccd,
    cshtrd
FROM
    comp_global_daily.g_sec_dprc
WHERE
    datadate >= '2001-01-01'::date
    AND gvkey = '100022'
    AND iid = '01W'
ORDER BY
    date;
```

TRFD includes cash equivalent distributions, reinvestment of dividends and the compounding effect of dividends paid on reinvested dividends. To compute returns, the dividend ajusted price is PRICE/AJEXDI×TRFD:

```sql
SELECT
    datadate,
    prcod / ajexdi * trfd AS r_prcod,
    prchd / ajexdi * trfd AS r_prchd,
    prcld / ajexdi * trfd AS r_prcld,
    prccd / ajexdi * trfd AS r_prccd,
    cshtrd
FROM
    comp_global_daily.g_sec_dprc
WHERE
    datadate >= '2001-01-01'::date
    AND gvkey = '100022'
    AND iid = '01W'
ORDER BY
    date;
```

## Exchange
We will limit our scope to [major European exchanges](https://fese.eu/app/uploads/2020/07/European-Exchange-Report-2019_Final.pdf) in Austria, Belgium, Denmark, Finland, France, Germany, Ireland, Italy, Luxembourg, the Netherlands, Norway, Portugal, Spain, Sweden, Switzerland and the United Kingdom. Here are their codes in WRDS:

| exchg |        exchgdesc        |
| :---: | :---------------------: |
|  104  | NYSE Euronext Amsterdam |
|  132  | NYSE Euronext Brussels  |
|  151  |     Swiss Exchange      |
|  154  |   Deutsche Boerse AG    |
|  171  |          XETRA          |
|  172  |  Irish Stock Exchange   |
|  192  |  NYSE Euronext Lisbon   |
|  194  |  London Stock Exchange  |
|  201  |     Bolsa De Madrid     |
|  209  |     Borsa Italiana      |
|  228  |      Oslo Bors ASA      |
|  256  |    NASDAQ OMX Nordic    |
|  257  |    Boerse Stuttgart     |
|  273  |      Wiener Börse       |
|  286  |   NYSE Euronext Paris   |

If you are looking for other exchanges' codes, run the following queries:

```sql
SELECT
	*
FROM
	comp_global_daily.r_ex_codes;
```

To look up companies' listed stock on major European exchanges using EUR, run the following querry:

```sql
SELECT
    sec.gvkey,
    sec.iid,
    excntry
FROM
    comp_global_daily.g_security AS sec
    JOIN comp_global_daily.g_funda AS fund ON sec.gvkey = fund.gvkey
        AND sec.iid = fund.iid
WHERE
    sec.exchg = ANY (ARRAY [104, 107, 132, 151, 154, 171, 192, 194, 201, 209, 256, 257, 273, 276, 286]);
```

Global companies like BMW list their shares on multiple exchanges. We want to find the reference listing which also has available fundamental data. To find European company's stock issues which links to their fundamental data, run the following query:

```sql
SELECT
	*
FROM
	comp_global_daily.g_security
WHERE
	exchg = ANY (ARRAY[104,132, 171, 151, 192, 194, 201, 209, 256, 286])
	AND ibtic IS NOT NULL;
```

Using the query below, we can find the daily stock prices for companies on major European exchanges using EUR:

```sql
SELECT
    datadate,
    price.gvkey,
    curcdd,
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
    AND price.cshtrd IS NOT NULL
    AND price.gvkey = eu.gvkey
    AND price.iid = eu.iid;
```

## Fundamentals

Fundamenatal data is available on WRDS as a table called `g_fundq`. The columns' definition can be found [here](https://wrds-www.wharton.upenn.edu/data-dictionary/comp_global_daily/g_fundq/). The following code queries yearly fundamental data of Siemens AG in 2005:

```sql
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
    nicon + xido as net_income,
    -- ebit
    ebit,
    -- ebitda
    ebitda,
    -- eps excluding extraordinary items
    epsexcon,
    -- eps including extraordinary items
    epsincon,
    -- Invested capital, icapt = teq + dltt
    icapt,
    -- cash from operations from continuing and discontinued operations
    oancf,
    -- CAPEX
    capx
FROM
    comp_global_daily.g_funda
WHERE
    gvkey = '019349'
    AND datadate = '2005-09-30'::date;
```

## Estimates

It is also possible to include the estimates made by analysts to improve the model performance. The surprise summary data can be queried using the following code: 

```sql
SELECT
    *
FROM
    ibes.surpsum AS surprise,
    (
        SELECT
            *
        FROM
            comp_global_daily.g_security
        WHERE
            exchg = ANY (ARRAY [104,132, 171, 151, 192, 194, 201, 209, 256, 286])
            AND ibtic IS NOT NULL) AS security_eu
WHERE
    surprise.ticker = security_eu.ibtic
    AND surprise.anndats >= '2001-01-01'::date
ORDER BY
    surprise.anndats;
```

## Currenct Exchange Rates
```sql
SELECT
    curr.datadate,
    curr.tocurd,
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
            tocurd = 'EUR') AS eur ON curr.datadate = eur.datadate;
```
