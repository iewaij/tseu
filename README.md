# Trading Strategies on the European Stock Market

## Market Data

## pandas-datareader 

`pandas_datareader` is a python package to download data from multiple sources, e.g. yahoo finance, Eurostat and OECD. As the package's name suggests, the data will be directly pulled into a pandas dataframe. The documentation for this package is limited, therefore you might need use `shift + tab` in jupyter notebooks or dig into [the source code](https://github.com/pydata/pandas-datareader) to understand the available parameters.

To install `pandas_datareader`: 

```
pip install wrds pandas_datareader
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

## WRDS
Wharton Research Data Services (WRDS) provides rich amount of data and fruitful tutorials. We will count on WRDS to pull security price, fundamental, and analyst data.

After finishing [initial setup](https://wrds-www.wharton.upenn.edu/pages/support/programming-wrds/programming-python/python-from-your-computer/#initial-setup-the-pgpass-file), you can create a new connection by using the following code:

```python
db = wrds.Connection(wrds_username='iewaij')
```

You can use this connection object (`db`) to query WRDS database. You can find company specific code using [WRDS company code lookup](https://wrds-web.wharton.upenn.edu/wrds/code_search/). Note that the most standard identifiers, such as companies’ tickers and CUSIPs, tend to change over time. Therefore, using GVKEY (Global Company Key) which is a unique number assigned to each company in the Compustat-Capital IQ database is always prefered. To pull the company information of BMW, you can query against [Compustat Global Daily](https://wrds-www.wharton.upenn.edu/data-dictionary/comp_global_daily/) library:

```python
db.raw_sql(
    """
    SELECT *
    FROM comp_global_daily.g_company
    WHERE gvkey = '100022'
    """)
```

The following code queries against the daily prices of BMW since 1/1/2001. Note that the adjusted price is PRICE/AJEXDI and you need to adjust that manually: 

```python
db.raw_sql(
    """
    SELECT datadate as date, prchd as high, prcld as low, prcod as open, prccd as close, cshtrd as volume, ajexdi 
    FROM comp_global_daily.g_sec_dprc
    WHERE datadate >= '2001-01-01'::date
    AND gvkey = '100022'
    AND iid = '01W'
    ORDER BY date
    """,
    date_cols=["datadate"])

```

You should always disconnect using `close()` when you exit your Python environment
or complete your data query download step and want to move onto another:

```python
db.close()
```

Or you can use `with` statement which automatically closes the connection when the query is finished:

```python
def get_data_wrds():
    with wrds.Connection(wrds_username="iewaij") as db:
        data = db.raw_sql(
            """
            SELECT datadate as date, prchd as high, prcld as low, prcod as open, prccd as close, cshtrd as volume, ajexdi 
            FROM comp_global_daily.g_sec_dprc
            WHERE datadate >= '2001-01-01'::date
            AND gvkey = '100022'
            AND iid = '01W'
            ORDER BY date
            """,
            date_cols=["datadate"],
        )
    return data
```
