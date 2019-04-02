import datetime
import MySQLdb as mdb
import pandas as pd
pd.core.common.is_list_like = pd.api.types.is_list_like
import pandas_datareader.data as web
from ratelimiter import RateLimiter
import dbconf.py

# Obtain a database connection to the MySQL instance
db_host = dbconf.db_host
db_user = dbconf.db_user
db_pass = dbconf.db_pass
db_name = dbconf.db_name
con = mdb.connect(db_host, db_user, db_pass, db_name)


def obtain_list_of_db_tickers():
    """Obtains a list of the ticker symbols in the database."""
    with con:
        cur = con.cursor()
        cur.execute("SELECT id, ticker FROM symbol")
        data = cur.fetchall()
        return [(d[0], d[1]) for d in data]


def get_daily_historic_data_iex(ticker):
    """Obtains full historical data from IEX returns a list of tuples.
  ticker: Ticker symbol, e.g. "GOOG" for Google, Inc."""
    # time range
    start = datetime.datetime(2015, 1, 1)
    end = datetime.datetime(2018, 12, 31)

    # Construct the URL with the correct integer query parameters
    # av_json = urllib2.urlopen("https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=%s&outputsize=full&apikey=YD2DLTZBJFNPFVX2" % (ticker))

    # Try connecting to IEX and obtain the data
    # On failure, print an error message.
    try:
        iex_data = web.DataReader(ticker, 'iex', start, end)
        prices = []
        for row in iex_data.itertuples():
            prices.append((datetime.datetime.strptime(row.Index, '%Y-%m-%d'),
                          row.open,
                          row.high,
                          row.low,
                          row.close,
                          row.volume))

    except Exception, e:
        print("Could not download IEX data: %s" % e)
    return prices


def insert_daily_data_into_db(data_vendor_id, symbol_id, daily_data):
    """Takes a list of tuples of daily data and adds it to the
    MySQL database. Appends the vendor ID and symbol ID to the data.

    daily_data: List of tuples of the OHLC data (with
    adj_close and volume)"""

    # Create the time now
    now = datetime.datetime.utcnow()

    # Amend the data to include the vendor ID and symbol ID
    daily_data = [(data_vendor_id, symbol_id, d[0], now, now,
                   d[1], d[2], d[3], d[4], d[5]) for d in daily_data]

    # Create the insert strings
    column_str = """data_vendor_id, symbol_id, price_date, created_date,
          last_updated_date, open_price, high_price, low_price,
          close_price, volume"""
    insert_str = ("%s, " * 10)[:-2]
    final_str = "INSERT INTO daily_price (%s) VALUES (%s)" % (column_str, insert_str)

    # Using the MySQL connection, carry out an INSERT INTO for every symbol
    with con:
        cur = con.cursor()
        cur.executemany(final_str, daily_data)


if __name__ == "__main__":
    # Loop over the tickers and insert the daily historical
    # data into the database
    tickers = obtain_list_of_db_tickers()
    rate_limiter = RateLimiter(max_calls=1, period=30)
    for t in tickers:
        with rate_limiter:
            print("Adding data for %s" % t[1])
            iex_prices = get_daily_historic_data_iex(t[1])
            insert_daily_data_into_db('1', t[0], iex_prices)