# PyQuant News is where finance practitioners level up with Python for quant finance, algorithmic trading, and market data analysis. Looking to get started? Check out the fastest growing, top-selling course to get started with Python for quant finance. For educational purposes. Not investment advice. Use at your own risk.

# ## Library installation
# Install the third-party libraries we need to download market data and work with tabular results in a notebook or script.

pip install yfinance pandas

# We do not install sqlite3 or sys because they are part of the Python standard library, so they come with Python by default.

# ## Imports and setup

# We use sys (argv) to read command-line arguments, sqlite3 to create a local SQLite database file, pandas for DataFrame cleaning and SQL I/O, and yfinance to fetch daily OHLCV bars from Yahoo Finance.

from sys import argv
import sqlite3
import pandas as pd
import yfinance as yf

# Keeping imports explicit helps us separate the “data plumbing” layer (SQLite) from the “research” layer (pandas), which is the same separation we want in a reproducible workflow.

# ## Normalize downloaded bars into schema

# Download raw bars with yfinance, then normalize column names and add a symbol so every row is self-identifying in our database.

def get_stock_data(symbol, start, end):
    data = yf.download(symbol, start=start, end=end)
    data.reset_index(inplace=True)
    data.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        },
        inplace=True,
    )
    data["symbol"] = symbol
    return data

# This “schema normalization” is where beginners usually lose reproducibility, because the same dataset can end up with different column names across files and sessions. Adding a "symbol" column upfront also prevents the classic mistake of mixing tickers when you later append more data.

# ## Append market data into SQLite

# Write normalized DataFrames into a single SQLite table named "stock_data" so we can backfill history and then keep appending new rows.

def save_data_range(symbol, start, end, con):
    data = get_stock_data(symbol, start, end)
    data.to_sql(
        "stock_data",
        con,
        if_exists="append",
        index=False,
    )

def save_last_trading_session(symbol, con):
    today = pd.Timestamp.today()
    data = get_stock_data(symbol, today, today)
    data.to_sql(
        "stock_data",
        con,
        if_exists="append",
        index=False,
    )

# Using DataFrame.to_sql keeps ingestion “boring” and repeatable, which is exactly the professional habit we want for research. In a real pipeline we would also enforce uniqueness (for example on date+symbol) to make reruns idempotent, but even this simple append-based store removes a lot of CSV chaos.

# Treating the database connection as an argument ("con") makes it easy to reuse the same functions in scripts, notebooks, or scheduled jobs without rewriting anything.

# ## Run ingestion from the command line

# Provide a small CLI entry point so we can backfill ("bulk") or do a daily update ("last") without touching notebook state.

if __name__ == "__main__":
    con = sqlite3.connect("market_data.sqlite")

    if argv[1] == "bulk":
        symbol = argv[2]
        start = argv[3]
        end = argv[4]
        save_data_range(symbol, start, end, con)
        print(f"{symbol} saved between {start} and {end}")
    elif argv[1] == "last":
        symbol = argv[2]
        save_last_trading_session(symbol, con)
        print(f"{symbol} saved")
    else:
        print("Enter bulk or last")

# The key idea here is operational: once ingestion is a one-line command, we stop re-downloading and re-cleaning ad hoc files for every new strategy test. SQLite writes to a single "market_data.sqlite" file, which makes it easy to version, back up, and share across projects.

# ## Query stored data for research

# Load stored rows back into pandas with SQL so our analysis notebooks can start from a stable, auditable table instead of fresh downloads.

import sqlite3
import pandas as pd

con = sqlite3.connect("market_data.sqlite")

df_1 = pd.read_sql_query("SELECT * from stock_data where symbol='SPY'", con)

df_2 = pd.read_sql_query(
    "SELECT * from stock_data where symbol='SPY' and volume > 100000", con
)

# Pulling data with SQL is what makes the “price database” useful: we can filter by symbol and conditions (like volume) before we even compute indicators. This is the backbone of reproducible research, because the exact query that produced your backtest input is easy to save, rerun, and audit later.