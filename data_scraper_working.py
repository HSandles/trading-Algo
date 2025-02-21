import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime
import os

from sqlalchemy import Column, Integer, Float, String, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


# Initialize SQLAlchemy Base
Base = declarative_base()

# Create an SQLite database connection
DATABASE_URL = "postgresql://postgres:xxx@localhost:5432/FX"
engine = create_engine(DATABASE_URL)  # Replace with your desired database URI

SessionLocal = sessionmaker(bind=engine)



commods = []

crypto = []

stocks = [
    
]

fx = [
    "AUDCAD.r",
    "AUDCHF.r",
    "AUDJPY.r",
    "AUDNZD.r",
    "AUDUSD",
    "CADCHF.r",
    "CADJPY.r",
    "EURAUD.r",
    "EURCAD.r",
    "EURCHF.r",
    "EURGBP.r",
    "EURJPY.r",
    "EURNZD.r",
    "EURUSD",
    "GBPAUD.r",
    "GBPCAD.r",
    "GBPCHF.r",
    "GBPJPY.r",
    "GBPNZD.r",
    "GBPUSD",
    "NZDCAD.r",
    "NZDCHF.r",
    "NZDJPY.r",
    "NZDUSD",
    "USDCAD",
    "USDCHF",
]


##################################################### MT5 SET UP AND CONNECTION ##################################################################


# Initialize the MT5 connection
def initialize_mt5():
    if not mt5.initialize():
        print("Failed to initialize MT5. Error code:", mt5.last_error())
        return False
    print("MT5 initialized successfully!")
    return True


# Login to MT5 account (optional, if needed)
def login_mt5(account, password, server):
    if not mt5.login(account, password=password, server=server):
        print("Failed to login to MT5 account. Error code:", mt5.last_error())
        return False
    print("Logged in to MT5 account successfully!")
    return True


# Fetch account information
def get_account_info():
    account_info = mt5.account_info()
    if account_info is None:
        print("Failed to fetch account info. Error code:", mt5.last_error())
    else:
        print("Account Info:")
        print(account_info)
    return account_info


########################################################### CANDLE CLASS ###########################################################################

# THIS NEEDS TO BE DEFINED AS AN SQL ALCHEMY MODEL TO ALLOW INTEGRATION INTO DATABASE
# TO DO THIS WE NEED TO inherit from Base (provided by SQLAlchemy's declarative base) and map its attributes to database columns.


class Candle(Base):
    __tablename__ = "1WEEK_candles"  # Table name in the database

    id = Column(
        Integer, primary_key=True, autoincrement=True
    )  # Auto-increment primary key
    time = Column(DateTime, nullable=False)
    symbol = Column(String, nullable=False)
    open_price = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    tick_volume = Column(Integer, nullable=False)
    spread = Column(Integer, nullable=True)
    real_volume = Column(Integer, nullable=True)
    tag = Column(String, nullable=False)

    # bullish and bearish candles - not really relevant right now

    def is_bullish(self):
        return self.close > self.open_price

    def is_bearish(self):
        return self.close < self.open_price

    # This is here to provide a string representation of a candle object for debugging etc

    def __repr__(self):
        return (
            f"Candle(time={self.time}, symbol={self.symbol}, open={self.open_price}, "
            f"high={self.high}, low={self.low}, close={self.close}, "
            f"tick_volume={self.tick_volume}, spread={self.spread}, real_volume={self.real_volume})"
        )


# Create the {TABLE_NAME} table if it does not exist
Base.metadata.create_all(engine)
print("Database tables created successfully (if not already existing).")


################################################################ FUCNTIONS ##############################################################################


# Fetch historical data
def get_historical_candles(symbol, timeframe, start_date, end_date):
    utc_from = datetime.strptime(start_date, "%Y-%m-%d")
    utc_to = datetime.strptime(end_date, "%Y-%m-%d")
    rates = mt5.copy_rates_range(symbol, timeframe, utc_from, utc_to)

    if rates is None:
        print(
            f"Failed to fetch historical data for {symbol}. Error code:",
            mt5.last_error(),
        )
        return None, None

    candles, df = process_rates(rates, symbol)
    return candles, df


# Convert rates to Candle objects and a DataFrame
def process_rates(rates, symbol):
    candles = []
    data_rows = []

    for rate in rates:
        # Convert UNIX timestamp to datetime
        time = datetime.utcfromtimestamp(int(rate["time"]))  # Ensure Python int

        tag = ""

        if symbol in stocks:
            tag = "stocks"
        elif symbol in fx:
            tag = "fx"

        # Create a Candle object
        candle = Candle(
            time=time,
            symbol=symbol,
            tag=tag,
            open_price=float(rate["open"]),  # Convert NumPy float → Python float
            high=float(rate["high"]),
            low=float(rate["low"]),
            close=float(rate["close"]),
            tick_volume=int(rate["tick_volume"]),  # Convert np.uint64 → int
            spread=int(rate["spread"]),  # Convert np.int32 → int
            real_volume=int(rate["real_volume"]),  # Convert np.uint64 → int
        )

        candles.append(candle)

        # Add data for DataFrame
        data_rows.append(
            {
                "time": time,
                "symbol": symbol,
                "open": float(rate["open"]),  # Ensure Python float
                "high": float(rate["high"]),
                "low": float(rate["low"]),
                "close": float(rate["close"]),
                "tick_volume": int(rate["tick_volume"]),  # Ensure Python int
                "spread": int(rate["spread"]),
                "real_volume": int(rate["real_volume"]),
            }
        )

    # Convert to pandas DataFrame
    df = pd.DataFrame(data_rows)
    return candles, df


# Insert Candle objects into the database
def save_candles_to_db(candles):
    session = SessionLocal()

    session.add_all(candles)  # Add converted data to the session
    session.commit()
    session.close()
    print(f"Inserted {len(candles)} candles into the database.")


# Save DataFrame to CSV
def save_data_to_csv(data, output_dir, start_date, end_date):
    os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists
    file_path = os.path.join(output_dir, f"fx_weekly{start_date}_{end_date}.csv")
    data.to_csv(file_path, index=False)
    print(f"Data saved to {file_path}")


# commented out 28/1/2025 - when adding candles class to change data types
# Fetch historical data
# def get_historical_data(symbol, timeframe, start_date, end_date):
#    utc_from = datetime.strptime(start_date, "%Y-%m-%d")
#    utc_to = datetime.strptime(end_date, "%Y-%m-%d")
#    rates = mt5.copy_rates_range(symbol, timeframe, utc_from, utc_to)

#    if rates is None:
#        print(
#            f"Failed to fetch historical data for {symbol}. Error code:",
#            mt5.last_error(),
#        )
#        return None

# commented out 28/1/2025 - when adding candles class to change data types
#    # Convert to pandas DataFrame
#    rates_df = pd.DataFrame(rates)
#    rates_df["time"] = pd.to_datetime(
#        rates_df["time"], unit="s"
#    )  # Convert UNIX timestamp to datetime
#    rates_df["symbol"] = symbol
#    return rates_df


# Save data to CSV - OLD - simon changes
# def save_data_to_csv(data, symbol, output_dir, start_date, end_date):
#     os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists
#     file_path = os.path.join(output_dir, f"{symbol}_H1_{start_date}_{end_date}.csv")
#     data.to_csv(file_path, index=False)
#     print(f"Data for {symbol} saved to {file_path}")


# # Main function
# def main():
#     try:
#         # Initialize MT5
#         if not initialize_mt5():
#             return

#         # Login (if required)
#         account = "7079316"
#         password = "MeepMoop1!"
#         server = "FPMarketsLLC-Demo"
#         login_mt5(account, password, server)

# # List of symbols to scrape
# symbols = (
#     "ACTIVISIONBLIZ",
#     "ADOBE",
#     "AMAZON",
#     "AMD",
#     "AMERICANAIRLINES",
#     "APPLE",
#     "ATLASSIAN",
#     "BAIDU",
#     "BOOKING",
#     "CISCO",
#     "COMCAST"

# )  # Add your desired symbols
# timeframe = mt5.TIMEFRAME_H1  # 1-hour timeframe
# start_date = "2024-01-01"
# end_date = "2025-01-01"
# output_dir = r"C:\Users\Dom\Documents\My_Snek_Stuff\trading_data"

# data = None

# # Fetch and save data for each symbol
# for symbol in symbols:
#     print(f"Fetching data for {symbol}...")
#     symbol_data = get_historical_data(symbol, timeframe, start_date, end_date)

#     if symbol_data is not None:
#         if data is not None:
#             data = pd.concat([data, symbol_data], ignore_index=True)
#         else:
#             data = symbol_data
#         # Add to data
#     # if data is not None:
#     #     save_data_to_csv(data, symbol, output_dir, start_date, end_date)
#     else:
#         print(f"Skipping {symbol} due to data fetch failure.")

# save_data_to_csv(data, output_dir, start_date, end_date)


# Main function 2
def main():

    # DATABASE CONNECTION CHECK - GOES HERE

    try:
        engine.connect()
        print("Database connection successful.")

    except Exception as e:
        print(f"Database connection failed: {e}")

        return  # Exit if the database is not reachable

    try:
        # Initialize MT5
        if not initialize_mt5():
            return

        # Login (if required)
        account = 7079316
        password = "MeepMoop1!"
        server = "FPMarketsLLC-Demo"
        login_mt5(account, password, server)

        # List of symbols to scrape
        symbols = stocks + fx

        timeframe = mt5.TIMEFRAME_W1  # ENTER YOUR CHOSEN TIMEFRAME - MUST BE IN MT5 FORMAT
        start_date = "2022-01-01"
        end_date = "2025-01-01"
        output_dir = r"C:\Users\Dom\Documents\My_Snek_Stuff\trading_data" # CHOOSE YOUR OUTPUT

        all_data = []
        all_candles = []

        # Fetch data and save to database
        for symbol in symbols:
            print(f"Fetching data for {symbol}...")
            candles, df = get_historical_candles(
                symbol, timeframe, start_date, end_date
            )

            if df is not None:
                all_candles.extend(candles)
                all_data.append(df)
            else:
                print(f"Skipping {symbol} due to data fetch failure.")

        if all_candles:
            save_candles_to_db(all_candles)

        # Combine all data into one DataFrame
        if all_data:
            final_data = pd.concat(all_data, ignore_index=True)
            save_data_to_csv(final_data, output_dir, start_date, end_date)
        else:
            print("No data fetched for any symbols.")

    finally:
        # Shutdown MT5 connection
        mt5.shutdown()
        print("MT5 connection closed.")


if __name__ == "__main__":
    main()



