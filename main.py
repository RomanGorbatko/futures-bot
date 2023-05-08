import threading
import time
from datetime import datetime, timedelta
import os.path

import pandas as pd
from dotenv import load_dotenv
from pandas_ta import ema, rsi, psar
from binance.exceptions import BinanceAPIException
from binance.client import Client
from binance import ThreadedWebsocketManager
from requests import ReadTimeout

from src.account import Account
from src.setting import Setting
from src.strategy import Strategy

load_dotenv()
ENV = os.getenv("ENV") or "local"

os.environ["TZ"] = "UTC"
event_log = "logs/{0}.csv"

df = {}

balance = starting_balance = (
    float(os.getenv("BALANCE")) if os.getenv("BALANCE") else 500.0
)

interval = Client.KLINE_INTERVAL_1MINUTE
start_time = (datetime.now() - timedelta(1)).strftime(
    "%Y-%m-%d 00:00:00"
)  # Yesterday time
end_time = time.strftime("%Y-%m-%d %H:%M:%S")  # Current time

account = Account(balance)
setting = Setting()
strategy = Strategy(account, setting)


def get_dataframe(s, i, st, et):
    local_df = pd.DataFrame(
        strategy.client.futures_historical_klines(
            symbol=s, interval=i, start_str=st, end_str=et
        ),
        columns=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "trades",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
            "ignore",
        ],
    )

    local_df = local_df.astype(float)
    local_df["timestamp"] = pd.to_datetime(local_df["timestamp"], unit="ms")
    local_df.set_index("timestamp", inplace=True)

    return local_df


def fix_dataframe_index(s):
    df[s]["ema1"] = ema(df[s]["close"], length=strategy.setting.ema1_length)
    df[s]["ema2"] = ema(df[s]["close"], length=strategy.setting.ema2_length)
    df[s]["ema3"] = ema(df[s]["close"], length=strategy.setting.ema3_length)


def handle_socket_message(event):
    s = event["ps"]

    event_df = pd.DataFrame([event["k"]])
    event_df = event_df.set_axis(
        [
            "kline_start_time",
            "kline_close_time",
            "interval",
            "first_trade_id",
            "last_trade_id",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "number_of_trades",
            "is_closed",
            "quote_asset_volume",
            "taker_buy_volume",
            "taker_buy_quote_asset_volume",
            "ignore",
        ],
        axis=1,
        copy=False,
    )
    event_df.drop("interval", axis=1, inplace=True)
    event_df.astype(float)

    df_data = df[s].iloc[-1]
    event_data = event_df.iloc[0]

    strategy.process_kline_event(s, df_data, float(event_data.close))


def update_dataframe(skip_timer=False):
    if not skip_timer:
        threading.Timer(60, update_dataframe).start()

    previous_minute = (datetime.now() - timedelta(minutes=1)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    current_minute = time.strftime("%Y-%m-%d %H:%M:%S")

    for s in setting.symbols:
        try:
            fresh_df = get_dataframe(s, interval, previous_minute, current_minute)

            index = fresh_df.first_valid_index()
            if index in df[s].index:
                df[s].drop(index, inplace=True)

            df[s] = pd.concat([df[s], fresh_df])
            fix_dataframe_index(s)
        except ReadTimeout as re:
            print(f"Time: {current_minute}, Exception ❗ Type: ReadTimeout")
        except BinanceAPIException as bae:
            print(
                f"Time: {current_minute}, Exception ❗ Type: BinanceAPIException, Message: f{bae.message}"
            )

            strategy.create_client()


strategy.utils.print_log(
    {
        "Balance": f"${balance:,.2f}",
        "Start Time": start_time,
        "Interval": interval,
        "Low Risk Per Trade": f"{setting.low_risk_per_trade * 100}%",
        "High Risk Per Trade": f"{setting.high_risk_per_trade * 100}%",
        "Stop Loss": f"{setting.stop_loss * 100}%",
        "Take Profit": f"{setting.take_profit * 100}%",
        "Trailing Stop Loss": f"{setting.trailing_stop_loss * 100}%",
        "Trailing Take Profit": f"{setting.trailing_take_profit * 100}%",
        "Max Trailing Take Profit": setting.max_trailing_takes,
    }
)

for symbol in setting.symbols:
    df[symbol] = get_dataframe(symbol, interval, start_time, end_time)
    fix_dataframe_index(symbol)

while True:
    now = time.localtime().tm_sec
    time.sleep(60 - now)

    update_dataframe()

    break

twm = ThreadedWebsocketManager()
twm.start()

for symbol in setting.symbols:
    twm.start_kline_futures_socket(callback=handle_socket_message, symbol=symbol)
twm.join()
