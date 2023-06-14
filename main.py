import threading
import time
from datetime import datetime, timedelta
import os.path

import pandas as pd
from dotenv import load_dotenv
from pandas_ta import ema, rsi, psar, atr, wma
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
    df[s]["ema5"] = ema(df[s]["close"], length=5)
    df[s]["ema9"] = ema(df[s]["close"], length=9)
    df[s]["ema10"] = ema(df[s]["close"], length=10)
    df[s]["ema15"] = ema(df[s]["close"], length=15)
    df[s]["ema20"] = ema(df[s]["close"], length=20)
    df[s]["ema25"] = ema(df[s]["close"], length=25)
    df[s]["ema30"] = ema(df[s]["close"], length=30)
    df[s]["ema35"] = ema(df[s]["close"], length=35)
    df[s]["ema40"] = ema(df[s]["close"], length=40)
    df[s]["ema45"] = ema(df[s]["close"], length=45)
    df[s]["ema50"] = ema(df[s]["close"], length=50)
    df[s]["ema55"] = ema(df[s]["close"], length=55)
    df[s]["ema60"] = ema(df[s]["close"], length=60)
    df[s]["ema65"] = ema(df[s]["close"], length=65)
    df[s]["ema70"] = ema(df[s]["close"], length=70)
    df[s]["ema75"] = ema(df[s]["close"], length=75)
    df[s]["ema80"] = ema(df[s]["close"], length=80)
    df[s]["ema85"] = ema(df[s]["close"], length=85)
    df[s]["ema90"] = ema(df[s]["close"], length=90)

    df[s]["wma14"] = wma(df[s]["close"], length=14)

    df[s]["atr14"] = atr(df[s]["high"], df[s]["low"], df[s]["close"])
    df[s]["rsi14"] = rsi(df[s]["close"])


def handle_socket_message(event):
    if "ps" not in event:
        return

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

    try:
        max_len = 100

        if len(df[s]) > max_len:
            df[s].drop(index=df[s].index[:len(df[s]) - max_len], axis=0, inplace=True)

        strategy.process_kline_event(s, df_data, float(event_data.close))
    except Exception as e:
        strategy.utils.print_log(
            {
                "Symbol": s,
                "Exception": " ❗",
                "Reason": "At process_kline_event",
                "Message": str(e),
            }
        )


def update_symbol_amplitude():
    threading.Timer(300, update_symbol_amplitude).start()

    setting.update_symbol_settings_from_db()


def update_dataframe(skip_timer=False):
    if not skip_timer:
        threading.Timer(60, update_dataframe).start()

    previous_minute = (datetime.now() - timedelta(minutes=1)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    current_minute = time.strftime("%Y-%m-%d %H:%M:%S")

    for s in setting.get_symbols_with_shitcoins():
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
        "Balance": f"${strategy.account.balance:,.2f}",
        "Start Time": start_time,
        "Indicator": setting.indicator,
        "Ema Amplitude": setting.ema_amplitude,
        "Low Risk Per Trade": f"{setting.low_risk_per_trade * 100}%",
        "High Risk Per Trade": f"{setting.high_risk_per_trade * 100}%",
        "Stop Loss": f"{setting.stop_loss * 100}%",
        "Take Profit": f"{setting.take_profit * 100}%",
        "Trailing Stop Loss": f"{setting.trailing_stop_loss * 100}%",
        "Trailing Take Profit": f"{setting.trailing_take_profit * 100}%",
        "Max Trailing Take Profit": setting.max_trailing_takes,
    }
)

for symbol in setting.get_symbols_with_shitcoins():
    df[symbol] = get_dataframe(symbol, interval, start_time, end_time)
    fix_dataframe_index(symbol)

while True:
    update_symbol_amplitude()

    time.sleep(60 - time.localtime().tm_sec)

    update_dataframe()

    break

twm = ThreadedWebsocketManager()
twm.start()

for symbol in setting.get_symbols_with_shitcoins():
    twm.start_kline_futures_socket(callback=handle_socket_message, symbol=symbol)
twm.join()
