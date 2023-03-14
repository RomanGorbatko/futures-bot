import threading
import time
from datetime import datetime, timedelta
import random
import os.path
import sys

import pandas as pd
from pandas_ta import ema, rsi, psar
from binance.client import Client
from binance import ThreadedWebsocketManager
from prettytable import PrettyTable

os.environ['TZ'] = 'ETC'
event_log = 'event.csv'

client = Client()

OPEN_LONG = 'open_long'
OPEN_SHORT = 'open_short'
INCREASE_LONG = 'increase_long'
INCREASE_SHORT = 'increase_short'

TREND_UP = 1
TREND_DOWN = -1
NO_TREND = 0

balance = starting_balance = 500
low_risk_per_trade = .05
high_risk_per_trade = .2

leverage = 20
stop_loss = .005  # 0.5%
take_profit = .01  # 3%
trailing_stop_loss = .005  # 0.5%
trailing_take_profit = .01  # 0.5%
max_trailing_take_profit = 2
touches = 0

ema1_length = 9
ema1_amplitude = 1.5

ema2_length = 20
ema2_amplitude = 2.5

ema3_length = 50
ema3_amplitude = 2.5

long_position = False
short_position = False
entry_price = 0
stop_loss_price = 0
take_profit_price = 0
asset_size = 0
position = 0
last_action = None
trades = 0
last_orders = []
trend = 0

wins = 0
loses = 0
trailing_loses = 0

symbol = 'APTUSDT'
interval = Client.KLINE_INTERVAL_1MINUTE
start_time = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d 00:00:00')  # Yesterday time
end_time = time.strftime('%Y-%m-%d %H:%M:%S')  # Current time


def get_percentage_difference(num_a, num_b):
    if isinstance(num_a, pd.Series):
        num_a = num_a.astype('float')

    if isinstance(num_b, pd.Series):
        num_b = num_b.astype('float')

    diff = num_a - num_b
    divided = diff / num_a

    return divided * 100


def calculate_medium_order_entry():
    return sum(item['entry_price'] for item in last_orders) / len(last_orders)


def calculate_entry_position_size():
    risk = low_risk_per_trade if touches == 0 else high_risk_per_trade
    position = ((balance * risk) * leverage) * (touches + 1)

    print(f"Risk: {low_risk_per_trade * 100}%, Position: {position}")
    return position


def calculate_pnl(price, reverse=False):
    rate = (price / entry_price) if reverse is False else (entry_price / price)

    return (rate * calculate_entry_position_size()) - calculate_entry_position_size()


def get_dataframe(s, i, st, et):
    local_df = pd.DataFrame(
        client.futures_historical_klines(symbol=s, interval=i, start_str=st, end_str=et),
        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume',
                 'trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
    )

    local_df = local_df.astype(float)
    local_df['timestamp'] = pd.to_datetime(local_df['timestamp'], unit='ms')
    local_df.set_index('timestamp', inplace=True)

    return local_df


def fix_dataframe_index():
    df["ema1"] = ema(df["close"], length=ema1_length)
    df["ema2"] = ema(df["close"], length=ema2_length)
    df["ema3"] = ema(df["close"], length=ema3_length)

    # df["ema1_amplitude"] = get_percentage_difference(df["close"], df["ema1"])
    # df["ema2_amplitude"] = get_percentage_difference(df["close"], df["ema2"])
    # df["ema3_amplitude"] = get_percentage_difference(df["close"], df["ema3"])


def dump_to_csv(event_data):
    # dump_header = event_df_columns
    # dump_header.remove('interval')

    # print(dump_header)
    # print(event_data.columns)
    # print(dump_header, list(event_data.columns))
    # if file does not exist write header
    if not os.path.isfile(event_log):
        event_data.to_csv(event_log, index=False, header=[
            'kline_start_time', 'kline_close_time', 'first_trade_id', 'last_trade_id', 'open', 'close',
            'high', 'low', 'volume', 'number_of_trades', 'is_closed', 'quote_asset_volume', 'taker_buy_volume',
            'taker_buy_quote_asset_volume', 'ignore'
        ])
    else:  # else it exists so append without writing the header
        event_data.to_csv(event_log, index=False, header=False, mode='a')


def process(df_data, event_data):
    global balance, trend, long_position, stop_loss_price, take_profit_price, touches, trailing_loses, loses, wins, \
        last_orders, entry_price, short_position, position, last_action, trades

    current_time = time.strftime('%Y-%m-%d %H:%M:%S')

    if pd.isna(df_data.ema1) or pd.isna(df_data.ema2) or pd.isna(df_data.ema3):
        return

    if balance <= 0:
        print(f"Time: {current_time}, LIQUIDATION! Balance: {balance:.5f}")
        sys.stdout.flush()
        twm.stop()
        return

    current_price = float(event_data.close)
    actual_amplitude = get_percentage_difference(current_price, df_data.ema2)

    # print(f"Current price: {current_price}, Ema1: {df_data.ema1}, Ema2: {df_data.ema2}, Ema3: {df_data.ema3}, Actual "
    #       f"Amplitude: {actual_amplitude}")

    # last_price = df_data.close
    # last_sma = df_data.ema
    #
    # if last_price > last_sma:
    #     trend = TREND_UP
    # elif last_price < last_sma:
    #     trend = TREND_DOWN
    # else:
    #     trend = NO_TREND

    # reason to long exit
    if long_position and (current_price <= stop_loss_price or current_price >= take_profit_price):
        exit_price = stop_loss_price if current_price <= stop_loss_price else take_profit_price
        pnl = calculate_pnl(exit_price)

        if pnl < 0:
            long_position = False

            if touches > 1:
                trailing_loses += 1
            else:
                loses += 1

            touches = 0

            balance += pnl
        else:
            if touches <= max_trailing_take_profit:
                last_orders.append(
                    {
                        'entry_price': current_price,
                        'last_action': INCREASE_LONG
                    }
                )

                touches += 1
                entry_price = calculate_medium_order_entry()
                stop_loss_price = entry_price * (1 - trailing_stop_loss)
                take_profit_price = entry_price * (1 + trailing_take_profit)

                print(f'Time: {current_time}, Increase: {touches - 1}, Entry Price: {entry_price:.5f}, '
                      f'Stop Loss Price: {stop_loss_price:.5f}, Take Profit Price: {take_profit_price:.5f}')
                sys.stdout.flush()

                return
            else:
                touches = 0
                long_position = False
                wins += 1
                balance += pnl

        print(f"Time: {current_time}, Close Long Position; Pnl: {pnl:.5f}, Entry Price: {entry_price:.5f}, "
              f"Exit Price: {exit_price:.5f}, Balance: {balance:.5f}")
        sys.stdout.flush()
        return

    if short_position and (current_price >= stop_loss_price or current_price <= take_profit_price):
        exit_price = stop_loss_price if current_price >= stop_loss_price else take_profit_price
        pnl = calculate_pnl(exit_price, True)

        if pnl < 0:
            short_position = False

            if touches > 1:
                trailing_loses += 1
            else:
                loses += 1

            touches = 0
            balance += pnl
        else:
            if touches <= max_trailing_take_profit:
                last_orders.append(
                    {
                        'entry_price': current_price,
                        'last_action': INCREASE_SHORT
                    }
                )

                touches += 1
                entry_price = calculate_medium_order_entry()

                stop_loss_price = entry_price * (1 + trailing_stop_loss)
                take_profit_price = entry_price * (1 - trailing_take_profit)

                print(f'Time: {current_time}, Increase: {touches - 1}, Entry Price: {entry_price:.5f}, '
                      f'Stop Loss Price: {stop_loss_price:.5f}, Take Profit Price: {take_profit_price:.5f}')
                sys.stdout.flush()

                return
            else:
                touches = 0
                short_position = False
                wins += 1
                balance += pnl

        print(f"Time: {current_time}, Close Short Position; Pnl: {pnl:.5f}, Entry Price: {entry_price:.5f}, "
              f"Exit Price: {exit_price:.5f}, Balance: {balance:.5f}")
        sys.stdout.flush()
        return

    if not short_position and touches == 0 \
            and current_price > df_data.ema1 \
            and current_price > df_data.ema2 \
            and current_price > df_data.ema3 \
            and actual_amplitude > 0 \
            and abs(actual_amplitude) >= ema1_amplitude:
        entry_price = current_price
        position = calculate_entry_position_size() / entry_price
        stop_loss_price = entry_price * (1 + stop_loss)
        take_profit_price = entry_price * (1 - take_profit)
        short_position = True
        last_action = OPEN_SHORT
        touches = 1
        trades += 1
        last_orders = []
        last_orders.append(
            {
                'entry_price': entry_price,
                'last_action': last_action
            }
        )
        print(
            f"Time: {current_time}, Open Short; Position Size: {position:.5f}, Entry Price: {entry_price:.5f}, "
            f"Stop Loss Price: {stop_loss_price:.5f}, Take Profit Price: {take_profit_price:.5f}, "
            f"Balance: {balance:.5f}")
        sys.stdout.flush()

    if not long_position and touches == 0 \
            and current_price < df_data.ema1 \
            and current_price < df_data.ema2 \
            and current_price < df_data.ema3 \
            and actual_amplitude < 0 \
            and abs(actual_amplitude) >= ema1_amplitude:
        entry_price = current_price
        position = calculate_entry_position_size() / entry_price
        stop_loss_price = entry_price * (1 - stop_loss)
        take_profit_price = entry_price * (1 + take_profit)
        long_position = True
        last_action = OPEN_LONG
        touches = 1
        trades += 1
        last_orders = []
        last_orders.append(
            {
                'entry_price': entry_price,
                'last_action': last_action
            }
        )
        print(
            f"Time: {current_time}, Open Long; Position Size: {position:.5f}, Entry Price: {entry_price:.5f}, "
            f"Stop Loss Price: {stop_loss_price:.5f}, Take Profit Price: {take_profit_price:.5f}"
            f", Balance: {balance:.5f}")
        sys.stdout.flush()


def handle_socket_message(event):
    event_df = pd.DataFrame([event['k']])
    event_df = event_df.set_axis([
        'kline_start_time', 'kline_close_time', 'interval', 'first_trade_id', 'last_trade_id', 'open', 'close',
        'high', 'low', 'volume', 'number_of_trades', 'is_closed', 'quote_asset_volume', 'taker_buy_volume',
        'taker_buy_quote_asset_volume', 'ignore'
    ], axis=1, copy=False)
    event_df.drop('interval', axis=1, inplace=True)
    event_df.astype(float)

    df_data = df.iloc[-1]
    event_data = event_df.iloc[0]

    # dump_to_csv(event_df)
    process(df_data, event_data)


def update_dataframe():
    global df

    threading.Timer(60, update_dataframe).start()

    previous_minute = (datetime.now() - timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
    current_minute = time.strftime('%Y-%m-%d %H:%M:%S')

    fresh_df = get_dataframe(symbol, interval, previous_minute, current_minute)

    index = fresh_df.first_valid_index()
    if index in df.index:
        df.drop(index, inplace=True)

    df = pd.concat([df, fresh_df])
    fix_dataframe_index()


t = PrettyTable(['Param', 'Value'])
t.add_row(['Start Time', start_time])
t.add_row(['End Time', end_time])
t.add_row(['Interval', interval])
t.add_row(['Low Risk Per Trade', f"{low_risk_per_trade * 100}%"])
t.add_row(['High Risk Per Trade', f"{high_risk_per_trade * 100}%"])
t.add_row(['Leverage', leverage])
t.add_row(['Stop Loss', f"{stop_loss * 100}%"])
t.add_row(['Take Profit', f"{take_profit * 100}%"])
t.add_row(['Trailing Stop Loss', f"{trailing_stop_loss * 100}%"])
t.add_row(['Trailing Take Profit', f"{trailing_take_profit * 100}%"])
t.add_row(['Max Trailing Take Profit', max_trailing_take_profit])
# t.add_row(['EMA Length', ema_length])
# t.add_row(['EMA Amplitude', ema_amplitude])
# t.add_row(['RSI Length', rsi_length])
# t.add_row(['RSI Long Reason', rsi_long_reason])
# t.add_row(['RSI Short Reason', rsi_short_reason])
print(t)
print(f"\n")
sys.stdout.flush()

df = get_dataframe(symbol, interval, start_time, end_time)
fix_dataframe_index()

while True:
    now = time.localtime().tm_sec
    time.sleep(60 - now)

    update_dataframe()

    break

twm = ThreadedWebsocketManager()
twm.start()

twm.start_kline_futures_socket(callback=handle_socket_message, symbol=symbol)
twm.join()
