import random
import os.path

import pandas as pd
from pandas_ta import ema, rsi, psar
from binance.client import Client
from prettytable import PrettyTable
import time

# set up Binance client
client = Client()

OPEN_LONG = 'open_long'
OPEN_SHORT = 'open_short'
INCREASE_LONG = 'increase_long'
INCREASE_SHORT = 'increase_short'

TREND_UP = 1
TREND_DOWN = -1
NO_TREND = 0

balance = starting_balance = 500  # загальна сума в USDT на фьючах
risk_per_trade = .2  # відсоток (5%) від balance доступний для трейду

leverage = 20
stop_loss = .0001  # 0.5%
take_profit = .03  # 3%
trailing_stop_loss = .01  # 0.5%
trailing_take_profit = .02  # 0.5%
max_trailing_take_profit = 3
touches = 0

ema_length = 300
ema_amplitude = 2.5

rsi_length = 13
rsi_long_reason = 70
rsi_short_reason = 30

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
# min_bal = 4000

# load historical data from Binance Futures
# symbol = 'LINKUSDT'
# symbol = 'DYDXUSDT'
# symbol = 'ETCUSDT'
# symbol = 'SOLUSDT'
# symbol = 'MASKUSDT'
symbol = 'APTUSDT'
# symbol = 'XRPUSDT'
# symbol = 'GRTUSDT'
# symbol = 'ETHUSDT'
# symbol = 'LDOUSDT'
# symbol = 'AGIXUSDT'
# symbol = 'BTCUSDT'
interval = Client.KLINE_INTERVAL_1MINUTE
start_time = '2023-01-01 00:00:00'
end_time = '2023-03-11 23:59:59'


def get_percentage_difference(num_a, num_b):
    diff = num_a.astype('float') - num_b.astype('float')
    divided = diff / num_a.astype('float')

    return divided * 100


def calculate_medium_order_entry():
    return sum(item['entry_price'] for item in last_orders) / len(last_orders)


def calculate_entry_position_size():
    return ((balance * risk_per_trade) * leverage) * (touches + 1)


def calculate_pnl(price, reverse=False):
    rate = (price / entry_price) if reverse is False else (entry_price / price)

    return (rate * calculate_entry_position_size()) - calculate_entry_position_size()


path = f"cache/{symbol}_{interval}_{start_time}_{end_time}.json"

if not os.path.exists(path):
    df = pd.DataFrame(
        client.futures_historical_klines(symbol=symbol, interval=interval, start_str=start_time, end_str=end_time),
        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume',
                 'trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])

    df.to_json(path, orient='records', lines=True)

    df = df.astype(float)
else:
    df = pd.read_json(path, lines=True)

df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
df.set_index('timestamp', inplace=True)

df["ema"] = ema(df["close"], length=ema_length)
df["rsi"] = rsi(df["close"], length=rsi_length)
# df["candle_amplitude"] = get_percentage_difference(df["high"], df["low"])
df["ema_amplitude"] = get_percentage_difference(df["close"], df["ema"])

for i in range(len(df)):
    if pd.isna(df["ema"][i]):
        continue

    if balance <= 0:
        print(f"Time: {df.index[i]}, LIQUIDATION! Balance: {balance:.5f}")
        break

    last_price = df['close'][i - 1]
    last_sma = df['ema'][i - 1]

    if last_price > last_sma:
        trend = TREND_UP
    elif last_price < last_sma:
        trend = TREND_DOWN
    else:
        trend = NO_TREND

    # reason to long exit
    if long_position and (df['low'][i] <= stop_loss_price or df['high'][i] >= take_profit_price):
        exit_price = stop_loss_price if df['low'][i] <= stop_loss_price else take_profit_price
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
                        'entry_price': df['open'][i],
                        'last_action': INCREASE_LONG
                    }
                )

                touches += 1
                entry_price = calculate_medium_order_entry()
                stop_loss_price = entry_price * (1 - trailing_stop_loss)
                take_profit_price = entry_price * (1 + trailing_take_profit)

                print(f'Time: {df.index[i]}, Increase: {touches - 1}, Entry Price: {entry_price:.5f}, '
                      f'Stop Loss Price: {stop_loss_price:.5f}, Take Profit Price: {take_profit_price:.5f}')

                continue
            else:
                touches = 0
                long_position = False
                wins += 1
                balance += pnl

        print(f"Time: {df.index[i]}, Close Long Position; Pnl: {pnl:.5f}, Entry Price: {entry_price:.5f}, "
              f"Exit Price: {exit_price:.5f}, Balance: {balance:.5f}")
        continue

    if short_position and (df['high'][i] >= stop_loss_price or df['low'][i] <= take_profit_price):
        exit_price = stop_loss_price if df['high'][i] >= stop_loss_price else take_profit_price
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
                        'entry_price': df['open'][i],
                        'last_action': INCREASE_SHORT
                    }
                )

                touches += 1
                entry_price = calculate_medium_order_entry()

                stop_loss_price = entry_price * (1 + trailing_stop_loss)
                take_profit_price = entry_price * (1 - trailing_take_profit)

                print(f'Time: {df.index[i]}, Increase: {touches - 1}, Entry Price: {entry_price:.5f}, '
                      f'Stop Loss Price: {stop_loss_price:.5f}, Take Profit Price: {take_profit_price:.5f}')

                continue
            else:
                touches = 0
                short_position = False
                wins += 1
                balance += pnl

        print(f"Time: {df.index[i]}, Close Short Position; Pnl: {pnl:.5f}, Entry Price: {entry_price:.5f}, "
              f"Exit Price: {exit_price:.5f}, Balance: {balance:.5f}")
        continue

    if not short_position and touches == 0 and trend == TREND_DOWN \
            and df['rsi'][i] < rsi_short_reason \
            and df['ema_amplitude'][i] < 0 \
            and abs(df['ema_amplitude'][i]) > ema_amplitude:
        entry_price = df['open'][i]
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
            f"Time: {df.index[i]}, Open Short; Position Size: {position:.5f}, Entry Price: {entry_price:.5f}, "
            f"Stop Loss Price: {stop_loss_price:.5f}, Take Profit Price: {take_profit_price:.5f}, Ema Amplitude: "
            f"{df['ema_amplitude'][i]:.5f}")

    if not long_position and touches == 0 and trend == TREND_UP \
            and df['rsi'][i] > rsi_long_reason \
            and df['ema_amplitude'][i] > 0 \
            and abs(df['ema_amplitude'][i]) > ema_amplitude:
        entry_price = df['open'][i]
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
            f"Time: {df.index[i]}, Open Long; Position Size: {position:.5f}, Entry Price: {entry_price:.5f}, "
            f"Stop Loss Price: {stop_loss_price:.5f}, Take Profit Price: {take_profit_price:.5f}, Ema Amplitude: "
            f"{df['ema_amplitude'][i]:.5f}")

print(f"\n")

t = PrettyTable(['Summary', 'Value'])
t.add_row(['Symbol', symbol])
t.add_row(['Starting Balance', f"${starting_balance:,.2f} USDT"])
t.add_row(['Final Balance', f"${balance:,.2f} USDT"])
t.add_row(['Trades', trades])
t.add_row(['Wins', wins])
t.add_row(['Trailing Loses', trailing_loses])
t.add_row(['Loses', loses])
print(t)

t = PrettyTable(['Param', 'Value'])
t.add_row(['Start Time', start_time])
t.add_row(['End Time', end_time])
t.add_row(['Interval', interval])
t.add_row(['Risk Per Trade', f"{risk_per_trade * 100}%"])
t.add_row(['Leverage', leverage])
t.add_row(['Stop Loss', f"{stop_loss * 100}%"])
t.add_row(['Take Profit', f"{take_profit * 100}%"])
t.add_row(['Trailing Stop Loss', f"{trailing_stop_loss * 100}%"])
t.add_row(['Trailing Take Profit', f"{trailing_take_profit * 100}%"])
t.add_row(['Max Trailing Take Profit', max_trailing_take_profit])
t.add_row(['EMA Length', ema_length])
t.add_row(['EMA Amplitude', ema_amplitude])
t.add_row(['RSI Length', rsi_length])
t.add_row(['RSI Long Reason', rsi_long_reason])
t.add_row(['RSI Short Reason', rsi_short_reason])
print(t)


