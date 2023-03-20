import threading
import time
from datetime import datetime, timedelta
import os.path
import sys
from pprint import pprint

import pandas as pd
import requests
from dotenv import load_dotenv
from pandas_ta import ema, rsi, psar
from binance.exceptions import BinanceAPIException
from binance.client import Client
from binance import ThreadedWebsocketManager
from prettytable import PrettyTable
from requests import ReadTimeout

load_dotenv()
ENV = os.getenv('ENV') or 'local'

os.environ['TZ'] = 'UTC'
event_log = 'event.csv'

OPEN_LONG = 'open_long'
OPEN_SHORT = 'open_short'
INCREASE_LONG = 'increase_long'
INCREASE_SHORT = 'increase_short'

DIRECTION_LONG = 'Long'
DIRECTION_SHORT = 'Short'

balance = starting_balance = 500
low_risk_per_trade = .02
high_risk_per_trade = .1

leverage = 20
stop_loss = .01  # 0.5%
take_profit = .02  # 3%
trailing_stop_loss = .005  # 0.5%
trailing_take_profit = .01  # 0.5%
max_trailing_takes = 2
touches = 0

ema1_length = 9
ema_amplitude_minimum = 2
ema_amplitude_peak = 0

ema2_length = 20
ema3_length = 50

long_position = False
short_position = False
entry_price = 0
stop_loss_price = 0
take_profit_price = 0
asset_size = 0
position_size = 0
last_action = None
trades = 0
last_orders = []
trend = 0

wins = 0
loses = 0
trailing_loses = 0

twm = ThreadedWebsocketManager()
socket_name = None
symbol = 'APTUSDT'
interval = Client.KLINE_INTERVAL_1MINUTE
start_time = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d 00:00:00')  # Yesterday time
end_time = time.strftime('%Y-%m-%d %H:%M:%S')  # Current time


def print_log(data, pt=None):
    telegram_text = 'Env: ' + ENV + '\n'

    if pt is None:
        pt = PrettyTable(['Param', 'Value'])
        for key, value in data.items():
            pt.add_row([key, value])
            telegram_text += str(key) + ': ' + str(value) + '\n'
        pt.add_row(['Env', ENV])

    print(pt)
    sys.stdout.flush()

    if os.getenv('TELEGRAM_CHAT_ID') and os.getenv('TELEGRAM_BOT_ID'):
        send_text = 'https://api.telegram.org/bot' + os.getenv('TELEGRAM_BOT_ID') + '/sendMessage?chat_id=' \
                    + str(os.getenv('TELEGRAM_CHAT_ID')) + '&parse_mode=html&text=' + telegram_text

        response = requests.get(send_text)


def create_client():
    return Client()


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


def calculate_entry_position_size(high_risk=False):
    global position_size

    risk = high_risk_per_trade if high_risk else low_risk_per_trade
    position_size = ((balance * risk) * leverage)

    # print(f"Risk: {risk * 100}%, Position: ${position_size:,.2f}")
    return position_size


def calculate_pnl(price, reverse=False):
    rate = (entry_price / price) if reverse else (price / entry_price)

    return (rate * position_size) - position_size


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


def close_position(pnl, direction):
    global long_position, short_position, trailing_loses, loses, wins, \
        touches, position_size, asset_size, balance

    if direction is DIRECTION_LONG:
        long_position = False
    else:
        short_position = False

    if pnl < 0:
        if touches > 1:
            trailing_loses += 1
        else:
            loses += 1
    else:
        wins += 1

    touches = 0
    position_size = 0
    asset_size = 0

    balance += pnl


def manage_opened_position(current_price, direction):
    global stop_loss_price, take_profit_price, touches, long_position, trailing_loses, loses, position_size, \
        asset_size, balance, entry_price, wins, short_position

    current_time = time.strftime('%Y-%m-%d %H:%M:%S')

    if direction is DIRECTION_LONG:
        exit_condition = current_price <= stop_loss_price
    else:
        exit_condition = current_price >= stop_loss_price

    exit_price = stop_loss_price if exit_condition else take_profit_price
    pnl = calculate_pnl(exit_price, direction is DIRECTION_SHORT)

    if pnl < 0:  # stop loss
        close_position(pnl, direction)
    else:  # take profits
        if touches <= max_trailing_takes:  # trailing
            last_action_increase = INCREASE_LONG if direction is DIRECTION_LONG else INCREASE_SHORT
            last_orders.append(
                {
                    'entry_price': current_price,
                    'last_action': last_action_increase
                }
            )

            touches += 1
            entry_price = calculate_medium_order_entry()
            position_size += calculate_entry_position_size(True)
            asset_size = position_size / entry_price

            if direction is DIRECTION_LONG:
                stop_loss_price = entry_price * (1 - trailing_stop_loss)
                take_profit_price = entry_price * (1 + trailing_take_profit)
            else:
                stop_loss_price = entry_price * (1 + trailing_stop_loss)
                take_profit_price = entry_price * (1 - trailing_take_profit)

            print_log({
                'Time': current_time,
                'Symbol': symbol,
                f'Increase {direction}': touches - 1,
                'Position Size': f'{position_size:,.2f}',
                'Asset Size': f'{asset_size:.3f}',
                'Entry Price': f'{entry_price:.5f}',
                'Stop Loss Price': f'{stop_loss_price:.5f}',
                'Take Profit Price': f'{take_profit_price:.5f}',
            })

            return
        else:  # absolute take profit
            close_position(pnl, direction)

    print_log({
        'Time': current_time,
        'Symbol': symbol,
        'Close': f'{direction} 🔵️',
        'Pnl': f'${pnl:,.2f}',
        'Entry Price': f'{entry_price:.5f}',
        'Exit Price': f'{exit_price:.5f}',
        'Balance': f'${balance:,.2f}',
    })


def process_kline_event(df_data, event_data):
    global balance, trend, long_position, stop_loss_price, take_profit_price, touches, trailing_loses, loses, wins, \
        last_orders, entry_price, short_position, position_size, last_action, trades, asset_size, ema_amplitude_peak

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

    print(f"Symbol: {symbol}, Current price: {current_price}, Ema1: {df_data.ema1}, Ema2: {df_data.ema2}, Ema3: {df_data.ema3}, Actual "
          f"Amplitude: {actual_amplitude}")

    # reason to long exit
    if long_position and (current_price <= stop_loss_price or current_price >= take_profit_price):
        manage_opened_position(current_price, DIRECTION_LONG)
        return

    if short_position and (current_price >= stop_loss_price or current_price <= take_profit_price):
        manage_opened_position(current_price, DIRECTION_SHORT)
        return

    if not short_position and touches == 0 \
            and current_price > df_data.ema1 \
            and current_price > df_data.ema2 \
            and current_price > df_data.ema3 \
            and actual_amplitude > 0 \
            and abs(actual_amplitude) >= ema_amplitude_minimum:
        entry_price = current_price
        position_size = calculate_entry_position_size()
        asset_size = position_size / entry_price
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

        print_log({
            'Time': current_time,
            'Symbol': symbol,
            'Open': 'Short 🔴',
            'Position Size': f'${position_size:,.2f}',
            'Asset Size': f'{asset_size:.5f}',
            'Entry Price': f'{entry_price:.5f}',
            'Stop Loss Price': f'{stop_loss_price:.5f}',
            'Take Profit Price': f'{take_profit_price:.5f}',
            'Balance': f'${balance:,.2f}',
        })

    # if not long_position and touches == 0 \
    #         and current_price < df_data.ema1 \
    #         and current_price < df_data.ema2 \
    #         and current_price < df_data.ema3:
    #     if abs(actual_amplitude) > ema_amplitude_peak:
    #         ema_amplitude_peak = abs(actual_amplitude)
    #
    #     print(abs(actual_amplitude), ema_amplitude_peak)

    if not long_position and touches == 0 \
            and current_price < df_data.ema1 \
            and current_price < df_data.ema2 \
            and current_price < df_data.ema3 \
            and actual_amplitude < 0 \
            and abs(actual_amplitude) >= ema_amplitude_minimum:
        entry_price = current_price
        position_size = calculate_entry_position_size()
        asset_size = position_size / entry_price
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

        print_log({
            'Time': current_time,
            'Symbol': symbol,
            'Open': 'Long 🟢',
            'Position Size': f'${position_size:,.2f}',
            'Asset Size': f'{asset_size:.5f}',
            'Entry Price': f'{entry_price:.5f}',
            'Stop Loss Price': f'{stop_loss_price:.5f}',
            'Take Profit Price': f'{take_profit_price:.5f}',
            'Balance': f'${balance:,.2f}',
        })
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
    process_kline_event(df_data, event_data)


def update_dataframe(skip_timer=False):
    global df, client

    if not skip_timer:
        threading.Timer(60, update_dataframe).start()

    previous_minute = (datetime.now() - timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
    current_minute = time.strftime('%Y-%m-%d %H:%M:%S')

    if symbol is None:
        return

    try:
        fresh_df = get_dataframe(symbol, interval, previous_minute, current_minute)

        index = fresh_df.first_valid_index()
        if index in df.index:
            df.drop(index, inplace=True)

        df = pd.concat([df, fresh_df])
        fix_dataframe_index()
    except ReadTimeout as re:
        print(f"Time: {current_minute}, Exception ❗ Type: ReadTimeout")
    except BinanceAPIException as bae:
        print(f"Time: {current_minute}, Exception ❗ Type: BinanceAPIException, Message: f{bae.message}")

        client = create_client()

        if datetime.now().minute == 0:
            print(f"Time: {current_minute}, Ping")
            sys.stdout.flush()


def switch_symbol(should_start_streaming=False, min_volume=300000000):
    global symbol, twm

    threading.Timer(120, switch_symbol, [True]).start()

    if long_position is True or short_position is True:
        return

    if twm.is_alive():
        print('stopped1')
        twm.stop_socket(socket_name)
        twm.stop()
        twm.stop_client()
        # twm = ThreadedWebsocketManager()

    ticker_details = client.futures_ticker()

    perpetual_details = [x for x in ticker_details if float(x['volume']) > min_volume]
    perpetual_details = sorted(perpetual_details, key=lambda item: float(item['priceChangePercent']), reverse=True)

    first_ticker = perpetual_details[0]
    last_ticker = perpetual_details[-1]

    if abs(float(first_ticker['priceChangePercent'])) > abs(float(last_ticker['priceChangePercent'])):
        winner = first_ticker
    else:
        winner = last_ticker

    symbol = winner['symbol']

    if should_start_streaming:
        print_log({
            'Switched Symbol': symbol,
            'Changed': f"{float(winner['priceChangePercent'])}%",
            'Volume': f"${float(winner['volume']):,.2f}"
        })

        start_streaming(symbol)


def start_streaming(s):
    global twm, socket_name

    if twm.is_alive():
        print('stopped2')
        twm.stop()
        twm = ThreadedWebsocketManager()

    twm.start()
    socket_name = twm.start_kline_futures_socket(callback=handle_socket_message, symbol=s)
    twm.join()


client = create_client()

# switch_symbol()

print_log({
    'Symbol': symbol,
    'Start Time': start_time,
    'End Time': end_time,
    'Interval': interval,
    'Low Risk Per Trade': f"{low_risk_per_trade * 100}%",
    'High Risk Per Trade': f"{high_risk_per_trade * 100}%",
    'Leverage': leverage,
    'Stop Loss': f"{stop_loss * 100}%",
    'Take Profit': f"{take_profit * 100}%",
    'Trailing Stop Loss': f"{trailing_stop_loss * 100}%",
    'Trailing Take Profit': f"{trailing_take_profit * 100}%",
    'Max Trailing Take Profit': max_trailing_takes,
    'Ema Amplitude Minimum': ema_amplitude_minimum
})

df = get_dataframe(symbol, interval, start_time, end_time)
fix_dataframe_index()

while True:
    now = time.localtime().tm_sec
    time.sleep(60 - now)

    update_dataframe()
    start_streaming(symbol)

    break
