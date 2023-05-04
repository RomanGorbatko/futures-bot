import csv
import threading
import time
from datetime import datetime, timedelta
import os.path
import sys

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
event_log = 'logs/{0}.csv'

OPEN_LONG = 'open_long'
OPEN_SHORT = 'open_short'
INCREASE_LONG = 'increase_long'
INCREASE_SHORT = 'increase_short'

DIRECTION_LONG = 'Long'
DIRECTION_SHORT = 'Short'

balance = starting_balance = float(os.getenv('BALANCE')) if os.getenv('BALANCE') else 500.
live = bool(os.getenv('LIVE')) if os.getenv('LIVE') is not None and os.getenv('LIVE') == 'True' else False
should_dump_to_csv = bool(os.getenv('DUMP_TO_CSV')) if os.getenv('DUMP_TO_CSV') is not None and os.getenv('DUMP_TO_CSV') == 'True' else False

low_risk_per_trade = .02
high_risk_per_trade = .1

taker_fee = .0004
maker_fee = .0002

paper_leverage = 100
stop_loss = .005  # 1%
take_profit = .015  # 1.5%
trailing_stop_loss = .01  # 0.5%
trailing_take_profit = .01  # 0.5%
max_trailing_takes = 2
touches = 0

ema1_length = 9
ema1_amplitude = 2

ema2_length = 20
ema2_amplitude = 2.5

ema3_length = 50
ema3_amplitude = 2.5

symbol_position = None
long_position = False
short_position = False
entry_price = 0
stop_loss_price = 0
take_profit_price = 0
asset_size = 0
position_size = 0
position_fee = 0
last_action = None
trades = 0
last_orders = []
trend = 0

wins = 0
loses = 0
trailing_loses = 0

last_stop_loss_order_id = 0
last_take_profit_order_id = 0

df = {}
symbols = [
    'APTUSDT', 'DYDXUSDT', 'ANKRUSDT',
    'OPUSDT', 'MATICUSDT', 'DOTUSDT',
    'APEUSDT', 'AVAXUSDT', '1000SHIBUSDT',
    'IMXUSDT', 'LINKUSDT', 'GALAUSDT',
    'BNBUSDT', 'INJUSDT', 'FILUSDT',
    'SOLUSDT', 'FLMUSDT', 'FTMUSDT',
    'ETCUSDT', 'TRXUSDT', 'LTCUSDT',
    'MANAUSDT', 'LDOUSDT', 'XRPUSDT',
    'ADAUSDT'

    # 'AAVEUSDT', 'NEARUSDT', 'ATOMUSDT', 'DOGEUSDT',
]
symbols_settings = {}
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
    if os.getenv('BINANCE_API_KEY') and os.getenv('BINANCE_API_SECRET'):
        return Client(os.getenv('BINANCE_API_KEY'), os.getenv('BINANCE_API_SECRET'))

    return Client()


def update_current_balance():
    global balance
    info = client.futures_account_balance()

    *_, usdt_balance = filter(lambda d: d['asset'] == 'USDT', info)

    balance = float(usdt_balance['balance'])


def update_leverage():
    global symbols_settings

    info = client.futures_leverage_bracket()

    for s in symbols:
        *_, leverage_info = filter(lambda d: d['symbol'] == s, info)
        leverage_info['brackets'].sort(key=lambda x: x['initialLeverage'], reverse=True)

        if s not in symbols_settings:
            symbols_settings[s] = {}

        # comment below line to disable
        # leverage_info['brackets'][0]['initialLeverage'] = 2

        symbols_settings[s]['leverage'] = leverage_info['brackets'][0]

        print(s, get_symbol_leverage(s))

        client.futures_change_leverage(
            symbol=s,
            # leverage=2
            leverage=symbols_settings[s]['leverage']['initialLeverage']
        )


def setup_symbols_settings():
    global symbols_settings

    futures_info = client.futures_exchange_info()

    for s in symbols:
        *_, symbol_info = filter(lambda d: d['symbol'] == s, futures_info['symbols'])

        if s not in symbols_settings:
            symbols_settings[s] = {}

        symbols_settings[s]['info'] = symbol_info


def setup_binance():
    update_current_balance()
    update_leverage()
    setup_symbols_settings()


def get_symbol_leverage(s):
    if not live:
        return paper_leverage

    return symbols_settings[s]['leverage']['initialLeverage']


def get_symbol_quantity_precision(s):
    return symbols_settings[s]['info']['quantityPrecision']


def get_symbol_price_precision(s):
    return symbols_settings[s]['info']['pricePrecision']


def get_percentage_difference(num_a, num_b):
    if isinstance(num_a, pd.Series):
        num_a = num_a.astype('float')

    if isinstance(num_b, pd.Series):
        num_b = num_b.astype('float')

    diff = num_a - num_b
    divided = diff / num_a

    return divided * 100


def calculate_avg_order_entry():
    return sum(item['position_size'] for item in last_orders) \
           / sum(item['asset_size'] for item in last_orders)


def calculate_taker_fee(size):
    return size - (size * (1 - taker_fee))


def calculate_maker_fee(size):
    return size - (size * (1 - maker_fee))


def calculate_entry_position_size(s, high_risk=False):
    global position_fee

    risk = high_risk_per_trade if high_risk else low_risk_per_trade

    # print(f"Risk: {risk * 100}%, Position: ${position_size:,.2f}")
    size = (balance * risk) * get_symbol_leverage(s)
    fee = calculate_taker_fee(size)

    position_fee += fee

    return size - fee


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


def fix_dataframe_index(s):
    df[s]["ema1"] = ema(df[s]["close"], length=ema1_length)
    df[s]["ema2"] = ema(df[s]["close"], length=ema2_length)
    df[s]["ema3"] = ema(df[s]["close"], length=ema3_length)

    # df["ema1_amplitude"] = get_percentage_difference(df["close"], df["ema1"])
    # df["ema2_amplitude"] = get_percentage_difference(df["close"], df["ema2"])
    # df["ema3_amplitude"] = get_percentage_difference(df["close"], df["ema3"])


def dump_to_csv(s, event_data, current_price):
    row = event_data.values.tolist()
    row.append(current_price)

    headers = [
        'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore', 'ema1', 'ema2', 'ema3',
        'current_price'
    ]

    if not os.path.isfile(event_log.format(s)):
        with open(event_log.format(s), 'w', newline='') as out_csv:
            writer = csv.DictWriter(out_csv, fieldnames=headers, delimiter=',', lineterminator='\n')
            writer.writeheader()
    else:
        with open(event_log.format(s), 'a', newline='') as out_csv:
            writer = csv.writer(out_csv, delimiter=',', lineterminator='\n')
            writer.writerow(row)


def close_position(s, pnl, direction):
    global long_position, short_position, trailing_loses, loses, wins, \
        touches, position_size, asset_size, balance, symbol_position, position_fee, \
        last_stop_loss_order_id, last_take_profit_order_id

    symbol_position = None
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

    fee = calculate_maker_fee(position_size)
    position_fee += fee

    touches = 0
    position_size = 0
    asset_size = 0
    last_stop_loss_order_id = 0
    last_take_profit_order_id = 0

    balance += (pnl - position_fee)


def manage_opened_position(s, current_price, direction):
    global stop_loss_price, take_profit_price, touches, long_position, trailing_loses, loses, position_size, \
        asset_size, balance, entry_price, wins, short_position, position_fee, last_stop_loss_order_id, \
        last_take_profit_order_id

    current_time = time.strftime('%Y-%m-%d %H:%M:%S')

    if direction is DIRECTION_LONG:
        exit_condition = current_price <= stop_loss_price
    else:
        exit_condition = current_price >= stop_loss_price

    exit_price = stop_loss_price if exit_condition else take_profit_price
    pnl = calculate_pnl(exit_price, direction is DIRECTION_SHORT)

    if pnl < 0:  # stop loss
        close_position(s, pnl, direction)
    else:  # take profits
        if touches <= max_trailing_takes:  # trailing
            last_action_increase = INCREASE_LONG if direction is DIRECTION_LONG else INCREASE_SHORT
            increase_position_size = calculate_entry_position_size(s, True)
            increase_asset_size = increase_position_size / current_price

            last_orders.append(
                {
                    'symbol': s,
                    'last_action': last_action_increase,
                    'entry_price': current_price,
                    'asset_size': increase_asset_size,
                    'position_size': increase_position_size,
                }
            )

            touches += 1
            entry_price = calculate_avg_order_entry()

            # print('entry_price calculate_avg_order_entry', entry_price)
            if live:
                client.futures_create_order(
                    symbol=s,
                    side=Client.SIDE_BUY if direction is DIRECTION_LONG else Client.SIDE_SELL,
                    type=Client.ORDER_TYPE_MARKET,
                    quantity=round(increase_asset_size, get_symbol_quantity_precision(s)),
                )
                position = client.futures_position_information(symbol=s)[0]
                entry_price = float(position['entryPrice'])

            position_size += increase_position_size
            asset_size += increase_asset_size

            # print('entry_price position', entry_price)

            if direction is DIRECTION_LONG:
                stop_loss_price = entry_price * (1 - trailing_stop_loss)
                take_profit_price = entry_price * (1 + trailing_take_profit)
            else:
                stop_loss_price = entry_price * (1 + trailing_stop_loss)
                take_profit_price = entry_price * (1 - trailing_take_profit)

            if live:
                client.futures_cancel_order(symbol=s, orderId=last_stop_loss_order_id)

                stop_order = client.futures_create_order(
                    symbol=s,
                    side=Client.SIDE_SELL if direction is DIRECTION_LONG else Client.SIDE_BUY,
                    type=Client.FUTURE_ORDER_TYPE_STOP_MARKET,
                    closePosition='true',
                    stopPrice=round(stop_loss_price, get_symbol_price_precision(s)),
                    workingType='MARK_PRICE'
                )

                last_stop_loss_order_id = stop_order['orderId']

                if (touches - 1) == 2:
                    stop_order = client.futures_create_order(
                        symbol=s,
                        side=Client.SIDE_BUY if direction is DIRECTION_LONG else Client.SIDE_SELL,
                        type=Client.FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
                        closePosition='true',
                        stopPrice=round(take_profit_price, get_symbol_price_precision(s)),
                        workingType='MARK_PRICE'
                    )

                    last_take_profit_order_id = stop_order['orderId']

            print_log({
                'Symbol': s,
                'Time': current_time,
                f'Increase {direction}': touches - 1,
                'Position Size': f'${position_size:,.4f}',
                'Asset Size': f'{asset_size:.4f}',
                'Entry Price': f'{entry_price:.5f}',
                'Stop Loss Price': f'{stop_loss_price:.5f}',
                'Take Profit Price': f'{take_profit_price:.5f}',
            })

            return
        else:  # absolute take profit
            close_position(s, pnl, direction)

    print_log({
        'Symbol': s,
        'Time': current_time,
        'Close': f'{direction} 🔵️',
        'Clear Pnl': f'${pnl:,.4f}',
        'Fee': f'${position_fee:,.4f}',
        'Entry Price': f'{entry_price:.5f}',
        'Exit Price': f'{exit_price:.5f}',
        'Balance': f'${balance:,.4f}',
    })

    position_fee = 0


def open_position(s, current_price, direction):
    global symbol_position, entry_price, position_size, asset_size, stop_loss_price, take_profit_price, long_position, \
        last_action, touches, trades, last_orders, short_position, last_stop_loss_order_id

    if direction is DIRECTION_LONG:
        long_position = True
    else:
        short_position = True

    current_time = time.strftime('%Y-%m-%d %H:%M:%S')

    symbol_position = s
    entry_price = current_price
    position_size = calculate_entry_position_size(s)
    asset_size = position_size / entry_price

    if live:
        client.futures_create_order(
            symbol=s,
            side=Client.SIDE_BUY if direction is DIRECTION_LONG else Client.SIDE_SELL,
            type=Client.ORDER_TYPE_MARKET,
            quantity=round(asset_size, get_symbol_quantity_precision(s)),
        )
        position = client.futures_position_information(symbol=s)[0]
        entry_price = float(position['entryPrice'])

    touches = 1
    trades += 1
    last_orders = []

    if direction is DIRECTION_LONG:
        stop_loss_price = entry_price * (1 - stop_loss)
        take_profit_price = entry_price * (1 + take_profit)
        last_action = OPEN_LONG
    else:
        stop_loss_price = entry_price * (1 + stop_loss)
        take_profit_price = entry_price * (1 - take_profit)
        last_action = OPEN_SHORT

    if live:
        stop_order = client.futures_create_order(
            symbol=s,
            side=Client.SIDE_SELL if direction is DIRECTION_LONG else Client.SIDE_BUY,
            type=Client.FUTURE_ORDER_TYPE_STOP_MARKET,
            closePosition='true',
            stopPrice=round(stop_loss_price, get_symbol_price_precision(s)),
            workingType='MARK_PRICE'
        )

        last_stop_loss_order_id = stop_order['orderId']

    last_orders.append(
        {
            'symbol': s,
            'last_action': last_action,
            'entry_price': entry_price,
            'asset_size': asset_size,
            'position_size': position_size,
        }
    )

    print_log({
        'Symbol': s,
        'Time': current_time,
        'Open': 'Long 🟢' if direction is DIRECTION_LONG else 'Short 🔴',
        'Position Size': f'${position_size:,.4f}',
        'Asset Size': f'{asset_size:.4f}',
        'Entry Price': f'{entry_price:.5f}',
        'Stop Loss Price': f'{stop_loss_price:.5f}',
        'Take Profit Price': f'{take_profit_price:.5f}',
        'Balance': f'${balance:,.4f}',
    })


def process_kline_event(s, df_data, event_data):
    global balance, trend, long_position, stop_loss_price, take_profit_price, touches, trailing_loses, loses, wins, \
        last_orders, entry_price, short_position, position_size, last_action, trades, asset_size, symbol_position

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
    is_amplitude_valid = abs(actual_amplitude) >= ema1_amplitude

    if should_dump_to_csv:
        dump_to_csv(s, df_data, current_price)

    # print(f"Symbol: {s}, Current price: {current_price}, Ema1: {df_data.ema1}, Ema2: {df_data.ema2}, Ema3: {df_data.ema3}, Actual "
    #       f"Amplitude: {abs(actual_amplitude)}, Is Amplitude Valid: {is_amplitude_valid}")

    # reason to long exit
    if long_position \
            and s == symbol_position \
            and (current_price <= stop_loss_price or current_price >= take_profit_price):
        manage_opened_position(s, current_price, DIRECTION_LONG)
        return

    if short_position \
            and s == symbol_position \
            and (current_price >= stop_loss_price or current_price <= take_profit_price):
        manage_opened_position(s, current_price, DIRECTION_SHORT)
        return

    if not short_position and not long_position \
            and touches == 0 \
            and current_price > df_data.ema1 \
            and current_price > df_data.ema2 \
            and current_price > df_data.ema3 \
            and actual_amplitude > 0 \
            and is_amplitude_valid:
        open_position(s, current_price, DIRECTION_SHORT)

    if not long_position and not short_position \
            and touches == 0 \
            and current_price < df_data.ema1 \
            and current_price < df_data.ema2 \
            and current_price < df_data.ema3 \
            and actual_amplitude < 0 \
            and is_amplitude_valid:
        open_position(s, current_price, DIRECTION_LONG)


def handle_socket_message(event):
    s = event['ps']

    event_df = pd.DataFrame([event['k']])
    event_df = event_df.set_axis([
        'kline_start_time', 'kline_close_time', 'interval', 'first_trade_id', 'last_trade_id', 'open', 'close',
        'high', 'low', 'volume', 'number_of_trades', 'is_closed', 'quote_asset_volume', 'taker_buy_volume',
        'taker_buy_quote_asset_volume', 'ignore'
    ], axis=1, copy=False)
    event_df.drop('interval', axis=1, inplace=True)
    event_df.astype(float)

    df_data = df[s].iloc[-1]
    event_data = event_df.iloc[0]

    process_kline_event(s, df_data, event_data)


def update_dataframe(skip_timer=False):
    global df, client

    if not skip_timer:
        threading.Timer(60, update_dataframe).start()

    previous_minute = (datetime.now() - timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
    current_minute = time.strftime('%Y-%m-%d %H:%M:%S')

    for s in symbols:
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
            print(f"Time: {current_minute}, Exception ❗ Type: BinanceAPIException, Message: f{bae.message}")

            client = create_client()


client = create_client()
if live:
    setup_binance()

print_log({
    'Balance': f'${balance:,.2f}',
    'Start Time': start_time,
    'Interval': interval,
    'Low Risk Per Trade': f"{low_risk_per_trade * 100}%",
    'High Risk Per Trade': f"{high_risk_per_trade * 100}%",
    'Stop Loss': f"{stop_loss * 100}%",
    'Take Profit': f"{take_profit * 100}%",
    'Trailing Stop Loss': f"{trailing_stop_loss * 100}%",
    'Trailing Take Profit': f"{trailing_take_profit * 100}%",
    'Max Trailing Take Profit': max_trailing_takes,
})

for symbol in symbols:
    df[symbol] = get_dataframe(symbol, interval, start_time, end_time)
    fix_dataframe_index(symbol)

while True:
    now = time.localtime().tm_sec
    time.sleep(60 - now)

    update_dataframe()

    break

twm = ThreadedWebsocketManager()
twm.start()

for symbol in symbols:
    twm.start_kline_futures_socket(callback=handle_socket_message, symbol=symbol)
twm.join()