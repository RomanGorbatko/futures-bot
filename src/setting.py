import os
import time

import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract


class Setting:
    is_back_test: bool = False
    is_hyperopt: bool = False

    low_risk_per_trade = 0.02
    high_risk_per_trade = 0.1

    taker_fee = 0.0004
    maker_fee = 0.0002

    paper_leverage = 100
    stop_loss = 0.005  # 1%
    take_profit = 0.015  # 1.5%
    trailing_stop_loss = 0.01  # 0.5%
    trailing_take_profit = 0.01  # 0.5%
    max_trailing_takes = 2

    ema_amplitude = 2.4
    indicator = "ema9"
    max_atr_value = 0.04

    touches = 0
    wins = 0
    loses = 0
    trailing_loses = 0

    last_action = None
    last_orders = []

    OPEN_LONG = "open_long"
    OPEN_SHORT = "open_short"
    INCREASE_LONG = "increase_long"
    INCREASE_SHORT = "increase_short"

    DIRECTION_LONG = "Long"
    DIRECTION_SHORT = "Short"

    symbols_settings = {}

    use_trailing_entry = False
    trailing_amplitude_diff = 15
    amplitude_manager = {}

    symbols = [
        'APTUSDT', 'DYDXUSDT', 'ANKRUSDT',
        'OPUSDT', 'MATICUSDT', 'DOTUSDT',
        'APEUSDT', 'AVAXUSDT', 'ENJUSDT',
        'IMXUSDT', 'LINKUSDT', 'GALAUSDT',
        'BNBUSDT', 'INJUSDT', 'FILUSDT',
        'SOLUSDT', 'FLMUSDT', 'FTMUSDT',
        'ETCUSDT', 'TRXUSDT', 'LTCUSDT',
        'MANAUSDT', 'XRPUSDT', 'LDOUSDT',
        'ADAUSDT', 'DOGEUSDT', 'ATOMUSDT',
        'AAVEUSDT', 'NEARUSDT', 'AXSUSDT',
    ]

    shit_coins = [
        'GRTUSDT', 'WAVESUSDT', '1000SHIBUSDT',
        'TOMOUSDT', 'RLCUSDT', 'JASMYUSDT',
        'KAVAUSDT', 'AGIXUSDT', 'SPELLUSDT',
        'IDEXUSDT', 'AMBUSDT', 'LINAUSDT',
        'KEYUSDT', 'HBARUSDT', 'RNDRUSDT',
        'DUSKUSDT', '1000PEPE', 'INJUSDT',
        'STORJUSDT', 'SUIUSDT', 'MTLUSDT',
        'TLMUSDT', 'ALPHAUSDT', 'RENUSDT',
    ]

    def __init__(self):
        pass

    def get_symbols_with_shitcoins(self) -> list:
        return self.symbols + self.shit_coins

    @staticmethod
    def create_mysql_connection():
        return mysql.connector.connect(host=os.getenv('MYSQL_HOST'),
                                       database=os.getenv('MYSQL_DATABASE'),
                                       user=os.getenv('MYSQL_USER'),
                                       password=os.getenv('MYSQL_PWD'))

    @staticmethod
    def close_mysql_connection(connection: MySQLConnectionAbstract):
        if connection.is_connected():
            connection.close()

    def update_symbol_settings_from_db(self):
        connection = self.create_mysql_connection()

        cursor = connection.cursor(dictionary=True)
        cursor.execute("select * from symbol_settings")

        for setting in cursor.fetchall():
            self.set_symbol_setting(setting["symbol"], "amplitude", float(setting["amplitude"]))
            self.set_symbol_setting(setting["symbol"], "indicator", setting["indicator"])

        self.close_mysql_connection(connection)

    def save_symbol_settings_to_db(self, symbol: str, amplitude: float, indicator: str, balance: float = None):
        connection = self.create_mysql_connection()

        cursor = connection.cursor(dictionary=True)
        cursor.execute(f"select id from symbol_settings where symbol = '{symbol}'")

        row = cursor.fetchone()

        now = time.strftime('%Y-%m-%d %H:%M:%S')
        if row is not None:
            cursor.execute(
                f"update symbol_settings "
                f"set amplitude = {amplitude}, "
                f"indicator = '{indicator}', "
                f"hyperopted_balance = '{str(balance)}', "
                f"updated_at = '{now}' "
                f"where id = {row['id']}"
            )
        else:
            cursor.execute(
                f"insert into symbol_settings (symbol, amplitude, indicator, hyperopted_balance, created_at) values"
                f"('{symbol}', {amplitude}, '{indicator}', '{str(balance)}', '{now}')"
            )

        connection.commit()

        self.close_mysql_connection(connection)

    def set_symbol_setting(self, symbol: str, setting: str, value: any):
        if symbol not in self.symbols_settings:
            self.symbols_settings[symbol] = {}

        self.symbols_settings[symbol][setting] = value

    def get_symbol_setting(self, symbol: str, setting: str) -> any:
        def get_default_setting() -> any:
            if setting == "amplitude":
                return self.ema_amplitude
            elif setting == "indicator":
                return self.indicator

            return None

        if symbol not in self.symbols_settings:
            return get_default_setting()
        else:
            if setting in self.symbols_settings[symbol]:
                return self.symbols_settings[symbol][setting]
            else:
                return get_default_setting()



