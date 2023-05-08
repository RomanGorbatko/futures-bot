import os
from datetime import datetime

import pandas as pd
import binance

from src.account import Account
from src.setting import Setting
from src.utils import Utils


class Strategy:
    account: Account = None
    setting: Setting = None
    utils: Utils = None

    liquidation_callback: callable = None
    should_dump_to_csv: bool = False
    live: bool = False
    client: binance.Client = None

    def __init__(self, account: Account, setting: Setting):
        self.account = account
        self.setting = setting

        self.utils = Utils(os.getenv("ENV"))
        self.live = (
            bool(os.getenv("LIVE"))
            if os.getenv("LIVE") is not None and os.getenv("LIVE") == "True"
            else False
        )
        self.should_dump_to_csv = (
            bool(os.getenv("DUMP_TO_CSV"))
            if os.getenv("DUMP_TO_CSV") is not None and os.getenv("DUMP_TO_CSV") == "True"
            else False
        )

        if not self.setting.is_back_test:
            self.create_client()

        if self.live:
            self.setup_binance()

    @staticmethod
    def get_percentage_difference(num_a, num_b):
        if isinstance(num_a, pd.Series):
            num_a = num_a.astype("float")
        else:
            num_a = float(num_a)

        if isinstance(num_b, pd.Series):
            num_b = num_b.astype("float")
        else:
            num_b = float(num_b)

        diff = num_a - num_b
        divided = diff / num_a

        return divided * 100

    def update_current_balance(self):
        info = self.client.futures_account_balance()

        *_, usdt_balance = filter(lambda d: d['asset'] == 'USDT', info)

        balance = float(usdt_balance['balance'])

    def update_leverage(self):
        info = self.client.futures_leverage_bracket()

        for s in self.setting.symbols:
            *_, leverage_info = filter(lambda d: d['symbol'] == s, info)
            leverage_info['brackets'].sort(key=lambda x: x['initialLeverage'], reverse=True)

            if s not in self.setting.symbols_settings:
                self.setting.symbols_settings[s] = {}

            # comment below line to disable
            # leverage_info['brackets'][0]['initialLeverage'] = 2

            self.setting.symbols_settings[s]['leverage'] = leverage_info['brackets'][0]

            self.client.futures_change_leverage(
                symbol=s,
                # leverage=2
                leverage=self.setting.symbols_settings[s]['leverage']['initialLeverage']
            )

    def setup_symbols_settings(self):
        futures_info = self.client.futures_exchange_info()

        for s in self.setting.symbols:
            *_, symbol_info = filter(lambda d: d['symbol'] == s, futures_info['symbols'])

            if s not in self.setting.symbols_settings:
                self.setting.symbols_settings[s] = {}

            self.setting.symbols_settings[s]['info'] = symbol_info

    def setup_binance(self):
        self.update_current_balance()
        self.update_leverage()
        self.setup_symbols_settings()

    def create_client(self):
        if os.getenv("BINANCE_API_KEY") and os.getenv("BINANCE_API_SECRET"):
            self.client = binance.Client(
                os.getenv("BINANCE_API_KEY"), os.getenv("BINANCE_API_SECRET")
            )

        self.client = binance.Client()

    def calculate_taker_fee(self, size: float):
        return size - (size * (1 - self.setting.taker_fee))

    def calculate_maker_fee(self, size: float):
        return size - (size * (1 - self.setting.maker_fee))

    def get_symbol_leverage(self, s: str):
        if not self.live:
            return self.setting.paper_leverage

        return self.setting.symbols_settings[s]["leverage"]["initialLeverage"]

    def get_symbol_quantity_precision(self, s: str):
        return self.setting.symbols_settings[s]["info"]["quantityPrecision"]

    def get_symbol_price_precision(self, s: str):
        return self.setting.symbols_settings[s]["info"]["pricePrecision"]

    def calculate_avg_order_entry(self):
        return sum(item["position_size"] for item in self.setting.last_orders) / sum(
            item["asset_size"] for item in self.setting.last_orders
        )

    def calculate_entry_position_size(self, s: str, high_risk: bool = False):
        risk = (
            self.setting.high_risk_per_trade
            if high_risk
            else self.setting.low_risk_per_trade
        )

        # print(f"Risk: {risk * 100}%, Position: ${position_size:,.2f}")
        size = (self.account.balance * risk) * self.get_symbol_leverage(s)
        fee = self.calculate_taker_fee(size)

        self.account.position_fee += fee

        return size - fee

    def calculate_pnl(self, price: float, reverse: bool = False):
        entry_price = self.account.entry_price
        position_size = self.account.position_size

        rate = (entry_price / price) if reverse else (price / entry_price)

        return (rate * position_size) - position_size

    def open_position(
        self, s: str, current_price: float, direction: str, current_time: str
    ):
        if direction is self.setting.DIRECTION_LONG:
            self.account.long_position = True
        else:
            self.account.short_position = True

        self.account.symbol_position = s
        self.account.entry_price = current_price
        self.account.position_size = self.calculate_entry_position_size(s)
        self.account.asset_size = self.account.position_size / self.account.entry_price

        if self.live:
            side = (
                binance.Client.SIDE_BUY
                if direction is self.setting.DIRECTION_LONG
                else binance.Client.SIDE_SELL
            )

            self.client.futures_create_order(
                symbol=s,
                side=side,
                type=binance.Client.ORDER_TYPE_MARKET,
                quantity=round(
                    self.account.asset_size, self.get_symbol_quantity_precision(s)
                ),
            )
            position = self.client.futures_position_information(symbol=s)[0]
            self.account.entry_price = float(position["entryPrice"])

        self.setting.touches = 1
        self.setting.last_orders = []

        if direction is self.setting.DIRECTION_LONG:
            self.account.stop_loss_price = self.account.entry_price * (
                1 - self.setting.stop_loss
            )
            self.account.take_profit_price = self.account.entry_price * (
                1 + self.setting.take_profit
            )
            self.setting.last_action = self.setting.OPEN_LONG
        else:
            self.account.stop_loss_price = self.account.entry_price * (
                1 + self.setting.stop_loss
            )
            self.account.take_profit_price = self.account.entry_price * (
                1 - self.setting.take_profit
            )
            self.setting.last_action = self.setting.OPEN_SHORT

        if self.live:
            side = (
                binance.Client.SIDE_SELL
                if direction is self.setting.DIRECTION_LONG
                else binance.Client.SIDE_BUY
            )

            stop_order = self.client.futures_create_order(
                symbol=s,
                side=side,
                type=binance.Client.FUTURE_ORDER_TYPE_STOP_MARKET,
                closePosition="true",
                stopPrice=round(
                    self.account.stop_loss_price, self.get_symbol_price_precision(s)
                )
            )

            self.account.last_stop_loss_order_id = stop_order["orderId"]

        self.setting.last_orders.append(
            {
                "symbol": s,
                "last_action": self.setting.last_action,
                "entry_price": self.account.entry_price,
                "asset_size": self.account.asset_size,
                "position_size": self.account.position_size,
            }
        )

        self.utils.print_log(
            {
                "Symbol": s,
                "Time": current_time,
                "Open": "Long 🟢"
                if direction is self.setting.DIRECTION_LONG
                else "Short 🔴",
                "Position Size": f"${self.account.position_size:,.4f}",
                "Asset Size": f"{self.account.asset_size:.4f}",
                "Entry Price": f"{self.account.entry_price:.5f}",
                "Stop Loss Price": f"{self.account.stop_loss_price:.5f}",
                "Take Profit Price": f"{self.account.take_profit_price:.5f}",
                "Balance": f"${self.account.balance:,.4f}",
            }
        )

    def close_position(self, s: str, pnl: float, direction: str):
        self.account.symbol_position = None

        if direction is self.setting.DIRECTION_LONG:
            self.account.long_position = False
        else:
            self.account.short_position = False

        if pnl < 0:
            if self.setting.touches > 1:
                self.setting.trailing_loses += 1
            else:
                self.setting.loses += 1
        else:
            self.setting.wins += 1

        fee = self.calculate_maker_fee(self.account.position_size)
        self.account.position_fee += fee

        self.setting.touches = 0
        self.account.position_size = 0
        self.account.asset_size = 0
        self.account.last_stop_loss_order_id = 0
        self.account.last_take_profit_order_id = 0

        self.account.balance += pnl - self.account.position_fee

    def manage_opened_position(
        self, s: str, current_price: float, direction: str, current_time: str
    ):
        if direction is self.setting.DIRECTION_LONG:
            exit_condition = current_price <= self.account.stop_loss_price
        else:
            exit_condition = current_price >= self.account.stop_loss_price

        exit_price = (
            self.account.stop_loss_price
            if exit_condition
            else self.account.take_profit_price
        )
        pnl = self.calculate_pnl(exit_price, direction is self.setting.DIRECTION_SHORT)

        if pnl < 0:  # stop loss
            self.close_position(s, pnl, direction)
        else:  # take profits
            if self.setting.touches <= self.setting.max_trailing_takes:  # trailing
                last_action_increase = (
                    self.setting.INCREASE_LONG
                    if direction is self.setting.DIRECTION_LONG
                    else self.setting.INCREASE_SHORT
                )

                increase_position_size = self.calculate_entry_position_size(s, True)
                increase_asset_size = increase_position_size / current_price

                self.setting.last_orders.append(
                    {
                        "symbol": s,
                        "last_action": last_action_increase,
                        "entry_price": current_price,
                        "asset_size": increase_asset_size,
                        "position_size": increase_position_size,
                    }
                )

                self.setting.touches += 1
                self.account.entry_price = self.calculate_avg_order_entry()

                # print('entry_price calculate_avg_order_entry', entry_price)
                if self.live:
                    side = (
                        binance.Client.SIDE_BUY
                        if direction is self.setting.DIRECTION_LONG
                        else binance.Client.SIDE_SELL
                    )

                    self.client.futures_create_order(
                        symbol=s,
                        side=side,
                        type=binance.Client.ORDER_TYPE_MARKET,
                        quantity=round(
                            increase_asset_size, self.get_symbol_quantity_precision(s)
                        ),
                    )
                    position = self.client.futures_position_information(symbol=s)[0]
                    self.account.entry_price = float(position["entryPrice"])

                self.account.position_size += increase_position_size
                self.account.asset_size += increase_asset_size

                # print('entry_price position', entry_price)

                if direction is self.setting.DIRECTION_LONG:
                    self.account.stop_loss_price = self.account.entry_price * (
                        1 - self.setting.trailing_stop_loss
                    )
                    self.account.take_profit_price = self.account.entry_price * (
                        1 + self.setting.trailing_take_profit
                    )
                else:
                    self.account.stop_loss_price = self.account.entry_price * (
                        1 + self.setting.trailing_stop_loss
                    )
                    self.account.take_profit_price = self.account.entry_price * (
                        1 - self.setting.trailing_take_profit
                    )

                if self.live:
                    self.client.futures_cancel_order(
                        symbol=s, orderId=self.account.last_stop_loss_order_id
                    )

                    side = (
                        binance.Client.SIDE_SELL
                        if direction is self.setting.DIRECTION_LONG
                        else binance.Client.SIDE_BUY
                    )

                    stop_order = self.client.futures_create_order(
                        symbol=s,
                        side=side,
                        type=binance.Client.FUTURE_ORDER_TYPE_STOP_MARKET,
                        closePosition="true",
                        stopPrice=round(
                            self.account.stop_loss_price,
                            self.get_symbol_price_precision(s),
                        )
                    )

                    self.account.last_stop_loss_order_id = stop_order["orderId"]

                    if (self.setting.touches - 1) == self.setting.max_trailing_takes:
                        side = (
                            binance.Client.SIDE_BUY
                            if direction is self.setting.DIRECTION_LONG
                            else binance.Client.SIDE_SELL
                        )

                        stop_order = self.client.futures_create_order(
                            symbol=s,
                            side=side,
                            type=binance.Client.FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
                            closePosition="true",
                            stopPrice=round(
                                self.account.take_profit_price,
                                self.get_symbol_price_precision(s),
                            )
                        )

                        self.account.last_take_profit_order_id = stop_order["orderId"]

                self.utils.print_log(
                    {
                        "Symbol": s,
                        "Time": current_time,
                        f"Increase {direction}": self.setting.touches - 1,
                        "Position Size": f"${self.account.position_size:,.4f}",
                        "Asset Size": f"{self.account.asset_size:.4f}",
                        "Entry Price": f"{self.account.entry_price:.5f}",
                        "Stop Loss Price": f"{self.account.stop_loss_price:.5f}",
                        "Take Profit Price": f"{self.account.take_profit_price:.5f}",
                    }
                )

                return
            else:  # absolute take profit
                self.close_position(s, pnl, direction)

        self.utils.print_log(
            {
                "Symbol": s,
                "Time": current_time,
                "Close": f"{direction} 🔵️",
                "Clear Pnl": f"${pnl:,.4f}",
                "Fee": f"$-{self.account.position_fee:,.4f}",
                "Entry Price": f"{self.account.entry_price:.5f}",
                "Exit Price": f"{exit_price:.5f}",
                "Balance": f"${self.account.balance:,.4f}",
            }
        )

        self.account.position_fee = 0

    # still development
    def is_amplitude_valid(self, s: str, actual_amplitude: float) -> bool:
        actual_amplitude = abs(actual_amplitude)

        if actual_amplitude >= self.setting.ema_amplitude:
            if s not in self.setting.amplitude_manager \
                    or (s in self.setting.amplitude_manager and self.setting.amplitude_manager[s] < actual_amplitude):
                self.setting.amplitude_manager[s] = actual_amplitude

            if actual_amplitude < self.setting.amplitude_manager[s]:
                amplitude_diff = self.get_percentage_difference(self.setting.amplitude_manager[s], actual_amplitude)

                if amplitude_diff >= self.setting.trailing_amplitude_diff: #  and actual_amplitude >= 4.8
                    print(f'Diff: amplitude_diff {amplitude_diff}, actual_amplitude {actual_amplitude}')
                    return True
        else:
            if s in self.setting.amplitude_manager:
                del self.setting.amplitude_manager[s]

        return False

    def process_kline_event(self, s: str, df_data: pd.DataFrame, current_price: float):
        current_time = datetime.fromtimestamp(
            int(float(df_data.close_time)) / 1000
        ).strftime("%Y-%m-%d %H:%M:%S")

        if pd.isna(df_data.ema9) or pd.isna(df_data.ema20) or pd.isna(df_data.ema55):
            return

        if self.account.balance <= 0:
            print(
                f"Time: {current_time}, LIQUIDATION! Balance: {self.account.balance:.5f}"
            )
            self.liquidation_callback(self)
            return

        actual_amplitude = self.get_percentage_difference(
            current_price, float(df_data.ema20)
        )

        # if self.setting.is_back_test:
        #     if abs(actual_amplitude) >= self.setting.ema_amplitude:
        #         print(s, current_time, actual_amplitude)

        if self.setting.use_trailing_entry:
            is_amplitude_valid = self.is_amplitude_valid(s, actual_amplitude)
        else:
            is_amplitude_valid = abs(actual_amplitude) >= self.setting.ema_amplitude

        if self.should_dump_to_csv:
            self.utils.dump_to_csv(s, df_data, current_price)

        # print(f"Symbol: {s}, Current price: {current_price}, Ema1: {df_data.ema1}, Ema2: {df_data.ema2}, "
        #       f"Ema3: {df_data.ema3}, Actual "
        #       f"Amplitude: {abs(actual_amplitude)}, Is Amplitude Valid: {is_amplitude_valid}")

        if (
            self.account.long_position
            and s == self.account.symbol_position
            and (
                current_price <= self.account.stop_loss_price
                or current_price >= self.account.take_profit_price
            )
        ):
            self.manage_opened_position(
                s, current_price, self.setting.DIRECTION_LONG, current_time
            )
            return

        if (
            self.account.short_position
            and s == self.account.symbol_position
            and (
                current_price >= self.account.stop_loss_price
                or current_price <= self.account.take_profit_price
            )
        ):
            self.manage_opened_position(
                s, current_price, self.setting.DIRECTION_SHORT, current_time
            )
            return

        if (
            not self.account.short_position
            and not self.account.long_position
            and self.setting.touches == 0
            and current_price > float(df_data.ema9)
            and current_price > float(df_data.ema20)
            and current_price > float(df_data.ema50)
            and actual_amplitude > 0
            and is_amplitude_valid
        ):
            self.open_position(
                s, current_price, self.setting.DIRECTION_SHORT, current_time
            )

        if (
            not self.account.long_position
            and not self.account.short_position
            and self.setting.touches == 0
            and current_price < float(df_data.ema9)
            and current_price < float(df_data.ema20)
            and current_price < float(df_data.ema50)
            and actual_amplitude < 0
            and is_amplitude_valid
        ):
            self.open_position(
                s, current_price, self.setting.DIRECTION_LONG, current_time
            )
