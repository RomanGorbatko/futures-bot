import csv
import logging
import sys
import os

import requests
from prettytable import PrettyTable


class Utils:
    event_log = 'logs/{0}.csv'

    def __init__(self, ENV: str = "local"):
        self.ENV = ENV

    @staticmethod
    def logger():
        logging.basicConfig(level=logging.INFO, filename="logs/out.log", filemode="a+",
                            format="%(asctime)s %(levelname)s %(message)s")

        return logging

    def print_log(self, data):
        telegram_text = "Env: " + self.ENV + "\n"

        self.logger().info(data)

        for key, value in data.items():
            telegram_text += str(key) + ": " + str(value) + "\n"

        if os.getenv("TELEGRAM_CHAT_ID") and os.getenv("TELEGRAM_BOT_ID"):
            send_text = (
                "https://api.telegram.org/bot"
                + os.getenv("TELEGRAM_BOT_ID")
                + "/sendMessage?chat_id="
                + str(os.getenv("TELEGRAM_CHAT_ID"))
                + "&parse_mode=html&text="
                + telegram_text
            )

            response = requests.get(send_text)

    def dump_to_csv(self, s, event_data, current_price):
        row = event_data.values.tolist()
        row.append(current_price)

        headers = [
            'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore',
            'ema5', 'ema9', 'ema10', 'ema15', 'ema20', 'ema25', 'ema30', 'ema35', 'ema40', 'ema45', 'ema50', 'ema55',
            'ema60', 'ema65', 'ema70', 'ema75', 'ema80', 'ema85', 'ema90',
            'wma14', 'atr14', 'rsi14',
            'current_price'
        ]

        if not os.path.isfile(self.event_log.format(s)):
            with open(self.event_log.format(s), 'w', newline='') as out_csv:
                writer = csv.DictWriter(out_csv, fieldnames=headers, delimiter=',', lineterminator='\n')
                writer.writeheader()
        else:
            with open(self.event_log.format(s), 'a', newline='') as out_csv:
                writer = csv.writer(out_csv, delimiter=',', lineterminator='\n')
                writer.writerow(row)
