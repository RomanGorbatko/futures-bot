import csv
import datetime
import glob
import os
import time
from threading import Thread

import pandas as pd
import numpy as np
from dotenv import load_dotenv

from src.account import Account
from src.setting import Setting
from src.strategy import Strategy

load_dotenv()
os.environ['TZ'] = 'UTC'

balance = float(os.getenv('BALANCE')) if os.getenv('BALANCE') else 500.

account = Account(balance)
setting = Setting()
setting.is_back_test = True
setting.is_hyperopt = True

strategy = Strategy(account, setting)

log_files = glob.glob('logs/*.{}'.format('csv'))

from_date = None
# from_date = "05-05-2023 00:00:01"

# strategy.setting.ema1_amplitude = 2.25
# strategy.setting.ema2_amplitude = 2.25
# strategy.setting.take_profit = 0.02
# strategy.setting.max_trailing_takes = 3

strategy.setting.use_trailing_entry = False
strategy.setting.trailing_amplitude_diff = 10

hyperopt_params = {
    "indicator": [
        "ema5", "ema9", "ema10", "ema15", "ema20", "ema25", "ema30",
        "ema35", "ema40", "ema45", "ema50", "ema55", "ema60", "ema65",
        "ema70", "ema75", "ema80", "ema85", "ema90", "wma14"
    ],
    "amplitude": {
        "min": 1,
        "max": 5,
        "step": 0.1
    },
    "best_result": 500,
    "best_params": {
        "indicator": None,
        "amplitude": None,
    }
}


def run_back_test(csv_list, symbol):
    header = []

    print(
        datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), symbol,
        strategy.setting.ema_amplitude, strategy.setting.indicator, len(csv_list)
    )

    for i, row in enumerate(csv_list):
        if i == 0:
            header = row
            continue

        df = pd.DataFrame([row], columns=header)
        df_data = df.iloc[0]

        if from_date:
            if float(df_data.close_time) < from_date:
                continue

        strategy.process_kline_event(symbol, df_data, float(df_data['current_price']))


if from_date:
    from_date = time.mktime(
        datetime.datetime.strptime(from_date, "%d-%m-%Y %H:%M:%S").timetuple()
    ) * 1000


def process_file(file):
    symbol = file[5:-4]

    # if symbol not in ['LDOUSDT', 'INJUSDT']:
    #     continue

    print(f'Processing symbol {symbol}')
    try:
        file_abs_path = os.getcwd() + "/" + file

        print(file_abs_path, os.path.isfile(file_abs_path))

        with open(os.getcwd() + "/" + file, 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            csv_enumerate = list(csv_reader)

            for indicator in hyperopt_params["indicator"]:
                strategy.setting.indicator = indicator

                for amplitude in np.arange(
                        hyperopt_params["amplitude"]["min"],
                        hyperopt_params["amplitude"]["max"] + 1,
                        hyperopt_params["amplitude"]["step"]):
                    strategy.account.balance = 500
                    strategy.setting.ema_amplitude = round(amplitude, 2)

                    strategy.utils.print_log(
                        {
                            "Try params": "",
                            "Symbol": symbol,
                            "Indicator": indicator,
                            "Amplitude": strategy.setting.ema_amplitude,
                        }
                    )

                    run_back_test(csv_enumerate, symbol)

                    if strategy.account.balance > hyperopt_params["best_result"]:
                        hyperopt_params["best_result"] = strategy.account.balance
                        hyperopt_params["best_params"]["indicator"] = indicator
                        hyperopt_params["best_params"]["amplitude"] = amplitude

                        strategy.utils.print_log(
                            {
                                "Best Result": "",
                                "Symbol": symbol,
                                "Indicator": hyperopt_params["best_params"]["indicator"],
                                "Amplitude": hyperopt_params["best_params"]["amplitude"],
                                "Balance": hyperopt_params["best_result"],
                            }
                        )

    except FileNotFoundError as re:
        # raise re
        print(re)
    finally:
        if hyperopt_params["best_result"] > 500:
            strategy.setting.save_symbol_settings_to_db(
                symbol,
                float(hyperopt_params["best_params"]["amplitude"]),
                hyperopt_params["best_params"]["indicator"],
                round(hyperopt_params["best_result"], 2)
            )

        hyperopt_params["best_result"] = 500
        hyperopt_params["best_params"]["indicator"] = None
        hyperopt_params["best_params"]["amplitude"] = None


threads = []
for f in log_files:
    threads.append(Thread(target=process_file, args=(f,)))
    threads[-1].start()

for thread in threads:
    thread.join()
