import csv
import datetime
import glob
import os
import time

import pandas as pd
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

strategy = Strategy(account, setting)

logs_path = 'logs'
os.chdir(logs_path)
log_files = glob.glob('*.{}'.format('csv'))

from_date = None
# from_date = "05-05-2023 00:00:01"

# strategy.setting.ema1_amplitude = 2.25
# strategy.setting.ema2_amplitude = 2.25
# strategy.setting.take_profit = 0.02
# strategy.setting.max_trailing_takes = 3

strategy.setting.use_trailing_entry = False
strategy.setting.trailing_amplitude_diff = 10

strategy.setting.indicator = "ema9"
strategy.setting.ema_amplitude = 2.4

if from_date:
    from_date = time.mktime(
        datetime.datetime.strptime(from_date, "%d-%m-%Y %H:%M:%S").timetuple()
    ) * 1000

for file in log_files:
    symbol = file[:-4]

    if symbol not in ['LDOUSDT']:
        continue

    print(f'Processing symbol {symbol}')
    try:
        file_abs_path = os.getcwd() + "/" + file

        print(file_abs_path, os.path.isfile(file_abs_path))

        with open(os.getcwd() + "/" + file, 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')

            header = []
            for i, row in enumerate(csv_reader):
                if i == 0:
                    header = row
                    continue

                df = pd.DataFrame([row], columns=header)
                df_data = df.iloc[0]

                if from_date:
                    if float(df_data.close_time) < from_date:
                        continue

                strategy.process_kline_event(symbol, df_data, float(df_data['current_price']))
    except FileNotFoundError as re:
        # raise re
        print(re)

print(f'Wins: {strategy.setting.wins}')
print(f'Loses: {strategy.setting.loses}')
print(f'Trailing Loses: {strategy.setting.trailing_loses}')
