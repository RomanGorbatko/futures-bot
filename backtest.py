import csv
import glob
import os

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

for file in log_files:
    symbol = file[:-4]

    # if symbol != 'FILUSDT' and symbol != 'APTUSDT':
    #     continue

    try:
        with open(file, 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')

            header = []
            for i, row in enumerate(csv_reader):
                if i == 0:
                    header = row
                    continue

                df = pd.DataFrame([row], columns=header)
                df_data = df.iloc[0]

                strategy.process_kline_event(symbol, df_data, float(df_data['current_price']))
    except FileNotFoundError as re:
        print(re)

    # exit()
