import pandas as pd
import os

CHUNK_SIZE = 50000
csv_file_list = ["./logs/old/DYDXUSDT.csv", "./logs/new/DYDXUSDT.csv"]
output_file = "./logs/DYDXUSDT.csv"

os.remove(output_file) if os.path.exists(output_file) else None

for i, csv_file_name in enumerate(csv_file_list):
    if i == 0:
        skip_row = []
    else:
        skip_row = [0]

    chunk_container = pd.read_csv(csv_file_name, chunksize=CHUNK_SIZE, skiprows=skip_row)
    for chunk in chunk_container:
        print(chunk)
        chunk.to_csv(output_file, mode="a", index=False)
