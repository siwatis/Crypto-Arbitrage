import requests
import csv
import schedule
import time
import numpy as np
from datetime import datetime
    
pair = input("Give a pair (Ex : DOGEUSDT):")
print(pair)

spot_api = f'https://www.binance.com/api/v3/depth?symbol={pair}&limit=10'
futures_api = f'https://www.binance.com/fapi/v1/depth?symbol={pair}&limit=10'

csv_name = f'{pair}_{int(time.time())}.csv'
columns = ['timestamp','market',
           'best_bid_price','best_bid_quant','large_bid_volume',
           'best_ask_price','best_ask_quant','large_ask_volume']

def generate_header():
    with open(csv_name, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(columns)

def fetch_data():
    try:
        spot_response = requests.get(spot_api)
        futures_response = requests.get(futures_api)
        timestamp = round(time.time(), 2)
        spot_data = spot_response.json()
        best_bid_price = float(spot_data.get('bids')[0][0])
        best_bid_quant = float(spot_data.get('bids')[0][1])
        best_ask_price = float(spot_data.get('asks')[0][0])
        best_ask_quant = float(spot_data.get('asks')[0][1])
        large_bid_volume = np.array(spot_data.get("bids")).astype(float).prod(axis=1).sum()
        large_ask_volume = np.array(spot_data.get("asks")).astype(float).prod(axis=1).sum()
        spot_data_dict = {columns[0] : round(timestamp, 0),
                    columns[1] : 's',
                    columns[2] : best_bid_price,
                    columns[3] : best_bid_quant,
                    columns[4] : large_bid_volume,
                    columns[5] : best_ask_price,
                    columns[6] : best_ask_quant,
                    columns[7] : large_ask_volume}
        futures_data = futures_response.json()
        best_bid_price = float(futures_data.get('bids')[0][0])
        best_bid_quant = float(futures_data.get('bids')[0][1])
        best_ask_price = float(futures_data.get('asks')[0][0])
        best_ask_quant = float(futures_data.get('asks')[0][1])
        large_bid_volume = np.array(futures_data.get("bids")).astype(float).prod(axis=1).sum()
        large_ask_volume = np.array(futures_data.get("asks")).astype(float).prod(axis=1).sum()
        futures_data_dict = {columns[0] : round(timestamp, 0),
                    columns[1] : 'f',
                    columns[2] : best_bid_price,
                    columns[3] : best_bid_quant,
                    columns[4] : large_bid_volume,
                    columns[5] : best_ask_price,
                    columns[6] : best_ask_quant,
                    columns[7] : large_ask_volume}
    except:
        spot_data_dict = {columns[0] : timestamp,
                    columns[1] : 's',
                    columns[2] : np.nan,
                    columns[3] : np.nan,
                    columns[4] : np.nan,
                    columns[5] : np.nan,
                    columns[6] : np.nan,
                    columns[7] : np.nan}
        futures_data_dict = {columns[0] : timestamp,
                    columns[1] : 'f',
                    columns[2] : np.nan,
                    columns[3] : np.nan,
                    columns[4] : np.nan,
                    columns[5] : np.nan,
                    columns[6] : np.nan,
                    columns[7] : np.nan}
    return [timestamp, spot_data_dict, futures_data_dict]

def append_data_to_csv():
    data = fetch_data()
    spot_data = data[1]
    futures_data = data[2]
    with open(csv_name, 'a+', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writerow(spot_data)
        print(datetime.fromtimestamp(data[0]))
        print(spot_data)
        writer.writerow(futures_data)
        print(futures_data)

def scheduled_data_update():
    append_data_to_csv()

generate_header()

# Wait to start at 10 seconds
print('Waiting..')
while time.time() % 10 >= 0.001:
    if time.time() % 10 < 0.001:
        break
print('Start fetching',pair)

schedule.every(4.56).seconds.do(scheduled_data_update)

while True:
    schedule.run_pending()
    #time.sleep(1)
