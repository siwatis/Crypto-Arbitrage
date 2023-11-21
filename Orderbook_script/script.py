import requests
import csv
import schedule
import time
        
#pair = input("Give a pair (Ex : 'DOGEUSDT'):")
pair = 'ETHUSDC'

spot_api = f'https://www.binance.com/api/v3/depth?symbol=ETHUSDC&limit=10'
futures_api = f'https://www.binance.com/api/v3/depth?symbol=ETHUSDC&limit=10'

csv_name = f'{pair}.csv'
row = 1
columns = ['timestamp','market',
           'best_bid_price','best_bid_quant',
           'best_ask_price','best_ask_quant']

def generate_header():
    with open(csv_name, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(columns)

def fetch_spot_data():
    response = requests.get(spot_api)
    if response.status_code == 200:
        data = response.json()
        market = 's'
        timestamp = int(data.get('lastUpdateId'))
        best_bid_price = float(data.get('bids')[0][0])
        best_bid_quant = float(data.get('bids')[0][1])
        best_ask_price = float(data.get('asks')[0][0])
        best_ask_quant = float(data.get('asks')[0][1])
        data_dict = {columns[0] : timestamp,
                     columns[1] : market,
                     columns[2] : best_bid_price,
                     columns[3] : best_bid_quant,
                     columns[4] : best_ask_price,
                     columns[5] : best_ask_quant}
        return data_dict
    else:
        raise Exception(f"API Error: {response.status_code}")

def fetch_futures_data():
    response = requests.get(futures_api)
    if response.status_code == 200:
        data = response.json()
        market = 'f'
        timestamp = data.get('lastUpdateId')
        best_bid_price = float(data.get('bids')[0][0])
        best_bid_quant = float(data.get('bids')[0][1])
        best_ask_price = float(data.get('asks')[0][0])
        best_ask_quant = float(data.get('asks')[0][1])
        data_dict = {columns[0] : timestamp,
                     columns[1] : market,
                     columns[2] : best_bid_price,
                     columns[3] : best_bid_quant,
                     columns[4] : best_ask_price,
                     columns[5] : best_ask_quant}
        return data_dict
    else:
        raise Exception(f"API Error: {response.status_code}")

def append_data_to_csv():
    spot_data = fetch_spot_data()
    futures_data = fetch_futures_data()
    with open(csv_name, 'a+', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=[columns])
        writer.writerow(spot_data)
        row += 1
        print(f'Row {row} : {spot_data}')
        writer.writerow(futures_data)
        row += 1
        print(f'Row {row} : {futures_data}')

def scheduled_data_update():
    append_data_to_csv()

# Generate header
generate_header()
schedule.every(10).seconds.do(scheduled_data_update)

while True:
    schedule.run_pending()
    time.sleep(1)
