import requests 
import json
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

# Set parameters
pair = 'SOL-USDT'
to_timestamp = 1698796740
expected_cost = 0.0015
roll = 1440 # 1 day

def strategy_spread_arbitrage(pair=pair, end_time=to_timestamp, roll=roll, cost=expected_cost):

    # Get prices
    cc_key = 'cb14b4524f80ec7799b0dbf210445f10412843e5a7356e0d7f48860518d14504'
    spot_pick = 'spot_close'
    futures_pick = 'futures_close'
    limit = 2000
    timestamp_list = [to_timestamp - (i*60*2000) for i in range(25)]
    df_ = []
    for ts in timestamp_list:
        spot_api = f'https://data-api.cryptocompare.com/spot/v1/historical/minutes?market=binance&instrument={pair}&limit={limit}&response_format=JSON&groups=OHLC&to_ts={ts}&api_key={cc_key}'
        futures_api = f'https://data-api.cryptocompare.com/futures/v1/historical/minutes?market=binance&instrument={pair}-VANILLA-PERPETUAL&limit={limit}&groups=OHLC&to_ts={ts}&api_key={cc_key}'

        spot_df = pd.DataFrame(requests.get(spot_api).json().get('Data')).set_index('TIMESTAMP').drop(columns=['UNIT']).add_prefix('spot_')
        spot_df.index = pd.to_datetime(spot_df.index, unit='s')
        spot_df.columns = [x.lower() for x in spot_df.columns.tolist()]
        futures_df = pd.DataFrame(requests.get(futures_api).json().get('Data')).set_index('TIMESTAMP').drop(columns=['UNIT']).add_prefix('futures_')
        futures_df.index = pd.to_datetime(futures_df.index, unit='s')
        futures_df.columns = [x.lower() for x in futures_df.columns.tolist()]

        concat = pd.concat([spot_df, futures_df], axis=1)
        concat['spread'] = concat[futures_pick] - concat[spot_pick]
        concat['percent_diff'] = concat['spread'] / concat[spot_pick]
        df_.append(concat)
        time.sleep(0.1)
    df_ = pd.concat(df_, axis=0).sort_index()

    # Strategy backtesting
    df = df_.copy()
    df['diff_ma'] = df['percent_diff'].rolling(roll).mean()
    ## signal
    df['signal_entry'] = df['percent_diff'] >= (df['diff_ma'] + expected_cost) # Entry when percent spread equal or more than MA plus expected cost
    #df['signal_entry'] = df['percent_diff'] >= (df['diff_ma'] + (4 * df['percent_diff'].rolling(roll).std())) # Entry when percent spread equal or more than 4sd of MA
    df['signal_exit'] = df['percent_diff'] <= (df['diff_ma'] - (4 * df['percent_diff'].rolling(roll).std())) # Exit when percent spread go down to 4sd of MA or below
    ## action prices
    df['spot_action'] = df['spot_open'].shift(-1)
    df['futures_action'] = df['futures_open'].shift(-1)
    ## position on-off
    df.loc[(df['signal_entry'] == True) & (df['signal_entry'].shift(1) == False), 'on_position'] = 1
    df.loc[(df['signal_exit'].shift(1) == True) & (df['signal_exit'].shift(2) == False), 'on_position'] = 0
    df['on_position'].ffill(inplace=True)
    df.loc[(df['on_position'] == 1) & (df['on_position'].shift(1) == 0), 'action_cost'] = 1
    df.loc[(df['on_position'] == 0) & (df['on_position'].shift(1) == 1), 'action_cost'] = 1
    df['action_cost'].fillna(0, inplace=True)
    ## returns & portfolio
    df['spot_return'] = df['spot_action'].pct_change()
    df['futures_return'] = (df['futures_action'].shift(1))/(df['futures_action']) - 1
    df['real_spot_return'] = ((1 + df['spot_return']) * (1 + df['action_cost'] * -0.001)) - 1
    df['real_futures_return'] = ((1 + df['futures_return']) * (1 + df['action_cost'] * -0.0005)) - 1
    df['expected_return'] = df['on_position'] * ((df['real_spot_return']*0.5) + (df['real_futures_return']*0.5))
    df['expected_return'].fillna(0, inplace=True)
    df['portfolio'] = df['expected_return'].add(1).cumprod().sub(1)
    
    return df
