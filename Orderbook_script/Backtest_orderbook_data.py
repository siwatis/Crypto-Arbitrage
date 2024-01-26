import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from toolz import pipe

def read_orderbook_data(filename):
    return pd.read_csv(filename)

def prepare_orderbook_data(df):
    df['best_bid_vol'] = df['best_bid_price'] * df['best_bid_quant']
    df['best_ask_vol'] = df['best_ask_price'] * df['best_ask_quant']
    selected_columns = ['best_bid_price','best_ask_price','best_bid_vol','best_ask_vol']
    raw_spot = df[df['market'] == 's'].set_index('timestamp')[selected_columns].rename(columns={'best_bid_price':'spot_bid',
                                                                                                'best_ask_price':'spot_ask',
                                                                                                'best_bid_vol':'spot_bid_vol',
                                                                                                'best_ask_vol':'spot_ask_vol'})
    raw_futures = df[df['market'] == 'f'].set_index('timestamp')[selected_columns].rename(columns={'best_bid_price':'futures_bid',
                                                                                                   'best_ask_price':'futures_ask',
                                                                                                   'best_bid_vol':'futures_bid_vol',
                                                                                                   'best_ask_vol':'futures_ask_vol'})
    data = pd.concat([raw_spot, raw_futures], axis=1)
    return data

def strategy_signal(data, rolling=(1*24*60*60), expected_cost=0.0015):
    data['pct_spread'] = (data['futures_bid'] / data['spot_ask']) - 1
    data['ma_pct_spread'] = data['pct_spread'].ffill().rolling(rolling).mean()
    data['signal_entry_zone'] = data['pct_spread'] > (data['ma_pct_spread'] + expected_cost)
    data['signal_entry'] = (data['signal_entry_zone'] == True) & (data['signal_entry_zone'].shift(1) == False)
    data['signal_exit_zone'] = data['pct_spread'] < (data['ma_pct_spread'] - 3*(data['pct_spread'].ffill().rolling(rolling).std()))
    data['signal_exit'] = (data['signal_exit_zone'] == True) & (data['signal_exit_zone'].shift(1) == False)
    return data

if __name__ == '__main__':
    pipe('[filename.csv]', read_orderbook_data, prepare_orderbook_data, strategy_signal)
