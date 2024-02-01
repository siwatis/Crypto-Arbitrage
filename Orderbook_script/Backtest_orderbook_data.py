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
    data['signal_group'] = data['signal_entry'].apply(lambda x: 1 if x == True else 0).cumsum().mul(data['signal_entry_zone']).replace(0, np.nan)
    return data

def backtest(data):
    data.loc[data['signal_entry'].shift(1) == True, 'action'] = 1 # action entry
    data.loc[data['signal_exit'].shift(1) == True, 'action'] = -1 # action exit
    data['action'].fillna(0, inplace=True)
    data['action_cost'] = data['action'].apply(lambda x: 1 if x != 0 else 0)
    data.loc[(data['action'] == 1) & (data['action'].shift(1) == 0), 'on_position'] = 1
    data.loc[(data['action'].shift(1) == -1) & (data['action'].shift(2) == 0), 'on_position'] = 0
    data['on_position'].ffill(inplace=True)

    data['spot_action_entry_price'] = data['spot_bid'].div(data['spot_ask']).sub(1).mul(data['action'].apply(lambda x: 1 if x == 1 else 0))
    data['spot_rest_price'] = data['spot_bid'].div(data['spot_bid'].shift(1)).sub(1).mul(data['action'].apply(lambda x: 1 if x != 1 else 0))
    data['spot_return'] = data['spot_action_entry_price'].add(data['spot_rest_price']).mul(data['on_position']).sub(data['action_cost']*0.0015)

    data['futures_action_entry_price'] = data['futures_ask'].div(data['spot_bid']).sub(1).mul(data['action'].apply(lambda x: 1 if x == 1 else 0))
    data['futures_rest_price'] = data['futures_ask'].div(data['futures_ask'].shift(1)).sub(1).mul(data['action'].apply(lambda x: 1 if x != 1 else 0))
    data['futures_return'] = data['futures_action_entry_price'].add(data['futures_rest_price']).mul(data['on_position']).sub(data['action_cost']*0.00075)

    data['spot_cumulative_return'] = data['spot_return'].add(1).cumprod().sub(1)
    data['futures_cumulative_return'] = data['futures_return'].add(1).cumprod().sub(1)
    data['portfolio_cumulative_return'] = data['spot_return'].add(data['futures']).div(2).add(1).cumprod().sub(1)
    return data

if __name__ == '__main__':
    pipe('[filename.csv]', read_orderbook_data, prepare_orderbook_data, strategy_signal)
