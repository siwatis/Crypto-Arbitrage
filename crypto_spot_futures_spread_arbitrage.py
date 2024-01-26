import requests 
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from toolz import pipe

# Set parameters
pair = 'AVAX-USDT'
to_timestamp = 1702400000
expected_cost = 0.0015
roll = 1440 # 7 day
exit_std = 3

def get_cc_price(pair=pair, days=90, limit=2000, to_timestamp=to_timestamp):

    cc_key = '--crypto_compare_api_key--'
    timestamp_list = [time.time() - (i*60*limit) for i in range(math.ceil(60*24*days/limit))]
    df = []
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
        df.append(concat)
        time.sleep(0.1)
    df = pd.concat(df, axis=0).sort_index()

    return df

def backtest(df, rolling=roll, exit_std=exit_std, conservative=False):

    spot_pick = 'spot_close'
    futures_pick = 'futures_close'
    df['spread'] = df[futures_pick] - df[spot_pick]
    df['percent_diff'] = df['spread'] / df[spot_pick]
    df['diff_ma'] = df['percent_diff'].rolling(rolling).mean()

    ## signal
    df['signal_entry'] = df['percent_diff'] > (df['diff_ma'] + expected_cost) # Entry when percent spread equal or more than MA plus expected cost
    df['signal_exit'] = df['percent_diff'] <= (df['diff_ma'] - (exit_std * df['percent_diff'].rolling(rolling).std())) # Exit when percent spread go down to 4sd of MA or below

    ## action prices
    df['spot_action'] = df['spot_open'].shift(-1)
    df['futures_action'] = df['futures_open'].shift(-1)
    df['spot_action_conservative'] = df['spot_close'].shift(-1)
    df['futures_action_conservative'] = df['futures_close'].shift(-1)

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

    ## conservative returns & portfolio
    if conservative:
        df['spot_return_conservative'] = df['spot_action_conservative'].pct_change()
        df['futures_return_conservative'] = (df['futures_action_conservative'].shift(1))/(df['futures_action_conservative']) - 1
        df['real_spot_return_conservative'] = ((1 + df['spot_return_conservative']) * (1 + df['action_cost'] * -0.001)) - 1
        df['real_futures_return_conservative'] = ((1 + df['futures_return_conservative']) * (1 + df['action_cost'] * -0.0005)) - 1
        df['expected_return_conservative'] = df['on_position'] * ((df['real_spot_return_conservative']*0.5) + (df['real_futures_return_conservative']*0.5))
        df['expected_return_conservative'].fillna(0, inplace=True)
        df['portfolio_conservative'] = df['expected_return_conservative'].add(1).cumprod().sub(1)

    return df

def plot_strategy(df, pair):
    plt.subplots(figsize=[12,6])
    plt.fill_between(df.index, df.on_position * df.percent_diff.max(), 
                     color="cornflowerblue", alpha=0.2)
    plt.fill_between(df.index, df.on_position * df.percent_diff.min(), 
                     color="cornflowerblue", alpha=0.2, label='Strategy on position')
    plt.plot(df.percent_diff, color='mediumslateblue', alpha=0.5, linewidth=0.9, label='Percentage of spread between spot and futures prices')
    plt.plot(df.percent_diff.rolling(roll).mean(), alpha=0.6, color='darkslateblue', label = f'MA ({roll} minutes moving average of percent spread)')
    plt.plot(df.percent_diff.rolling(roll).mean() + expected_cost, alpha=0.6, color='mediumaquamarine', label='Entry zone (>= MA + expected transaction costs)')
    plt.plot(df.percent_diff.rolling(roll).mean() - (2.75*df['percent_diff'].rolling(roll).std()), alpha=0.4, color='firebrick', label=f'Exit zone (<= MA - {exit_std} standard deviation)')
    #plt.plot(df.percent_diff.rolling(roll).mean() - expected_cost, alpha=0.6, color='firebrick', label='Exit zone')
    plt.scatter(x = df[df['percent_diff']>df.percent_diff.rolling(roll).mean() + expected_cost].index,
                y = df[df['percent_diff']>df.percent_diff.rolling(roll).mean() + expected_cost].percent_diff,
                color='limegreen', s=20, alpha=0.6, label='Entry signal (Open long-spot and short-futures)')
    plt.scatter(x = df[df['percent_diff']<=df.percent_diff.rolling(roll).mean() - (exit_std * df['percent_diff'].rolling(roll).std())].index,
                y = df[df['percent_diff']<=df.percent_diff.rolling(roll).mean() - (exit_std * df['percent_diff'].rolling(roll).std())].percent_diff, 
                color='indianred', s=10, alpha=0.6, label='Exit signal (Close spot and futures positions)')
    plt.axhline(0, alpha=0.5, color='grey')
    plt.ylabel('Percent Spread')
    plt.title(f"Long spot - short futures arbitrage strategy signals of {pair} in 90 days)")
    #plt.ylim((-0.008,0.003))
    plt.legend(loc='lower left', ncol = 2)
    plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1))
    plt.tight_layout()
    plt.show()

def plot_portfolio(df, pair, conservative=False):
    plt.subplots(figsize=[12,3])
    plt.axhline(0, alpha=0.5, color='grey')
    plt.plot(df.portfolio, color='mediumslateblue', alpha=0.7, linewidth=1.2, label=f'Cumulative returns: {round(df.portfolio[-1]*100,2)}% at the end')
    if conservative:
        try:
            plt.plot(df.portfolio_conservative, color='sandybrown', alpha=0.7, linewidth=1.2, label=f'Cumulative returns (conservative): {round(df.portfolio_conservative[-1]*100,2)}% at the end')
        except Exception:
            pass
    plt.title(f"Long spot - short futures arbitrage strategy portfolio return of {pair} in 90 days")
    plt.ylabel('Cumulative portfolio return')
    try:
        if (df.portfolio.min() > -0.01) & (df.portfolio_conservative.min() > -0.01):
            plt.ylim((-0.01))
    except Exception:
        pass
    plt.legend()
    plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1))
    plt.tight_layout()
    plt.show()

def plot_all(df, pair, conservative=False):
    plot_strategy(df, pair)
    plot_portfolio(df, pair, conservative=conservative)