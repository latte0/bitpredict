import pymongo
import pandas as pd
from math import log
from time import time
import sys
from scipy.stats import linregress
import pickle

client = pymongo.MongoClient()
db = client['bitmicro']


def get_book_df(symbol, limit, convert_timestamps=False):
    books_db = db[symbol+'_books']
    cursor = books_db.find().sort('_id', -1).limit(limit)
    books = pd.DataFrame(list(cursor))
    books = books.set_index('_id')
    if convert_timestamps:
        books.index = pd.to_datetime(books.index, unit='s')

    def to_df(x):
        return pd.DataFrame(x[:20])
    k = books.applymap(to_df).sort_index()

    print(len(k))
    return k


def get_width_and_mid(books):
    best_bid = books.bids.apply(lambda x: x.price[0])
    best_ask = books.asks.apply(lambda x: x.price[0])
    return best_ask-best_bid, (best_bid + best_ask)/2


def get_future_mid(books, offset, sensitivity):
    def future(timestamp):
        i = books.index.get_loc(timestamp+offset, method='nearest')
        print i
        print books.index[i]
        print books.mid.iloc[i]
        return books.mid.iloc[i]
    return (books.index.map(future)/books.mid).apply(log)


def get_power_imbalance(books, n=10, power=2):

    def calc_imbalance(book):
        def calc(x):
            return x.amount*(.5*book.width/(x.price-book.mid))**power
        bid_imbalance = book.bids.iloc[:n].apply(calc, axis=1)
        ask_imbalance = book.asks.iloc[:n].apply(calc, axis=1)
        return (bid_imbalance-ask_imbalance).sum()
    imbalance = books.apply(calc_imbalance, axis=1)
    return imbalance


def get_power_adjusted_price(books, n=10, power=2):

    def calc_adjusted_price(book):
        def calc(x):
            return x.amount*(.5*book.width/(x.price-book.mid))**power
        bid_inv = 1/book.bids.iloc[:n].apply(calc, axis=1)
        ask_inv = 1/book.asks.iloc[:n].apply(calc, axis=1)
        bid_price = book.bids.price.iloc[:n]
        ask_price = book.asks.price.iloc[:n]
        return (bid_price*bid_inv + ask_price*ask_inv).sum() /\
            (bid_inv + ask_inv).sum()
    adjusted = books.apply(calc_adjusted_price, axis=1)
    return (adjusted/books.mid).apply(log).fillna(0)


def get_trade_df(symbol, min_ts, max_ts, convert_timestamps=False):
    trades_db = db[symbol+'_trades']
    query = {'timestamp': {'$gt': min_ts, '$lt': max_ts}}
    cursor = trades_db.find(query).sort('_id', pymongo.ASCENDING)
    trades = pd.DataFrame(list(cursor))
    if not trades.empty:
        trades = trades.set_index('_id')
        if convert_timestamps:
            trades.index = pd.to_datetime(trades.index, unit='s')
    return trades


def get_trades_indexes(books, trades, offset, live=False):
    def indexes(ts):
        ts = int(ts)
        i_0 = trades.timestamp.searchsorted([ts-offset], side='left')[0]
        if live:
            i_n = -1
        else:
            i_n = trades.timestamp.searchsorted([ts-1], side='right')[0]
        return (i_0, i_n)
    return books.index.map(indexes)


def get_trades_count(books, trades):
    def count(x):
        return len(trades.iloc[x.indexes[0]:x.indexes[1]])
    return books.apply(count, axis=1)


def get_trades_average(books, trades):

    def mean_trades(x):
        trades_n = trades.iloc[x.indexes[0]:x.indexes[1]]
        if not trades_n.empty:
            return (trades_n.price*trades_n.amount).sum()/trades_n.amount.sum()
    return (books.mid/books.apply(mean_trades, axis=1)).apply(log).fillna(0)


def get_aggressor(books, trades):

    def aggressor(x):
        trades_n = trades.iloc[x.indexes[0]:x.indexes[1]]
        if trades_n.empty:
            return 0
        buys = trades_n['side'] == 'BUY'
        buy_vol = trades_n[buys].amount.sum()
        sell_vol = trades_n[~buys].amount.sum()
        return buy_vol - sell_vol
    return books.apply(aggressor, axis=1)


def get_trend(books, trades):

    def trend(x):
        trades_n = trades.iloc[x.indexes[0]:x.indexes[1]]
        if len(trades_n) < 3:
            return 0
        else:
            return linregress(trades_n.index.values, trades_n.price.values)[0]
    return books.apply(trend, axis=1)


def make_features(symbol, sample, mid_offsets,
                  trades_offsets, powers, live=False):
    start = time()
    stage = time()

    books = get_book_df(symbol, sample)
    print(len(books))
    if not live:
        stage = time()
    books['width'], books['mid'] = get_width_and_mid(books)
    books=books.dropna()
    if not live:
        stage = time()
    for n in mid_offsets:
        books['mid{}'.format(n)] = get_future_mid(books, n, sensitivity=1)
    print(len(books))
    print(books)
    if not live:
        books = books.dropna()
        print 'offset mids run time:', (time()-stage)/60, 'minutes'
        stage = time()
    print(len(books))
    for p in powers:
        books['imbalance{}'.format(p)] = get_power_imbalance(books, 10, p)
        books['adj_price{}'.format(p)] = get_power_adjusted_price(books, 10, p)
    if not live:
        print 'power calcs run time:', (time()-stage)/60, 'minutes'
        stage = time()
    books = books.drop(['bids', 'asks'], axis=1)

    print(len(books))
    min_ts = books.index.min() - trades_offsets[-1]
    max_ts = books.index.max()
    if live:
        max_ts += 10
    trades = get_trade_df(symbol, min_ts, max_ts)
    for n in trades_offsets:
        if trades.empty:
            books['indexes'] = 0
            books['t{}_count'.format(n)] = 0
            books['t{}_av'.format(n)] = 0
            books['agg{}'.format(n)] = 0
            books['trend{}'.format(n)] = 0
        else:
            books['indexes'] = get_trades_indexes(books, trades, n, live)
            books['t{}_count'.format(n)] = get_trades_count(books, trades)
            books['t{}_av'.format(n)] = get_trades_average(books, trades)
            books['agg{}'.format(n)] = get_aggressor(books, trades)
            books['trend{}'.format(n)] = get_trend(books, trades)
    if not live:
        stage = time()


    print(len(books))
    return books.drop('indexes', axis=1)


def make_data(symbol, sample):
    data = make_features(symbol,
                         sample=sample,
                         mid_offsets=[60],
                         trades_offsets=[30, 60, 120, 180],
                         powers=[2, 4, 8])
    return data

if __name__ == '__main__' and len(sys.argv) == 4:
    data = make_data(sys.argv[1], int(sys.argv[2]))
    with open(sys.argv[3], 'w+') as f:
        pickle.dump(data, f)
