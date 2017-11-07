import pybitflyer
import time
import datetime
import json
from pymongo import MongoClient
import sys

api = pybitflyer.API(api_key="YOURKEY", api_secret="YORSECRET")

#board = api.board(product_code="BTC_JPY")

#print(board)

symbol = sys.argv[1]
limit = 25

client = MongoClient()
db = client['bitmicro']
ltc_trades = db[symbol+'_trades']

def stamp(tstr):
    if not tstr.find('.') > -1:
        tstr = tstr + '.0'
    print(datetime.datetime.strptime(tstr, '%Y-%m-%dT%H:%M:%S.%f'))
    return time.mktime(datetime.datetime.strptime(tstr, '%Y-%m-%dT%H:%M:%S.%f').timetuple())


def format_trade_entry(trade):
    if all(key in trade for key in ('id', 'size', 'price', 'exec_date')):
        trade['_id'] = trade.pop('id')
        trade['amount'] = float(trade.pop('size'))
        trade['price'] = float(trade['price'])
        trade['timestamp'] = float(stamp(trade.pop('exec_date')))
    trade.pop('sell_child_order_acceptance_id')
    trade.pop('buy_child_order_acceptance_id')
    print(trade)
    return trade


def get_json():
    #resp = urllib2.urlopen(url)
    resp = api.executions(product_code="BTC_JPY")
    return json.loads(json.dumps(resp), object_hook=format_trade_entry)


print('Running...')
i=0
while True:
    start = time.time()
    trades = get_json()
    print(i)
    i = (i+1)%30
    for trade in trades:
        ltc_trades.update_one({'_id': trade['_id']},
                              {'$setOnInsert': trade}, upsert=True)
    last_timestamp = trades[0]['timestamp'] - 5
    time_delta = time.time()-start
    if time_delta < 1.0:
        time.sleep(5-time_delta)
