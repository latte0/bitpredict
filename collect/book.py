import pybitflyer
import time
import datetime
import json
from pymongo import MongoClient
import sys

api = pybitflyer.API(api_key="YOURKEY", api_secret="YORCODE)

#board = api.board(product_code="BTC_JPY")

#print(board)

symbol = sys.argv[1]
limit = 25

client = MongoClient()
db = client['bitmicro']
ltc_books = db[symbol+'_books']


def getutctime():
    return time.mktime(datetime.datetime.utcnow().timetuple())


def format_book_entry(entry):
    if all(key in entry for key in ('size', 'price')):
        entry['amount'] = float(entry.pop('size'))
        entry['price'] = float(entry['price'])
    #print(entry)
    return entry


def get_json():
    #resp = urllib2.urlopen(url)
    resp = api.board(product_code="BTC_JPY")
    return json.loads(json.dumps(resp), object_hook=format_book_entry)


print('Running...')
while True:
    start = getutctime()
    book = get_json()
    book['_id'] = getutctime()
    print(getutctime())
    print(len(book["bids"]))
    book["bids"] = book["bids"][:10]
    book["asks"] = book["asks"][:10]
    book.pop('mid_price')
    print(book["bids"])
#    print(book)
    ltc_books.update_one({'_id': book['_id']},
                              {'$setOnInsert': book}, upsert=True)
    time_delta = getutctime()-start
#    print(book)
    if time_delta < 5.0:
        time.sleep(5-time_delta)
