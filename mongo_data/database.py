from pymongo import MongoClient, ASCENDING, DESCENDING, UpdateOne
import tushare as ts

DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['quant_01']

pro = ts.pro_api('***')
