#  -*- coding: utf-8 -*-

from pymongo import UpdateOne
from database import DB_CONN
import tushare as ts
from datetime import datetime
from graph_database import GRAPH_DB_CONN
from bson.objectid import ObjectId

"""
从tushare获取日K数据，保存到本地的MongoDB数据库中，数据的objectid会存到对应股票的图neo4j数据库里
"""


class DailyCrawler:
    def __init__(self, pro, begin_date=None, end_date=None):
        self.pro = pro
        self.daily_qfq = DB_CONN['daily_qfq']
        self.daily = DB_CONN['daily']
        self.daily_hfq = DB_CONN['daily_hfq']
        self.g = GRAPH_DB_CONN
        self.begin_date = begin_date
        self.end_date = end_date

    def crawl_index(self):
        """
        抓取指数的日线数据，并保存到本地数据数据库中
        抓取的日期范围从2008-01-01至今
        """
        index_codes = ['000001', '000300', '399001', '399005', '399006']

        # 设置默认的日期范围
        if self.begin_date is None:
            self.begin_date = '2008-01-01'

        if self.end_date is None:
            self.end_date = datetime.now().strftime('%Y-%m-%d')

        for code in index_codes:
            df_daily = ts.get_k_data(code, index=True, start=self.begin_date, end=self.end_date)
            self.save_data(code, df_daily, self.daily, {'index': True})

    def crawl_index_new(self):
        """
        抓取指数的日线数据，并保存到本地数据数据库中
        抓取的日期范围从2008-01-01至今
        """
        index_codes = ['000001.SH']

        # 设置默认的日期范围
        if self.begin_date is None:
            self.begin_date = '20080101'

        if self.end_date is None:
            self.end_date = datetime.now().strftime('%Y%m%d')

        for code in index_codes:
            df_daily = self.pro.index_daily(ts_code=code, start_date=self.begin_date, end_date=self.end_date)
            print(df_daily)
            self.save_data(code, df_daily, self.daily, {'index': True})

    def save_data(self, code, df_daily, collection, extra_fields=None):
        """
        将从网上抓取的数据保存到本地MongoDB中

        :param code: 股票代码
        :param df_daily: 包含日线数据的DataFrame
        :param collection: 要保存的数据集
        :param extra_fields: 除了K线数据中保存的字段，需要额外保存的字段
        """
        print(collection)
        update_requests = []
        for df_index in df_daily.index:
            daily_obj = df_daily.loc[df_index]
            doc = self.daily_obj_2_doc(code, daily_obj)

            if extra_fields is not None:
                doc.update(extra_fields)

            update_requests.append(
                UpdateOne(
                    {'code': doc['code'], 'date': doc['date'], 'index': doc['index']},
                    {'$set': doc},
                    upsert=True)
            )
        print(update_requests)

        # 批量写入，提高访问效率
        if len(update_requests) > 0:
            update_result = collection.bulk_write(update_requests, ordered=False)
            print(update_result.upserted_ids.get(0))
            print('保存日线数据，代码： %s, 插入：%4d条, 更新：%4d条' %
                  (code, update_result.upserted_count, update_result.modified_count),
                  flush=True)

    def save_data_new(self, code, df_daily, collection, extra_fields=None, adj='bfq'):
        """
        将从网上抓取的数据保存到本地MongoDB中

        :param code: 股票代码
        :param df_daily: 包含日线数据的DataFrame
        :param collection: 要保存的数据集
        :param extra_fields: 除了K线数据中保存的字段，需要额外保存的字段
        """
        update_requests = []
        date_list = list(df_daily['trade_date'])
        for df_index in df_daily.index:
            daily_obj = df_daily.loc[df_index] #取行数据
            doc = self.daily_obj_2_doc_new(code, daily_obj)

            trade_date = doc['date']

            df_daily_basic = self.pro.daily_basic(ts_code=code, trade_date=trade_date, fields='close,turnover_rate,turnover_rate_f,volume_ratio,pe,'
                                                                                'pe_ttm,pb,ps,ps_ttm,total_share,float_share,'
                                                                               'free_share,total_mv,circ_mv')
            for df_index in df_daily_basic.index:
                daliy_basic_obj = df_daily_basic.loc[df_index]
                doc_basic =self.daily_basic_obj_2_doc_new(daliy_basic_obj)
            if extra_fields is not None:
                doc.update(extra_fields)

            update_requests.append(
                UpdateOne(
                    {'code': doc['code'], 'date': doc['date'], 'index': doc['index']}, #时间近的先插入
                    {'$set': dict(doc, **doc_basic)},
                    upsert=True)
            )
            print("This is update_requests:")
            print(update_requests)

        # 批量写入，提高访问效率
        if len(update_requests) > 0:
            update_result = collection.bulk_write(update_requests, ordered=False)
            print('保存日线数据，代码： %s, 插入：%4d条, 更新：%4d条' %
                  (code, update_result.upserted_count, update_result.modified_count),
                  flush=True)
            print(update_result.upserted_count)
            if update_result.upserted_count > 0:
                if adj == 'bfq':
                    mongo_id = 'mongo_bfq'
                elif adj == 'qfq':
                    mongo_id = 'mongo_qfq'
                elif adj == 'hfq':
                    mongo_id = 'mongo_hfq'
                for i in range(update_result.upserted_count):
                    stock_node = "Stock" + date_list[i]
                    query = "match(p:%s) where p.stock_code='%s' set p.%s = '%s'" % (stock_node, code, mongo_id, update_result.upserted_ids.get(i))
                    print(query)
                    try:
                        self.g.run(query)
                        print(update_result.upserted_ids.get(i))
                        print(code, stock_node)
                    except Exception as e:
                        print(e)

    def crawl(self):
        """
        获取所有股票从2008-01-01至今的K线数据（包括后复权和不复权三种），保存到数据库中
        """

        # 获取所有股票代码
        stock_df = ts.get_stock_basics()
        codes = list(stock_df.index)

        # 设置默认的日期范围
        if self.begin_date is None:
            self.begin_date = '2008-01-01'

        if self.end_date is None:
            self.end_date = datetime.now().strftime('%Y-%m-%d')

        for code in codes:
            # 抓取不复权的价格
            df_daily = ts.get_k_data(code, autype=None, start=self.begin_date, end=self.end_date)
            self.save_data(code, df_daily, self.daily, {'index': False})

            # 抓取后复权的价格
            df_daily_hfq = ts.get_k_data(code, autype='hfq', start=self.begin_date, end=self.end_date)
            self.save_data(code, df_daily_hfq, self.daily_hfq, {'index': False})

    def crawl_new(self):
        """
        获取所有股票从2008-01-01至今的K线数据（包括前、后复权和不复权三种），保存到数据库中
        """

        # 获取所有股票代码
        stock_df = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code')
        codes = list(stock_df['ts_code'])

        # 设置默认的日期范围
        if self.begin_date is None:
            self.begin_date = '20080101'

        if self.end_date is None:
            self.end_date = datetime.now().strftime('%Y%m%d')

        for code in codes:
            # 抓取前复权的价格
            df_daily_qfq = ts.pro_bar(pro_api=self.pro, ts_code=code, adj='qfq', start_date=self.begin_date, end_date=self.end_date, ma=[3,5,7,20,30,60,120,180,360])
            print(df_daily_qfq)
            self.save_data_new(code, df_daily_qfq, self.daily_qfq, {'index': False}, adj='qfq')

            # 抓取不复权的价格
            #df_daily = ts.pro_bar(pro_api=self.pro, ts_code=code, adj=None, start_date=begin_date, end_date=end_date)
            df_daily = ts.pro_bar(pro_api=self.pro, ts_code=code, adj=None, start_date=self.begin_date, end_date=self.end_date, ma=[3,5,7,20,30,60,120,180,360])
            print("This is df_daily:")
            print(df_daily)
            self.save_data_new(code, df_daily, self.daily, {'index': False}, adj='bfq')

            # 抓取后复权的价格
            df_daily_hfq = ts.pro_bar(pro_api=self.pro, ts_code=code, adj='hfq', start_date=self.begin_date, end_date=self.end_date, ma=[3,5,7,20,30,60,120,180,360])
            print(df_daily_hfq)
            self.save_data_new(code, df_daily_hfq, self.daily_hfq, {'index': False}, adj='hfq')

    @staticmethod
    def daily_obj_2_doc(code, daily_obj):
        return {
            'code': code,
            'date': daily_obj['trade_date'],
            'open': daily_obj['open'],
            'close': daily_obj['close'],
            'high': daily_obj['high'],
            'low': daily_obj['low'],
            'vol': daily_obj['vol'], #成交量（手）
            'change': daily_obj['change'], #涨跌额
            'pre_close': daily_obj['pre_close'],
            'pct_chg': daily_obj['pct_chg'], #涨跌幅（%）
            'amount': daily_obj['amount'] #成交额（千元）
        }

    @staticmethod
    def daily_obj_2_doc_new(code, daily_obj):
        return {
            'code': code,
            'date': daily_obj['trade_date'],
            'open': daily_obj['open'],
            'close': daily_obj['close'],
            'high': daily_obj['high'],
            'low': daily_obj['low'],
            'vol': daily_obj['vol'], #成交量（手）
            'change': daily_obj['change'], #涨跌额
            'pre_close': daily_obj['pre_close'],
            'pct_chg': daily_obj['pct_chg'], #涨跌幅（%）
            'amount': daily_obj['amount'], #成交额（千元）
            'ma3': daily_obj['ma3'], #3日均线
            'ma_v_3': daily_obj['ma_v_3'], #3日均成交量
            'ma5': daily_obj['ma5'],
            'ma_v_5': daily_obj['ma_v_5'],
            'ma7': daily_obj['ma7'],
            'ma_v_7': daily_obj['ma_v_7'],
            'ma20': daily_obj['ma20'],
            'ma_v_20': daily_obj['ma_v_20'],
            'ma30': daily_obj['ma30'],
            'ma_v_30': daily_obj['ma_v_30'],
            'ma60': daily_obj['ma60'],
            'ma_v_60': daily_obj['ma_v_60'],
            'ma120': daily_obj['ma120'],
            'ma_v_120': daily_obj['ma_v_120'],
            'ma180': daily_obj['ma180'],
            'ma_v_180': daily_obj['ma_v_180'],
            'ma360': daily_obj['ma360'],
            'ma_v_360': daily_obj['ma_v_360']
        }

    @staticmethod
    def daily_basic_obj_2_doc_new(daily_basic_obj):
        return {
            'close_bfq': daily_basic_obj['close'],
            'turnover_rate': daily_basic_obj['turnover_rate'],
            'turnover_rate_f': daily_basic_obj['turnover_rate_f'],
            'volume_ratio': daily_basic_obj['volume_ratio'],
            'pe': daily_basic_obj['pe'],
            'pe_ttm': daily_basic_obj['pe_ttm'],
            'pb': daily_basic_obj['pb'],
            'ps': daily_basic_obj['ps'],
            'ps_ttm': daily_basic_obj['ps_ttm'], #涨跌幅
            'total_share': daily_basic_obj['total_share'],
            'float_share': daily_basic_obj['float_share'],
            'free_share': daily_basic_obj['free_share'],
            'total_mv': daily_basic_obj['total_mv'],
            'circ_mv': daily_basic_obj['circ_mv']
        }


if __name__ == '__main__':
    pro = ts.pro_api('***')
    begin_date = '20190129'
    end_date = '20190101'
    dc = DailyCrawler(pro, begin_date, end_date)
    dc.crawl_index_new()  # 此处为了获取后续交易日信息
    #dc.crawl('2019-01-11', '2019-01-11')
    dc.crawl_new()
