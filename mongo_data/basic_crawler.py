#  -*- coding: utf-8 -*-

from datetime import datetime, timedelta

import tushare as ts
from pymongo import UpdateOne

from database import DB_CONN
from stock_util import get_trading_dates

"""
从tushare获取股票基础数据，保存到本地的MongoDB数据库中, 新版接口已整合，不需要此文件
"""

class BasicCrawler:
    def __init__(self, pro, begin_date=None, end_date=None):
        self.pro = pro
        self.begin_date = begin_date
        self.end_date = end_date

    def crawl_basic(self):
        """
        抓取指定时间范围内的股票基础信息
        :param begin_date: 开始日期
        :param end_date: 结束日期
        """

        if self.begin_date is None:
            self.begin_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

        if self.end_date is None:
            self.end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

        all_dates = get_trading_dates(self.begin_date, self.end_date)

        for date in all_dates:
            try:
                self.crawl_basic_at_date(date)
            except:
                print('抓取股票基本信息时出错，日期：%s' % date, flush=True)


    def crawl_basic_at_date(self, date):
        """
        从Tushare抓取指定日期的股票基本信息
        :param date: 日期
        """
        # 默认推送上一个交易日的数据
        df_basics = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code, symbol, name, area, industry, fullname,'
                                                                              'enname, market, exchange, curr_type, list_status,'
                                                                              'list_date, delist_date, is_hs')
        #df_basics = ts.get_stock_basics(date)

        # 如果当日没有基础信息，在不做操作
        if df_basics is None:
            return

        print(df_basics)

        update_requests = []
        indecis = set(df_basics.index)
        for index in indecis:
            doc = dict(df_basics.loc[index])
            try:
                # 将20180101转换为20180101的形式
                #list_date = datetime \
                #    .strptime(str(doc['list_date']), '%Y%m%d').strftime("%Y%m%d")

                code = doc['ts_code']
                doc.update({
                    'code': code,
                    'date': date,
                })

                update_requests.append(
                    UpdateOne(
                        {'code': code, 'date': date},
                        {'$set': doc}, upsert=True))
            except:
                print('发生异常，股票代码：%s，日期：%s' % (code, date), flush=True)
                print(doc, flush=True)

        if len(update_requests) > 0:
            update_result = DB_CONN['basic'].bulk_write(update_requests, ordered=False)

            print('抓取股票基本信息，日期：%s, 插入：%4d条，更新：%4d条' %
                  (date, update_result.upserted_count, update_result.modified_count), flush=True)


if __name__ == '__main__':
    pro = ts.pro_api('***')
    begin_date = '20190101'
    end_date = '20190129'
    bc = BasicCrawler(pro, begin_date, end_date)
    bc.crawl_basic()
