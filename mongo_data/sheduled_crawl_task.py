#  -*- coding: utf-8 -*-

import schedule
from daily_crawler import DailyCrawler
from daily_fixing import DailyFixing
from basic_crawler import BasicCrawler
from database import pro
import time
from datetime import datetime

"""
每天下午15:30执行抓取，只有周一到周五才真正执行抓取任务
"""

def crawl_daily():
    now_date = datetime.now()
    weekday = now_date.strftime('%w')
    if 0 < weekday < 6:
        now = now_date.strftime('%Y%m%d')
        dc = DailyCrawler(pro, begin_date=now, end_date=now)
        dc.crawl_index_new()
        dc.crawl_new()

def fixing_daily():
    now_date = datetime.now()
    weekday = now_date.strftime('%w')
    if 0 < weekday < 6:
        now = now_date.strftime('%Y%m%d')
        df = DailyFixing()
        df.fill_daily_k_at_suspension_days(begin_date=now, end_date=now)

def crawl_basic():
    now_date = datetime.now()
    weekday = now_date.strftime('%w')
    if 0 < weekday < 6:
        now = now_date.strftime('%Y%m%d')
        bc = BasicCrawler(pro, begin_date=now, end_date=now)
        bc.crawl_basic()

if __name__ == '__main__':
    schedule.every().day.at("15:30").do(crawl_daily)
    schedule.every().day.at("16:30").do(fixing_daily)
    while True:
        schedule.run_pending()
        time.sleep(10)
