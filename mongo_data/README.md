mongo_data部分将从tushare库取得数据，为了给***接口获取数据做备份以及与***数据区别开来，mongo的数据的objectid将作为属性存入neo4j股票信息中。

1.运行daily_crawler.py
crawl_index_new部分将获取指定日期范围内大盘指数数据，该部分数据将在后面用作获取所有交易日的源数据（stock_uitl中的get_trading_dates方法依赖于该部分数据），数据存于daily集合
抓取字段有：
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

crawl_new将获取指定日期范围内前复权，不复权，后复权三种数据，数据分别存于daily_qfq/daily/daily_hfq集合
抓取字段有：
            'code': code,
            'date': daily_obj['trade_date'],
            'open': daily_obj['open'],
            'close': daily_obj['close'],
            'high': daily_obj['high'],
            'low': daily_obj['low'],
            'vol': daily_obj['vol'], #成交量（手）
            'change': daily_obj['change'],
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

2.运行basic_crawler.py
crawl_basic会获取股票在交易日的基础信息，数据存于basic集合
抓取字段有：
        'code': #带.SH .SZ股票代码
        'date': #当前日期
        'symbol': #股票代码
        'name': #股票名称
        'area': #所在地域
        'industry': #所属行业
        'fullname': #股票全称
        'enname': #英文全称
        'market': #市场类型（主板/中小板/创业板）
        'exchange': #交易所代码
        'curr_type': #交易货币
        'list_status': #上市状态：L上市 D退市 P暂停上市
        'list_date': #上市日期
        'delist_date': #退市日期
        'is_hs': #是否沪深港通标的， N否 H沪股通 S深股通

3.运行daily_fixing.py
fill_daily_k_at_suspension_days会填充指定日期范围内的是否停牌数据，交易日is_trading=true，非交易日is_trading=false 注：非交易日不包含休息日，目的在于修复daily_qfq/daily/daily_hfq集合
补全非交易日数据：
                            'code': code,
                            'date': date,
                            'close': last_trading_daily['close'],
                            'open': last_trading_daily['close'],
                            'high': last_trading_daily['close'],
                            'low': last_trading_daily['close'],
                            'vol': 0,
                            'change': 0,
                            'pct_chg': 0,
                            'amount': 0,
                            'is_trading': False

4.运行finance_report_crawler.py
crawl_finance_report会从东方财富网抓取股票财报数据存入finance_report集合
爬取的数据字段有：
                'report_date': report['reportdate'][0:10], #季度报告日期
                'announced_date': report['latestnoticedate'][0:10], #公告日期
                'eps': report['basiceps'], #每股收益（元）
                'cuteps': report['cutbasiceps'], #每股收益（扣除）（元）
                'totaloperatereve': report['totaloperatereve'], #营业收入（元）
                'ystz': report['ystz'], #营业收入同比增长（%）
                'yshz': report['yshz'], #营业收入季度环比增长（%）
                'parentnetprofit': report['parentnetprofit'], #净利润（元）
                'sjltz': report['sjltz'], #净利润同比增长（%）
                'sjlhz': report['sjlhz'], #净利润环比增长（%）
                'roeweighted': report['roeweighted'], #净资产收益率（%）
                'bps': report['bps'], #每股净资产（元）
                'code': code

5.运行scheduled_crawl_task.py定时运行