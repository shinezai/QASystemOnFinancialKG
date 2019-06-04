#  -*- coding: utf-8 -*-

"""
实现股票池，条件是0 < PE <30， 按照PE正序排列，最多取100只票；
再平衡周期为7个交易日
主要的方法包括：
stock_pool：找到两个日期之间所有出的票
find_out_stocks：找到当前被调出的股票
evaluate_stock_pool：对股票池的性能做初步验证，确保股票池能够带来Alpha
"""

from pymongo import ASCENDING
import pandas as pd
import matplotlib.pyplot as plt
from database import DB_CONN
from stock_util import get_trading_dates

daily = DB_CONN['daily']
daily_hfq = DB_CONN['daily_hfq']


def stock_pool(begin_date, end_date):
    """
    股票池
    :param begin_date: 开始日期
    :param end_date: 结束日期
    :return: tuple，所有调整日，以及调整日和代码列表对应的dict
    """

    adjust_date_codes_dict = dict()

    # 获取指定时间区间的所有交易日
    all_dates =  get_trading_dates(begin_date=begin_date, end_date=end_date)

    # 上一期的所有股票代码
    last_phase_codes = []
    # 调整周期是20个交易日
    adjust_interval = 7
    # 所有的调整日
    all_adjust_dates = []
    # 在调整日调整股票池
    for _index in range(0, len(all_dates), adjust_interval):
        # 保存调整日
        adjust_date = all_dates[_index]
        all_adjust_dates.append(adjust_date)

        print('调整日期： %s' % adjust_date, flush=True)

        # 查询出调整当日，0 < pe_ttm < 30，且非停牌的股票
        # 最重要的一点是，按照pe_ttm正序排列，只取前100只
        daily_cursor = daily.find(
            {'date': adjust_date, 'pe': {'$lt': 30, '$gt': 0},
             'is_trading': True},
            sort=[('pe', ASCENDING)],
            projection={'code': True},
            limit=100
        )

        codes = [x['code'] for x in daily_cursor]

        # 本期股票列表
        this_phase_codes = []

        # 查询出上次股票池中正在停牌的股票
        if len(last_phase_codes) > 0:
            suspension_cursor = daily.find(
                {'code': {'$in': last_phase_codes}, 'date': adjust_date, 'is_trading': False},
                projection={'code': True}
            )
            suspension_codes = [x['code'] for x in suspension_cursor]

            # 保留股票池中正在停牌的股票
            this_phase_codes = suspension_codes

        print('上期停牌', flush=True)
        print(this_phase_codes, flush=True)

        # 用新的股票将剩余位置补齐
        this_phase_codes += codes[0: 100 - len(this_phase_codes)]
        # 将本次股票设为下次运行的时的上次股票池
        last_phase_codes = this_phase_codes

        # 建立该调整日和股票列表的对应关系
        adjust_date_codes_dict[adjust_date] = this_phase_codes

        print('最终出票', flush=True)
        print(this_phase_codes, flush=True)

    # 返回结果
    return all_adjust_dates, adjust_date_codes_dict


def find_out_stocks(last_phase_codes, this_phase_codes):
    """
    找到上期入选本期被调出的股票，这些股票将必须卖出
    :param last_phase_codes: 上期的股票列表
    :param this_phase_codes: 本期的股票列表
    :return: 被调出的股票列表
    """
    out_stocks = []

    for code in last_phase_codes:
        if code not in this_phase_codes:
            out_stocks.append(code)

    return out_stocks


def evaluate_stock_pool():
    """
    对股票池做一个简单的评价
    """
    # 设定评测周期
    adjust_dates, codes_dict = stock_pool('2015-01-01', '2015-12-31')

    # 用DataFrame保存收益
    df_profit = pd.DataFrame(columns=['profit', 'hs300'])

    df_profit.loc[adjust_dates[0]] = {'profit': 0, 'hs300': 0}

    hs300_begin_value = daily.find_one({'code': '000300', 'index': True, 'date': adjust_dates[0]})['close']

    # 通过净值计算累计收益
    net_value = 1
    for _index in range(1, len(adjust_dates) - 1):
        last_adjust_date = adjust_dates[_index - 1]
        current_adjust_date = adjust_dates[_index]
        # 获取上一期的股票池
        codes = codes_dict[last_adjust_date]

        # 构建股票代码和后复权买入价格的股票
        code_buy_close_dict = dict()
        buy_daily_cursor = daily_hfq.find(
            {'code': {'$in': codes}, 'date': last_adjust_date},
            projection={'close': True, 'code': True}
        )

        for buy_daily in buy_daily_cursor:
            code = buy_daily['code']
            code_buy_close_dict[code] = buy_daily['close']

        # 获取到期的股价
        sell_daily_cursor = daily_hfq.find(
            {'code': {'$in': codes}, 'date': current_adjust_date},
            projection={'close': True, 'code': True}
        )

        # 计算单期收益
        profit_sum = 0
        count = 0
        for sell_daily in sell_daily_cursor:
            code = sell_daily['code']

            if code in code_buy_close_dict:
                buy_close = code_buy_close_dict[code]
                sell_close = sell_daily['close']

                profit_sum += (sell_close - buy_close) / buy_close

                count += 1

        if count > 0:
            profit = round(profit_sum / count, 4)

            hs300_close = daily.find_one({'code': '000300', 'index': True, 'date': current_adjust_date})['close']

            # 计算净值和累积收益
            net_value = net_value * (1 + profit)
            df_profit.loc[current_adjust_date] = {
                'profit': round((net_value - 1) * 100, 4),
                'hs300': round((hs300_close - hs300_begin_value) * 100 / hs300_begin_value, 4)}

    # 绘制曲线
    df_profit.plot(title='Stock Pool Evaluation Result', kind='line')
    # 显示图像
    plt.show()


if __name__ == "__main__":
    evaluate_stock_pool()
