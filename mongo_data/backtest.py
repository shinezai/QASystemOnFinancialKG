"""
完成策略的回测，绘制以沪深300为基准的收益曲线，计算年化收益、最大回撤、夏普比率
主要的方法包括:
is_k_up_break_ma10：当日K线是否上穿10日均线
is_k_down_break_ma10：当日K线是否下穿10日均线
compare_close_2_ma_10：工具方法，某日收盘价和当日对应的10日均线的关系
backtest：回测主逻辑方法，从股票池获取股票后，按照每天的交易日一天天回测
compute_drawdown：计算最大回撤
compute_annual_profit：计算年化收益
compute_sharpe_ratio：计算夏普比率
"""

from pymongo import DESCENDING
import pandas as pd
import matplotlib.pyplot as plt
from stock_pool_strategy import stock_pool, find_out_stocks
from database import DB_CONN
from stock_util import get_trading_dates


def is_k_up_break_ma10(code, _date):
    """
    判断某只股票在某日是否满足K线上穿10日均线

    :param code: 股票代码
    :param _date: 日期
    :return: True/False
    """

    # 如果股票当日停牌或者是下跌，则返回False
    current_daily = DB_CONN['daily_hfq'].find_one(
        {'code': code, 'date': _date, 'is_trading': True})

    if current_daily is None:
        print('计算信号，K线上穿MA10，当日没有K线，股票 %s，日期：%s' % (code, _date), flush=True)
        return False

    # 计算MA10
    daily_cursor = DB_CONN['daily_hfq'].find(
        {'code': code, 'date': {'$lte': _date}},
        sort=[('date', DESCENDING)],
        limit=11,
        projection={'code': True, 'close': True, 'is_trading': True}
    )

    dailies = [x for x in daily_cursor]

    if len(dailies) < 11:
        print('计算信号，K线上穿MA10，前期K线不足，股票 %s，日期：%s' % (code, _date), flush=True)
        return False

    dailies.reverse()

    last_close_2_last_ma10 = compare_close_2_ma_10(dailies[0:10])
    current_close_2_current_ma10 = compare_close_2_ma_10(dailies[1:])

    print('计算信号，K线上穿MA10，股票：%s，日期：%s， 前一日 %s，当日：%s' %
          (code, _date, str(last_close_2_last_ma10), str(current_close_2_current_ma10)), flush=True)

    if last_close_2_last_ma10 is None or current_close_2_current_ma10 is None:
        return False

    # 判断收盘价和MA10的大小
    is_break = (last_close_2_last_ma10 <= 0) & (current_close_2_current_ma10 == 1)

    print('计算信号，K线上穿MA10，股票：%s，日期：%s， 前一日 %s，当日：%s，突破：%s' %
          (code, _date, str(last_close_2_last_ma10), str(current_close_2_current_ma10), str(is_break)), flush=True)

    return is_break


def is_k_down_break_ma10(code, _date):
    """
    判断某只股票在某日是否满足K线下穿10日均线

    :param code: 股票代码
    :param _date: 日期
    :return: True/False
    """

    # 如果股票当日停牌或者是下跌，则返回False
    current_daily = DB_CONN['daily'].find_one(
        {'code': code, 'date': _date, 'is_trading': True})
    if current_daily is None:
        print('计算信号，K线下穿MA10，当日没有K线，股票 %s，日期：%s' % (code, _date), flush=True)
        return False

    # 计算MA10
    daily_cursor = DB_CONN['daily_hfq'].find(
        {'code': code, 'date': {'$lte': _date}},
        sort=[('date', DESCENDING)],
        limit=11,
        projection={'code': True, 'close': True, 'is_trading': True}
    )

    dailies = [x for x in daily_cursor]

    if len(dailies) < 11:
        print('计算信号，K线下穿MA10，前期K线不足，股票 %s，日期：%s' % (code, _date), flush=True)
        return False

    dailies.reverse()

    last_close_2_last_ma10 = compare_close_2_ma_10(dailies[0:10])
    current_close_2_current_ma10 = compare_close_2_ma_10(dailies[1:])

    if last_close_2_last_ma10 is None or current_close_2_current_ma10 is None:
        return False

    # 判断收盘价和MA10的大小
    is_break = (last_close_2_last_ma10 >= 0) & (current_close_2_current_ma10 == -1)

    print('计算信号，K线下穿MA10，股票：%s，日期：%s， 前一日 %s，当日：%s, 突破：%s' %
          (code, _date, str(last_close_2_last_ma10), str(current_close_2_current_ma10), str(is_break)), flush=True)

    return is_break


def compare_close_2_ma_10(dailies):
    """
    比较当前的收盘价和MA10的关系
    :param dailies: 日线列表，10个元素，最后一个是当前交易日
    :return: 0 相等，1 大于， -1 小于, None 结果未知
    """
    current_daily = dailies[9]
    close_sum = 0
    code = None
    for daily in dailies:
        # 10天当中，只要有一天停牌则返回False
        if 'is_trading' not in daily or daily['is_trading'] is False:
            return None

        # 用后复权累计
        close_sum += daily['close']
        code = daily['code']

    # 计算MA10
    ma_10 = close_sum / 10

    # 判断收盘价和MA10的大小
    post_adjusted_close = current_daily['close']
    differ = post_adjusted_close - ma_10

    # print('计算信号，股票： %s, 收盘价：%7.2f, MA10: %7.2f, 差值：%7.2f' %
    #       (code, post_adjusted_close, ma_10, differ), flush=True)
    if differ > 0:
        return 1
    elif differ < 0:
        return -1
    else:
        return 0


def backtest(begin_date, end_date):
    """
    策略回测。结束后打印出收益曲线(沪深300基准)、年化收益、最大回撤、

    :param begin_date: 回测开始日期
    :param end_date: 回测结束日期
    """
    cash = 1E7
    single_position = 2E5

    # 时间为key的净值、收益和同期沪深基准
    df_profit = pd.DataFrame(columns=['net_value', 'profit', 'hs300'])

    all_dates = get_trading_dates(begin_date, end_date)

    hs300_begin_value = DB_CONN['daily'].find_one(
        {'code': '000300', 'index': True, 'date': all_dates[0]},
        projection={'close': True})['close']

    adjust_dates, date_codes_dict = stock_pool(begin_date, end_date)

    last_phase_codes = None
    this_phase_codes = None
    to_be_sold_codes = set()
    to_be_bought_codes = set()
    holding_code_dict = dict()
    last_date = None
    # 按照日期一步步回测
    for _date in all_dates:
        print('Backtest at %s.' % _date)

        # 当期持仓股票列表
        before_sell_holding_codes = list(holding_code_dict.keys())

        # 处理复权
        if last_date is not None and len(before_sell_holding_codes) > 0:
            last_daily_cursor = DB_CONN['daily'].find(
                {'code': {'$in': before_sell_holding_codes}, 'date': last_date, 'index': False},
                projection={'code': True, 'au_factor': True})

            code_last_aufactor_dict = dict()
            for last_daily in last_daily_cursor:
                code_last_aufactor_dict[last_daily['code']] = last_daily['au_factor']

            current_daily_cursor = DB_CONN['daily'].find(
                {'code': {'$in': before_sell_holding_codes}, 'date': _date, 'index': False},
                projection={'code': True, 'au_factor': True})

            for current_daily in current_daily_cursor:
                current_aufactor = current_daily['au_factor']
                code = current_daily['code']
                before_volume = holding_code_dict[code]['volume']
                if code in code_last_aufactor_dict:
                    last_aufactor = code_last_aufactor_dict[code]
                    after_volume = int(before_volume * (current_aufactor / last_aufactor))
                    holding_code_dict[code]['volume'] = after_volume
                    print('持仓量调整：%s, %6d, %10.6f, %6d, %10.6f' %
                          (code, before_volume, last_aufactor, after_volume, current_aufactor))

        # 卖出
        print('待卖股票池：', to_be_sold_codes, flush=True)
        if len(to_be_sold_codes) > 0:
            sell_daily_cursor = DB_CONN['daily'].find(
                {'code': {'$in': list(to_be_sold_codes)}, 'date': _date, 'index': False, 'is_trading': True},
                projection={'open': True, 'code': True}
            )

            for sell_daily in sell_daily_cursor:
                code = sell_daily['code']
                if code in before_sell_holding_codes:
                    holding_stock = holding_code_dict[code]
                    holding_volume = holding_stock['volume']
                    sell_price = sell_daily['open']
                    sell_amount = holding_volume * sell_price
                    cash += sell_amount

                    cost = holding_stock['cost']
                    single_profit = (sell_amount - cost) * 100 / cost
                    print('卖出 %s, %6d, %6.2f, %8.2f, %4.2f' %
                          (code, holding_volume, sell_price, sell_amount, single_profit))

                    del holding_code_dict[code]
                    to_be_sold_codes.remove(code)

        print('卖出后，现金: %10.2f' % cash)

        # 买入
        print('待买股票池：', to_be_bought_codes, flush=True)
        if len(to_be_bought_codes) > 0:
            buy_daily_cursor = DB_CONN['daily'].find(
                {'code': {'$in': list(to_be_bought_codes)}, 'date': _date, 'is_trading': True, 'index': False},
                projection={'code': True, 'open': True}
            )

            for buy_daily in buy_daily_cursor:
                if cash > single_position:
                    buy_price = buy_daily['open']
                    code = buy_daily['code']
                    volume = int(int(single_position / buy_price) / 100) * 100
                    buy_amount = buy_price * volume
                    cash -= buy_amount
                    holding_code_dict[code] = {
                        'volume': volume,
                        'cost': buy_amount,
                        'last_value': buy_amount}

                    print('买入 %s, %6d, %6.2f, %8.2f' % (code, volume, buy_price, buy_amount))

        print('买入后，现金: %10.2f' % cash)

        # 持仓股代码列表
        holding_codes = list(holding_code_dict.keys())
        # 如果调整日，则获取新一期的股票列表
        if _date in adjust_dates:
            print('股票池调整日：%s，备选股票列表：' % _date, flush=True)

            # 暂存为上期的日期
            if this_phase_codes is not None:
                last_phase_codes = this_phase_codes
            this_phase_codes = date_codes_dict[_date]
            print(this_phase_codes, flush=True)

            # 找到所有调出股票代码，在第二日开盘时卖出
            if last_phase_codes is not None:
                out_codes = find_out_stocks(last_phase_codes, this_phase_codes)
                for out_code in out_codes:
                    if out_code in holding_code_dict:
                        to_be_sold_codes.add(out_code)

        # 检查是否有需要第二天卖出的股票
        for holding_code in holding_codes:
            if is_k_down_break_ma10(holding_code, _date):
                to_be_sold_codes.add(holding_code)

        # 检查是否有需要第二天买入的股票
        to_be_bought_codes.clear()
        if this_phase_codes is not None:
            for _code in this_phase_codes:
                if _code not in holding_codes and is_k_up_break_ma10(_code, _date):
                    to_be_bought_codes.add(_code)

        # 计算总资产
        total_value = 0
        holding_daily_cursor = DB_CONN['daily'].find(
            {'code': {'$in': holding_codes}, 'date': _date},
            projection={'close': True, 'code': True}
        )
        for holding_daily in holding_daily_cursor:
            code = holding_daily['code']
            holding_stock = holding_code_dict[code]
            value = holding_daily['close'] * holding_stock['volume']
            total_value += value

            profit = (value - holding_stock['cost']) * 100 / holding_stock['cost']
            one_day_profit = (value - holding_stock['last_value']) * 100 / holding_stock['last_value']

            holding_stock['last_value'] = value
            print('持仓: %s, %10.2f, %4.2f, %4.2f' %
                  (code, value, profit, one_day_profit))

        total_capital = total_value + cash

        hs300_current_value = DB_CONN['daily'].find_one(
            {'code': '000300', 'index': True, 'date': _date},
            projection={'close': True})['close']

        print('收盘后，现金: %10.2f, 总资产: %10.2f' % (cash, total_capital))
        last_date = _date
        df_profit.loc[_date] = {
            'net_value': round(total_capital / 1e7, 2),
            'profit': round(100 * (total_capital - 1e7) / 1e7, 2),
            'hs300': round(100 * (hs300_current_value - hs300_begin_value) / hs300_begin_value, 2)
        }

    print(df_profit)
    drawdown = compute_drawdown(df_profit['net_value'])
    annual_profit, sharpe_ratio = compute_sharpe_ratio(df_profit['net_value'])

    print('回测结果 %s - %s，年化收益： %7.3f, 最大回撤：%7.3f, 夏普比率：%4.2f' %
          (begin_date, end_date, annual_profit, drawdown, sharpe_ratio))

    df_profit.plot(title='Backtest Result', y=['profit', 'hs300'], kind='line')
    plt.show()


def compute_drawdown(net_values):
    """
    计算最大回撤
    :param net_values: 净值列表
    """
    # 最大回撤初始值设为0
    max_drawdown = 0
    size = len(net_values)
    index = 0
    # 双层循环找出最大回撤
    for net_value in net_values:
        for sub_net_value in net_values[index:]:
            drawdown = 1 - sub_net_value / net_value
            if drawdown > max_drawdown:
                max_drawdown = drawdown

        index += 1

    return max_drawdown


def compute_annual_profit(trading_days, net_value):
    """
    计算年化收益
    """

    annual_profit = 0
    if trading_days > 0:
        # 计算年数
        years = trading_days / 245
        # 计算年化收益
        annual_profit = pow(net_value, 1 / years) - 1

    annual_profit = round(annual_profit * 100, 2)

    return annual_profit


def compute_sharpe_ratio(net_values):
    """
    计算夏普比率
    :param net_values: 净值列表
    """

    # 总交易日数
    trading_days = len(net_values)
    # 所有收益的DataFrame
    profit_df = pd.DataFrame(columns={'profit'})
    # 收益之后，初始化为第一天的收益
    profit_df.loc[0] = {'profit': round((net_values[0] - 1) * 100, 2)}
    # 计算每天的收益
    for index in range(1, trading_days):
        # 计算每日的收益变化
        profit = (net_values[index] - net_values[index - 1]) / net_values[index - 1]
        profit = round(profit * 100, 2)
        profit_df.loc[index] = {'profit': profit}

    # 计算当日收益标准差
    profit_std = pow(profit_df.var()['profit'], 1 / 2)

    # 年化收益
    annual_profit = compute_annual_profit(trading_days, net_values[-1])

    # 夏普比率
    sharpe_ratio = (annual_profit - 4.75) / (profit_std * pow(245, 1 / 2))

    return annual_profit, sharpe_ratio


if __name__ == "__main__":
    backtest('2015-01-01', '2015-12-31')
