# -*- coding: utf-8 -*-

import requests
import datetime
import csv
import re
import ast

domain = "***"
port = "***"
appId = "***"
secretId = "***"
authUrl = "http://" + domain + ":" + port + "/auth/v1/token?appid=" + appId + "&secret=" + secretId
queryUrl = "http://" + domain + ":" + port + "/nlp/v1/dataquery"
stockQuery = "\u6240\u6709\u80A1\u7968,\u6982\u5FF5,\u884C\u4E1A,#PDATE#\u80A1\u672C\u89C4\u6A21,\u5E02\u573A," \
                "#PDATE#\u5F00\u76D8\u4EF7,#PDATE#\u6536\u76D8\u4EF7,#PDATE#\u6DA8\u5E45,\u533A\u95F4\u65E5\u5747\u632F\u5E45[#FROMDATE#-#PDATE#]," \
                "\u533A\u95F4\u632F\u5E45[#FROMDATE#-#PDATE#],\u6240\u5C5E\u6307\u6570,#PDATE#\u6700\u4F4E\u4EF7,#PDATE#\u6700\u9AD8\u4EF7," \
                "#PDATE#\u6982\u5FF5\u9F99\u5934,#PDATE#\u6280\u672F\u5F62\u6001,#PDATE#\u9009\u80A1\u52A8\u5411,#PDATE#\u4E70\u5165\u4FE1\u53F7," \
                "#PDATE#\u5356\u51FA\u4FE1\u53F7,#PDATE#\u8BCA\u80A1\u5F97\u5206"
# 所有股票,概念,行业,#PDATE#股本规模,市场,
# #PDATE#开盘价,#PDATE#收盘价,#PDATE#涨幅,区间日均振幅[#FROMDATE#-#PDATE#],
# 区间振幅[#FROMDATE#-#PDATE#],所属指数,#PDATE#最低价,#PDATE#最高价,
# #PDATE#概念龙头,#PDATE#技术形态,#PDATE#选股动向,#PDATE#买入信号,
# #PDATE#卖出信号,#PDATE#诊股得分

indexQuery = "\u6307\u6570\u4EE3\u7801,\u6307\u6570\u7B80\u79F0,#PDATE#\u6536\u76D8\u4EF7,#PDATE#\u6DA8\u8DCC\u5E45,#PDATE#\u5F00\u76D8\u4EF7"
daysAmount = 7

stockQueryAll = "\u6240\u6709\u80a1\u7968,\u6982\u5ff5,\u884c\u4e1a,\u6240\u5c5e\u6307\u6570,\u80a1\u672c\u89c4\u6a21," \
                "\u5e02\u573a,\u6280\u672f\u5f62\u6001,\u9009\u80a1\u52a8\u5411,\u4e70\u5165\u4fe1\u53f7," \
                "\u5356\u51fa\u4fe1\u53f7,\u5f00\u76d8\u4ef7,\u6536\u76d8\u4ef7,\u6700\u9ad8\u4ef7,\u6700\u4f4e\u4ef7," \
                "\u91cf\u6bd4,\u59d4\u6bd4,\u6982\u5ff5\u9f99\u5934,\u6bcf\u80a1\u6536\u76ca," \
                "\u6bcf\u80a1\u51c0\u8d44\u4ea7,\u6362\u624b,\u80a1\u606f\u7387,\u6210\u4ea4\u91cf,\u6210\u4ea4\u989d," \
                "\u632f\u5e45,\u6da8\u5e45,\u80a1\u672c,\u6d41\u901a\u80a1\u672c,\u603b\u5e02\u503c," \
                "\u6d41\u901a\u5e02\u503c,\u6eda\u52a8\u5e02\u76c8\u7387,\u52a8\u6001\u5e02\u76c8\u7387," \
                "\u9759\u6001\u5e02\u76c8\u7387,\u5e02\u51c0\u7387,\u63a7\u5236\u4eba,\u63a7\u5236\u4eba\u7c7b\u578b," \
                "\u63a7\u5236\u4eba\u6301\u80a1\u6bd4\u4f8b,\u8d44\u91d1\u6d41\u5411,\u5229\u6da6," \
                "\u4ea4\u6613\u72b6\u6001,\u8bca\u80a1\u5f97\u5206,\u4e3b\u8425\u4ea7\u54c1,\u7ecf\u8425\u8303\u56f4," \
                "\u6240\u5728\u7701\u4efd,\u6240\u5728\u57ce\u5e02"

query = "\u4e70\u5165\u5e73\u5b89\u94f6\u884c"

# 所有股票,概念,行业,所属指数,股本规模,市场,技术形态,选股动向,买入信号,卖出信号,开盘价,收盘价,最高价,最低价,量比,委比,概念龙头,
# 每股收益,每股净资产,换手,股息率,成交量,成交额,振幅,涨幅,股本,流通股本,总市值,流通市值,滚动市盈率,动态市盈率,静态市盈率,市净率,
# 控制人,控制人类型,控制人持股比例,资金流向,利润,交易状态,诊股得分,主营产品,经营范围,所在省份,所在城市

stockQueryAllWithPDATE = "\u6240\u6709\u80a1\u7968,\u6982\u5ff5,\u884c\u4e1a,\u6240\u5c5e\u6307\u6570,#PDATE#\u80a1\u672c\u89c4\u6a21,"\
                         "\u5e02\u573a,#PDATE#\u6280\u672f\u5f62\u6001,#PDATE#\u9009\u80a1\u52a8\u5411,#PDATE#\u4e70\u5165\u4fe1\u53f7," \
                         "#PDATE#\u5356\u51fa\u4fe1\u53f7,#PDATE#\u5f00\u76d8\u4ef7,#PDATE#\u6536\u76d8\u4ef7,#PDATE#\u6700\u9ad8\u4ef7,#PDATE#\u6700\u4f4e\u4ef7," \
                         "#PDATE#\u91cf\u6bd4,#PDATE#\u59d4\u6bd4,\u6982\u5ff5\u9f99\u5934,#PDATE#\u6bcf\u80a1\u6536\u76ca," \
                         "#PDATE#\u6bcf\u80a1\u51c0\u8d44\u4ea7,#PDATE#\u6362\u624b,#PDATE#\u80a1\u606f\u7387,#PDATE#\u6210\u4ea4\u91cf,#PDATE#\u6210\u4ea4\u989d," \
                         "#PDATE#\u632f\u5e45,\u6da8\u5e45,#PDATE#\u80a1\u672c,#PDATE#\u6d41\u901a\u80a1\u672c,#PDATE#\u603b\u5e02\u503c," \
                         "#PDATE#\u6d41\u901a\u5e02\u503c,#PDATE#\u6eda\u52a8\u5e02\u76c8\u7387,#PDATE#\u52a8\u6001\u5e02\u76c8\u7387," \
                         "#PDATE#\u9759\u6001\u5e02\u76c8\u7387,#PDATE#\u5e02\u51c0\u7387,#PDATE#\u63a7\u5236\u4eba,#PDATE#\u63a7\u5236\u4eba\u7c7b\u578b," \
                         "#PDATE#\u63a7\u5236\u4eba\u6301\u80a1\u6bd4\u4f8b,#PDATE#\u8d44\u91d1\u6d41\u5411,#PDATE#\u5229\u6da6," \
                         "#PDATE#\u4ea4\u6613\u72b6\u6001,#PDATE#\u8bca\u80a1\u5f97\u5206,\u4e3b\u8425\u4ea7\u54c1,\u7ecf\u8425\u8303\u56f4," \
                         "\u6240\u5728\u7701\u4efd,\u6240\u5728\u57ce\u5e02"

# 所有股票,概念,行业,所属指数,股本规模,
# 市场,技术形态,选股动向,买入信号,
# 卖出信号,开盘价,收盘价,最高价,最低价,
# 量比,委比,概念龙头,每股收益,
# 每股净资产,换手,股息率,成交量,成交额,
# 振幅,涨幅,股本,流通股本,总市值,
# 流通市值,滚动市盈率,动态市盈率,
# 静态市盈率,市净率,控制人,控制人类型,
# 控制人持股比例,资金流向,利润,
# 交易状态,诊股得分,主营产品,经营范围,
# 所在省份,所在城市

topmanagerQuery = "\u6240\u6709\u80a1\u7968,\u9ad8\u7ba1,\u9ad8\u7ba1\u6bd5\u4e1a\u5b66\u6821"

yizhen = "\u6613\u5ce5"

class StockSpider():
    def __init__(self, pdate, path, path_astock, path_topmanager):
        self.pdate = pdate
        self.path = path
        self.path_astock = path_astock
        self.path_topmanager = path_topmanager
        self.key_dict = {
            '股票代码': 'stock_code',
            '股票简称': 'stock_name',
            '所属概念': 'concept',
            '所属同花顺行业': 'industry_ths',
            '所属行业': 'industry',  # 需要提取
            '所属指数类': 'index_type',
            '股本规模': 'equity_scale',
            '股票市场类型': 'market_type',
            '技术形态': 'tech_form',
            '选股动向': 'movement',
            '买入信号inter': 'buy_signal',
            '卖出信号inter': 'sell_signal',
            '开盘价:不复权': 'open',
            '收盘价:不复权': 'close',
            '最高价:不复权': 'high',
            '最低价:不复权': 'low',
            '量比': 'volume_rate',
            '委比': 'commit_rate',
            '概念龙头': 'concept_leading',
            '基本每股收益': 'eps',
            '每股净资产bps': 'bps',
            '换手率': 'turnover_rate',
            '股息率(股票获利率)': 'dividend_rate',
            '成交量': 'trading_volume',
            '成交额': 'trading_amount',
            '振幅': 'amplitude',
            '涨跌幅:前复权': 'wave', #这个地方要改下
            '总股本': 'capital_total',
            '流通a股': 'capital_flow',
            '总市值': 'value_total',
            'a股市值(不含限售股)': 'value_flow',
            '市盈率(pe,ttm)': 'pe_ttm',
            '市盈率(pe)': 'pe',
            '静态市盈率(中证发布)': 'pe_lyr',
            '市净率(pb)': 'pb',
            '实际控制人': 'controller',
            '变更前实控人': 'before_controller',
            '变更后实控人': 'controller',
            '实控人变更公告日期': 'controller_change_announce_date',
            '实控人变更截止日期': 'controller_change_deadline',
            '实际控制人类型': 'controller_type',
            '实控人变更次数': 'controller_change_times',
            '实际控制人持股比例': 'controller_hold_ratio',
            '资金流向': 'fund_flow',
            '归属于母公司所有者的净利润': 'retained_profits',
            '交易状态': 'trading_status',
            '牛叉诊股综合评分': 'score',
            '高管姓名': 'tm_name',
            '高管毕业学校': 'tm_school',
            '高管职务': 'tm_title',
            '高管薪酬': 'tm_salary',
            '高管性别': 'tm_gender',
            '高管国籍': 'tm_nationality',
            '高管学历': 'tm_educationbg',
            '主营产品名称': 'main_business',
            '经营范围': 'business_scope',
            '省份': 'province',
            '城市': 'city'}

    def write_csv(self, json_list):
        csvfile = open(path+'.csv', 'w', newline='')
        writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_ALL)
        flag = True
        for list_item in json_list:
            #print(list_item)
            dic = list_item
            if flag:
                keys = list(dic.keys())
               # print(keys)
                writer.writerow(keys)
                flag = False
            writer.writerow(list(dic.values()))
        csvfile.close()

    def write_json(self, path_suffix, json_list):
        jsonfile = open(self.path + path_suffix + '.json', 'wt', encoding='utf-8')

        for list_item in json_list:
            json_modify = {}
            str_dic = re.sub(r'\[\d{8}\]\':', "':", str(list_item)) #将keys中的日期去掉
            list_item = ast.literal_eval(str_dic) #变为json
            print(list_item)
            for attr, value in list_item.items():
                if value is None:
                    continue
                attr_en = self.key_dict[attr]
                if attr_en:
                    json_modify[attr_en] = value
                if attr_en in ['industry_ths']:
                    json_modify['industry'] = [value.split('-')[1]]
                if attr_en in ['concept', 'index_type', 'market_type', ]:
                    json_modify[attr_en] = value.split(';')
                if attr_en in ['concept_leading', 'tech_form', 'movement', 'buy_signal', 'sell_signal', 'tm_school', 'main_business']:
                    json_modify[attr_en] = value.split('||')
                if attr_en in ['equity_scale']:
                    json_modify[attr_en] = [value]
                if attr_en in ['controller', 'controller_type', 'tm_title']:
                    list_temp = []
                    for v in value.split(','):
                        list_temp.append(v.strip())
                    json_modify[attr_en] = list_temp
                if attr_en in ['main_business']:
                    #json_modify[attr_en] = value.replace('"', r'\"').split('||')
                    json_modify[attr_en] = value.split('||')
                if attr_en in ['business_scope']:
                    json_modify[attr_en] = value
            print(json_modify)
            jsonfile.write(str(json_modify)+'\n')
        jsonfile.close()

    def query(self, question, auth, domainStr, path_suffix):
        url = queryUrl + "?query=" + question
        url = url + "&domain=" + domainStr
        params = {"Access-Token": auth}
        r = s.get(url, headers=params)
        #print(r.json()["datas"])
        #r = requests.get(url, headers=params)
        if r.status_code == 404:
            raise Exception("404 Error!")
        elif r.status_code == 200:
            entity = r.json()["datas"]
            #self.write_csv(entity)
            self.write_json(path_suffix, entity)

    def update(self):

        auth_token = self.get_auth_token()
        print(auth_token)
        pdatetime = datetime.datetime.strptime(self.pdate, '%Y%m%d')
        ndaysago = (pdatetime - datetime.timedelta(days=daysAmount)).strftime('%Y%m%d')
        stock_query_filled = stockQueryAll.replace("#PDATE#", self.pdate).replace("#FROMDATE#", ndaysago)
        #stock_query_filled = query
        print(stock_query_filled + "\n")
        self.query(stock_query_filled, auth_token, "abs_股票领域", self.path_astock)
        self.query(topmanagerQuery, auth_token, "abs_股票领域", self.path_topmanager) #add top manager

    def get_auth_token(self):
        r = s.get(authUrl)
        #r = requests.get(authUrl)
        if r.status_code == 403:
            raise Exception("API token invalid")
        elif r.status_code == 401:
            raise Exception("API token missing")
        elif r.status_code == 413:
            raise Exception("The image is too large, please reduce the image size to below 1MB")
        elif r.status_code == 429:
            raise Exception("You have exceeded your quota. Please wait and try again soon.")
        elif r.status_code == 200:
            access_token = self.extract_access_token(r.json())
        return access_token

    def extract_access_token(self, entity):
        access_token = entity['access_token']
        return access_token

def gen_dates(b_date, days):
    print(days)
    day = datetime.timedelta(days=1)
    for i in range(days):
        yield b_date + day * i

if __name__ == "__main__":
    startDate = "20190121"
    endDate = "20190121"

    data = []
    start = datetime.datetime.strptime(startDate, "%Y%m%d").date()
   # print(start)
    end = datetime.datetime.strptime(endDate, "%Y%m%d").date()
   # print(end)

    for d in gen_dates(start, (end - start).days):
        d = d.strftime("%Y%m%d")
        data.append(d)
    data.append(endDate)
    s = requests.Session()
    a = requests.adapters.HTTPAdapter(pool_connections=11, pool_maxsize=11) #connection pool
    s.mount('http://' + domain + port, a)

    for day in data:
        path = "D:/QASystemOnFinancialKG/data/"
        path_astock = "astock#pDate#"
        path_topmanager = "topmanager#pDate#"
        path_astock = path_astock.replace("#pDate#", day)
        path_topmanager = path_topmanager.replace("#pDate#", day)
        print(day)
        print(path)
        print(path_topmanager)
        stock = StockSpider(day, path, path_astock, path_topmanager)
        stock.update()



    s.close()



