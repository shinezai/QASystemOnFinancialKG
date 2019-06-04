#!/usr/bin/env python3
# coding: utf-8
# File: build_stockgraph.py
# Author: https://github.com/shinezai
# Date: 19-02

import os
import re
import json
from py2neo import Graph, Node
from graph_database import GRAPH_DB_CONN

class StockGraph(object):
    def __init__(self, pDate):
        self.pDate = pDate
        path_astock = 'data/astock#PDATE#.json'.replace('#PDATE#', pDate)
        path_topmanager = 'data/topmanager#PDATE#.json'.replace('#PDATE#', pDate)
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.data_path_astock = os.path.join(cur_dir, path_astock)
        self.data_path_topmanager = os.path.join(cur_dir, path_topmanager)
        self.g = GRAPH_DB_CONN

    '''读取文件'''
    def read_nodes(self):
        #实体
        StockId = [] #股票代码
        StockName = [] #股票名称
        Concept = [] #概念
        ConceptLeading = [] #概念龙头
        Industry = [] #行业
        IndexType = [] #指数
        EquityScale = [] #股本规模
        MarketType = [] #市场类型
        Controller = [] #实际控制人
        ControllerType = [] #实际控制人类型
        TechForm = [] #技术形态
        Movement = [] #选股动向
        BuySignal = [] #买入信号
        SellSignal = [] #卖出信号
        TopManager = [] #高管
        School = [] #学校
        Title = [] #职务
        Gender = [] #性别
        Nationality = [] #国籍
        EducationBg = [] #学历
        MainBusiness = [] #主营产品
        Province = [] #省份
        City = [] #城市

        stock_infos = []
        tm_infos = []

        #构建节点实体关系
        rels_ConceptInvolved = [] #股票-概念
        rels_ConceptLeadingInvolved = [] #股票-概念龙头
        rels_IndustryInvolved = [] #股票-行业
        rels_IndexTypeIs = [] #股票-指数类型
        rels_EquityScaleIs = [] #股票-股本规模
        rels_MarketTypeIs = [] #股票-市场类型
        rels_IsControlledBy = [] #股票-控制人
        rels_TechFormIs = [] #股票-技术形态
        rels_MovementIs = [] #股票-选股动向
        rels_BuySignalIs = [] #股票-买入信号
        rels_SellSignalIs = [] #股票-卖出信号
        rels_TopManagerIs = [] #股票-高管
        rels_SchoolIs = [] #高管-毕业学校
        rels_TitleIs = [] #高管-职务
        rels_GenderIs = [] #高管-性别
        rels_NationalityIs = [] #高管-国籍
        rels_EducationBgIs = [] #高管-学历
        rels_MainBusinessIs = [] #股票-主营产品
        rels_ProvinceIs = [] #股票-所在省份
        rels_CityIs = [] #股票-所在城市

        count = 0
        for data in open(self.data_path_astock, encoding='utf-8'):
            stock_dict = {}
            count += 1
            print(count)
            #data1 = data.replace("'", '"')
            data = json.dumps(eval(data))
            #data = eval("'{}'".format(data))
            #data = re.sub(r'\\', '', data1)
            if count == 520:
                print(data)
            data_json = json.loads(data)
            print(data_json)
            stock = data_json['stock_code'][0:6]
            stock_dict['stock_code'] = data_json['stock_code']
            #stock = data_json['stock_id'] #保留末尾.SZ .SH
            stock_dict['stock_id'] = stock
            StockId.append(stock)
            stock_dict['stock_name'] = ''
            stock_dict['industry_ths'] = ''
            stock_dict['open'] = ''
            stock_dict['close'] = ''
            stock_dict['high'] = ''
            stock_dict['low'] = ''
            stock_dict['volume_rate'] = ''  # 量比
            stock_dict['commit_rate'] = ''  # 委比
            stock_dict['eps'] = ''
            stock_dict['bps'] = ''
            stock_dict['turnover_rate'] = ''
            stock_dict['dividend_rate'] = ''
            stock_dict['trading_volume'] = ''
            stock_dict['trading_amount'] = ''
            stock_dict['amplitude'] = ''
            stock_dict['wave'] = ''
            stock_dict['capital_total'] = ''
            stock_dict['capital_flow'] = ''
            stock_dict['value_total'] = ''
            stock_dict['value_flow'] = ''
            stock_dict['pe_ttm'] = ''
            stock_dict['pe'] = ''
            stock_dict['pe_lyr'] = ''
            stock_dict['pb'] = ''
            stock_dict['controller_hold_ratio'] = ''
            stock_dict['fund_flow'] = ''
            stock_dict['retained_profits'] = ''
            stock_dict['trading_status'] = ''
            stock_dict['score'] = ''
            stock_dict['business_scope'] = ''

            if 'stock_name' in data_json:
                StockName.append(data_json['stock_name'])

            if 'concept' in data_json:
                Concept += data_json['concept'] #此处为分割,得搞成list
                for concept in data_json['concept']:
                    rels_ConceptInvolved.append([stock, concept])

            if 'concept_leading' in data_json:
                ConceptLeading += data_json['concept_leading']
                for concept_leading in data_json['concept_leading']:
                    rels_ConceptLeadingInvolved.append([stock, concept_leading])

            if 'industry' in data_json:
                Industry += data_json['industry'] #需要提取二级行业
                for industry in data_json['industry']:
                    rels_IndustryInvolved.append([stock, industry])

            if 'index_type' in data_json:
                IndexType += data_json['index_type']
                for index_type in data_json['index_type']:
                    rels_IndexTypeIs.append([stock, index_type])

            if 'equity_scale' in data_json:
                EquityScale += data_json['equity_scale']
                for equity_scale in data_json['equity_scale']:
                    rels_EquityScaleIs.append([stock, equity_scale])

            if 'market_type' in data_json:
                MarketType += data_json['market_type']
                for concept in data_json['market_type']:
                    rels_MarketTypeIs.append([stock, concept])

            if 'tech_form' in data_json:
                TechForm += data_json['tech_form']
                for tech_form in data_json['tech_form']:
                    rels_TechFormIs.append([stock, tech_form])

            if 'movement' in data_json:
                Movement += data_json['movement']
                for movement in data_json['movement']:
                    rels_MovementIs.append([stock, movement])

            if 'buy_signal' in data_json:
                BuySignal += data_json['buy_signal']
                for buy_signal in data_json['buy_signal']:
                    rels_BuySignalIs.append([stock, buy_signal])

            if 'sell_signal' in data_json:
                SellSignal += data_json['sell_signal']
                for sell_signal in data_json['sell_signal']:
                    rels_SellSignalIs.append([stock, sell_signal])

            if 'stock_name' in data_json:
                stock_dict['stock_name'] = data_json['stock_name']

            if 'industry_ths' in data_json:
                stock_dict['industry_ths'] = data_json['industry_ths']

            if 'open' in data_json:
                stock_dict['open'] = data_json['open']

            if 'close' in data_json:
                stock_dict['close'] = data_json['close']

            if 'high' in data_json:
                stock_dict['high'] = data_json['high']

            if 'low' in data_json:
                stock_dict['low'] = data_json['low']

            if 'volume_rate' in data_json:
                stock_dict['volume_rate'] = data_json['volume_rate']

            if 'commit_rate' in data_json:
                stock_dict['commit_rate'] = data_json['commit_rate']

            if 'eps' in data_json:
                stock_dict['eps'] = data_json['eps']

            if 'bps' in data_json:
                stock_dict['bps'] = data_json['bps']

            if 'turnover_rate' in data_json:
                stock_dict['turnover_rate'] = data_json['turnover_rate']

            if 'dividend_rate' in data_json:
                stock_dict['dividend_rate'] = data_json['dividend_rate']

            if 'trading_volume' in data_json:
                stock_dict['trading_volume'] = data_json['trading_volume']

            if 'trading_amount' in data_json:
                stock_dict['trading_amount'] = data_json['trading_amount']

            if 'amplitude' in data_json:
                stock_dict['amplitude'] = data_json['amplitude']

            if 'wave' in data_json:
                stock_dict['wave'] = data_json['wave']

            if 'capital_total' in data_json:
                stock_dict['capital_total'] = data_json['capital_total']

            if 'capital_flow' in data_json:
                stock_dict['capital_flow'] = data_json['capital_flow']

            if 'value_total' in data_json:
                stock_dict['value_total'] = data_json['value_total']

            if 'value_flow' in data_json:
                stock_dict['value_flow'] = data_json['value_flow']

            if 'pe_ttm' in data_json:
                stock_dict['pe_ttm'] = data_json['pe_ttm']

            if 'pe' in data_json:
                stock_dict['pe'] = data_json['pe']

            if 'pe_lyr' in data_json:
                stock_dict['pe_lyr'] = data_json['pe_lyr']

            if 'pb' in data_json:
                stock_dict['pb'] = data_json['pb']

            if 'controller' in data_json:
                Controller += data_json['controller']
                for controller in data_json['controller']:
                    rels_IsControlledBy.append([stock, controller])

            if 'controller_type' in data_json:
                ControllerType += data_json['controller_type']

            if 'controller_hold_ratio' in data_json:
                stock_dict['controller_hold_ratio'] = data_json['controller_hold_ratio']

            if 'fund_flow' in data_json:
                stock_dict['fund_flow'] = data_json['fund_flow']

            if 'retained_profits' in data_json:
                stock_dict['retained_profits'] = data_json['retained_profits']

            if 'trading_status' in data_json:
                stock_dict['trading_status'] = data_json['trading_status']

            if 'score' in data_json:
                stock_dict['score'] = data_json['score']

            if 'business_scope' in data_json:
                stock_dict['business_scope'] = data_json['business_scope']

            if 'main_business' in data_json:
                MainBusiness += data_json['main_business']
                for business in data_json['main_business']:
                    rels_MainBusinessIs.append([stock, business])

            if 'province' in data_json:
                Province.append(data_json['province'])
                rels_ProvinceIs.append([stock, data_json['province']])

            if 'city' in data_json:
                City.append(data_json['city'])
                rels_CityIs.append([stock, data_json['city']])

            stock_infos.append(stock_dict)

        controller_dict = dict(zip(Controller, ControllerType))

        for data in open(self.data_path_topmanager, encoding='utf-8'):
            tm_dict = {}
            count += 1
            print(count)
            data = data.replace("'", '"')
            data_json = json.loads(data)
            #print(data_json)
            stock = data_json['stock_code'][0:6]
            # stock = data_json['stock_id'] #保留末尾.SZ .SH
            tm_name = data_json['tm_name']
            tm_dict['tm_name'] = tm_name
            TopManager.append(tm_name)
            rels_TopManagerIs.append([stock, tm_name])

            tm_dict['tm_salary'] = ''

            if 'tm_title' in data_json:
                Title += data_json['tm_title']
                for title in data_json['tm_title']:
                    rels_TitleIs.append([tm_name, title])

            if 'tm_salary' in data_json:
                tm_dict['tm_salary'] = data_json['tm_salary']

            if 'tm_gender' in data_json:
                Gender.append(data_json['tm_gender'])
                rels_GenderIs.append([tm_name, data_json['tm_gender']])

            if 'tm_nationality' in data_json:
                Nationality.append(data_json['tm_nationality'])
                rels_NationalityIs.append([tm_name, data_json['tm_nationality']])

            if 'tm_educationbg' in data_json:
                EducationBg.append(data_json['tm_educationbg'])
                rels_EducationBgIs.append([tm_name, data_json['tm_educationbg']])

            if 'tm_school' in data_json:
                School += data_json['tm_school']
                for school in data_json['tm_school']:
                    rels_SchoolIs.append([tm_name, school])

            tm_infos.append(tm_dict)

        return set(StockId), set(StockName), set(Concept), set(ConceptLeading), set(Industry), set(IndexType), set(EquityScale), \
               set(MarketType), set(Controller), set(TechForm), set(Movement), set(BuySignal), set(SellSignal), set(TopManager),\
               set(Title), set(Gender), set(Nationality), set(EducationBg), set(School), set(MainBusiness), set(Province),\
               set(City), stock_infos, controller_dict, tm_infos,\
               rels_ConceptInvolved, rels_ConceptLeadingInvolved, rels_IndustryInvolved, rels_IndexTypeIs, rels_EquityScaleIs, rels_MarketTypeIs, \
               rels_IsControlledBy, rels_TechFormIs, rels_MovementIs, rels_BuySignalIs, rels_SellSignalIs, rels_TopManagerIs, \
               rels_TitleIs, rels_GenderIs, rels_NationalityIs, rels_EducationBgIs, rels_SchoolIs, rels_MainBusinessIs,\
               rels_ProvinceIs, rels_CityIs

    '''建立节点'''
    def create_node(self, label, nodes):
        count = 0
        for node_name in nodes:
            node = Node(label, name=node_name)
            #self.g.create(node)
            self.g.merge(node, label, "name") #保证节点的唯一性
            count += 1
            print(count, len(nodes))
        return

    '''创建知识图谱中心股票的节点'''
    def create_stock_nodes(self, stock_infos):
        count = 0
        for stock_dict in stock_infos:
            StockWithDate = "Stock#PDATE#".replace("#PDATE#", self.pDate)
            node = Node(StockWithDate, stock_code=stock_dict['stock_code'], stock_id=stock_dict['stock_id'], stock_name=stock_dict['stock_name'],
                        industry_ths=stock_dict['industry_ths'], open=stock_dict['open'],
                        close=stock_dict['close'], high=stock_dict['high'], low=stock_dict['low'],
                        volume_rate=stock_dict['volume_rate'], commit_rate=stock_dict['commit_rate'],
                        eps=stock_dict['eps'], bps=stock_dict['bps'], turnover_rate=stock_dict['turnover_rate'],
                        dividend_rate=stock_dict['dividend_rate'], trading_volume=stock_dict['trading_volume'],
                        trading_amount=stock_dict['trading_amount'], amplitude=stock_dict['amplitude'],
                        wave=stock_dict['wave'], capital_total=stock_dict['capital_total'],
                        capital_flow=stock_dict['capital_flow'], value_total=stock_dict['value_total'],
                        value_flow=stock_dict['value_flow'], pe_ttm=stock_dict['pe_ttm'], pe=stock_dict['pe'],
                        pe_lyr=stock_dict['pe_lyr'], pb=stock_dict['pb'], controller_hold_ratio=stock_dict['controller_hold_ratio'],
                        fund_flow=stock_dict['fund_flow'], retained_profits=stock_dict['retained_profits'],
                        trading_status=stock_dict['trading_status'], score=stock_dict['score'], business_scope=stock_dict['business_scope'])
            for property_key in ['stock_code', 'stock_id', 'stock_name', 'industry_ths', 'open', 'close', 'high', 'low',
                         'volume_rate', 'commit_rate', 'eps', 'bps', 'turnover_rate', 'dividend_rate', 'trading_volume',
                         'trading_amount', 'amplitude', 'wave', 'capital_total', 'capital_flow', 'value_tolal', 'value_flow',
                         'pe_ttm', 'pe', 'pe_lyr', 'pb', 'controller_hold_ratio', 'fund_flow',
                         'retained_profits', 'trading_status', 'score', 'business_scope']:
                self.g.merge(node, StockWithDate, property_key)
            count += 1
            print(count)
        return

    def create_controller_nodes(self, controller_dict):
        count = 0
        for key, value in controller_dict.items():
            node = Node("Controller", name=key, type=value)
            for property_key in ['name', 'type']:
                self.g.merge(node, "Controller", property_key)
            count += 1
            print(count)
        return

    def create_topmanager_nodes(self, tm_infos):
        count = 0
        for tm_dict in tm_infos:
            node = Node("Person", name=tm_dict['tm_name'], tm_salary=tm_dict['tm_salary'])
            for property_key in ['name', 'tm_salary']:
                self.g.merge(node, "Person", property_key)
            count += 1
            print(count)
        return

    '''创建知识图谱实体节点类型schema'''
    def create_graphnodes(self):
        StockId, StockName, Concept, ConceptLeading, Industry, IndexType, EquityScale, MarketType, Controller, TechForm, Movement, BuySignal, \
        SellSignal, TopManager, Title, Gender, Nationality, EducationBg, School, MainBusiness, Province, City,\
        stock_infos, controller_dict, tm_infos, \
        rels_ConceptInvolved, rels_ConceptLeadingInvolved, rels_IndustryInvolved, \
        rels_IndexTypeIs, rels_EquityScaleIs, rels_MarketTypeIs, rels_IsControlledBy, rels_TechFormIs, rels_MovementIs,\
        rels_BuySignalIs, rels_SellSignalIs, rels_TopManagerIs, rels_TitleIs, rels_GenderIs, rels_NationalityIs, \
        rels_EducationBgIs, rels_SchoolIs, rels_MainBusinessIs, rels_ProvinceIs, rels_CityIs = self.read_nodes()

        self.create_stock_nodes(stock_infos)
        self.create_controller_nodes(controller_dict)
        self.create_node('Concept', Concept)
        print(len(Concept))
        self.create_node('ConceptLeading', ConceptLeading)
        print(len(ConceptLeading))
        self.create_node('Industry', Industry)
        print(len(Industry))
        self.create_node('IndexType', IndexType)
        print(len(IndexType))
        self.create_node('EquityScale', EquityScale)
        print(len(EquityScale))
        self.create_node('MarketType', MarketType)
        print(len(MarketType))
        self.create_node('TechForm', TechForm)
        print(len(TechForm))
        self.create_node('Movement', Movement)
        print(len(Movement))
        self.create_node('BuySignal', BuySignal)
        print(len(BuySignal))
        self.create_node('SellSignal', SellSignal)
        print(len(SellSignal))
        self.create_topmanager_nodes(tm_infos)
        self.create_node('Gender', Gender)
        print(len(Gender))
        self.create_node('Title', Title)
        print(len(Title))
        self.create_node('Nationality', Nationality)
        print(len(Nationality))
        self.create_node('EducationBg', EducationBg)
        print(len(EducationBg))
        self.create_node('School', School)
        print(len(School))
        self.create_node('MainBusiness', MainBusiness)
        print(len(MainBusiness))
        self.create_node('Province', Province)
        print(len(Province))
        self.create_node('City', City)
        print(len(City))

        return


    '''创建实体关系边'''
    def create_graphrels(self):
        StockId, StockName, Concept, ConceptLeading, Industry, IndexType, EquityScale, MarketType, Controller, TechForm, Movement, BuySignal, \
        SellSignal, TopManager, Title, Gender, Nationality, EducationBg, School, MainBusiness, Province, City,\
        stock_infos, controller_dict, tm_infos, \
        rels_ConceptInvolved, rels_ConceptLeadingInvolved, rels_IndustryInvolved, \
        rels_IndexTypeIs, rels_EquityScaleIs, rels_MarketTypeIs, rels_IsControlledBy, rels_TechFormIs, rels_MovementIs, \
        rels_BuySignalIs, rels_SellSignalIs, rels_TopManagerIs, rels_TitleIs, rels_GenderIs, rels_NationalityIs, \
        rels_EducationBgIs, rels_SchoolIs, rels_MainBusinessIs, rels_ProvinceIs, rels_CityIs = self.read_nodes()

        StockWithDate = "Stock#PDATE#".replace("#PDATE#", self.pDate)
        self.create_relationship(StockWithDate, 'Concept', rels_ConceptInvolved, 'ConceptInvolved', '所属概念')
        self.create_relationship(StockWithDate, 'ConceptLeading', rels_ConceptLeadingInvolved, 'ConceptLeadingInvolved', '概念龙头')
        self.create_relationship(StockWithDate, 'Industry', rels_IndustryInvolved, 'IndustryInvolved', '所属行业')
        self.create_relationship(StockWithDate, 'IndexType', rels_IndexTypeIs, 'IndexTypeIs', '所属指数类')
        self.create_relationship(StockWithDate, 'EquityScale', rels_EquityScaleIs, 'EquityScaleIs', '股本规模')
        self.create_relationship(StockWithDate, 'MarketType', rels_MarketTypeIs, 'MarketTypeIs', '股票市场类型')
        self.create_relationship(StockWithDate, 'TechForm', rels_TechFormIs, 'TechFormIs', '技术形态')
        self.create_relationship(StockWithDate, 'Movement', rels_MovementIs, 'MovementIs', '选股动向')
        self.create_relationship(StockWithDate, 'BuySignal', rels_BuySignalIs, 'BuySignalIs', '买入信号')
        self.create_relationship(StockWithDate, 'SellSignal', rels_SellSignalIs, 'SellSignalIs', '卖出信号')
        self.create_relationship(StockWithDate, 'Controller', rels_IsControlledBy, 'IsControlledBy', '实际控制人')
        self.create_relationship(StockWithDate, 'Person', rels_TopManagerIs, 'TopManagerIs', '高管')
        self.create_relationship(StockWithDate, 'MainBusiness', rels_MainBusinessIs, 'MainBusinessIs', '主营产品')
        self.create_relationship(StockWithDate, 'Province', rels_ProvinceIs, 'ProvinceIs', '省份')
        self.create_relationship(StockWithDate, 'City', rels_CityIs, 'CityIs', '城市')
        self.create_relationship('Person', 'School', rels_SchoolIs, 'SchoolIs', '毕业学校')
        self.create_relationship('Person', 'EducationBg', rels_EducationBgIs, 'EducationBgIs', '学历')
        self.create_relationship('Person', 'Nationality', rels_NationalityIs, 'NationalityIs', '国籍')
        self.create_relationship('Person', 'Gender', rels_GenderIs, 'GenderIs', '性别')
        self.create_relationship('Person', 'Title', rels_TitleIs, 'TitleIs', '高管职务')

    '''创建实体关联边'''
    def create_relationship(self, start_node, end_node, edges, rel_type, rel_name):
        count = 0
        # 去重处理
        set_edges = []
        for edge in edges:
            set_edges.append('###'.join(edge))
        all = len(set(set_edges))
        for edge in set(set_edges):
            edge = edge.split('###')
            p = edge[0]
            q = edge[1]
            query = "match(p:%s),(q:%s) where p.stock_id='%s'and q.name='%s' merge (p)-[rel:%s{name:'%s'}]->(q)" % (
                start_node, end_node, p, q, rel_type, rel_name)

            try:
                self.g.run(query)
                count += 1
                print(rel_type, count, all)
            except Exception as e:
                print(e)
        return

    '''导出数据'''
    def export_data(self):
        StockId, StockName, Concept, ConceptLeading, Industry, IndexType, EquityScale, MarketType, Controller, TechForm, Movement, BuySignal, \
        SellSignal, TopManager, Title, Gender, Nationality, EducationBg, School, MainBusiness, Province, City,\
        stock_infos, controller_dict, tm_infos, \
        rels_ConceptInvolved, rels_ConceptLeadingInvolved, rels_IndustryInvolved, \
        rels_IndexTypeIs, rels_EquityScaleIs, rels_MarketTypeIs, rels_IsControlledBy, rels_TechFormIs, rels_MovementIs, \
        rels_BuySignalIs, rels_SellSignalIs, rels_TopManagerIs, rels_TitleIs, rels_GenderIs, rels_NationalityIs,\
        rels_EducationBgIs, rels_SchoolIs, rels_MainBusinessIs, rels_ProvinceIs, rels_CityIs = self.read_nodes()

        f_stockid = open('stock_dict/stockid.txt', 'w+', encoding='utf-8')
        f_stockname = open('stock_dict/stockname.txt', 'w+', encoding='utf-8')
        f_concept = open('stock_dict/concept.txt', 'w+', encoding='utf-8')
        f_conceptleading = open('stock_dict/conceptleading.txt', 'w+', encoding='utf-8')
        f_industry = open('stock_dict/industry.txt', 'w+', encoding='utf-8')
        f_indextype = open('stock_dict/indextype.txt', 'w+', encoding='utf-8')
        f_equityscale = open('stock_dict/equityscale.txt', 'w+', encoding='utf-8')
        f_markettype = open('stock_dict/marketype.txt', 'w+', encoding='utf-8')
        f_controller = open('stock_dict/controller.txt', 'w+', encoding='utf-8')
        f_buysignal = open('stock_dict/buysignal.txt', 'w+', encoding='utf-8')
        f_sellsignal = open('stock_dict/sellsignal.txt', 'w+', encoding='utf-8')
        f_techform = open('stock_dict/techform.txt', 'w+', encoding='utf-8')
        f_movement = open('stock_dict/movement.txt', 'w+', encoding='utf-8')
        f_topmanager = open('stock_dict/topmanager.txt', 'w+', encoding='utf-8')
        f_title = open('stock_dict/title.txt', 'w+', encoding='utf-8')
        f_gender = open('stock_dict/gender.txt', 'w+', encoding='utf-8')
        f_nationality = open('stock_dict/nationality.txt', 'w+', encoding='utf-8')
        f_educationbg = open('stock_dict/educationbg.txt', 'w+', encoding='utf-8')
        f_school = open('stock_dict/school.txt', 'w+', encoding='utf-8')
        f_mainbusiness = open('stock_dict/mainbusiness.txt', 'w+', encoding='utf-8')
        f_province = open('stock_dict/province.txt', 'w+', encoding='utf-8')
        f_city = open('stock_dict/city.txt', 'w+', encoding='utf-8')

        f_stockid.write('\n'.join(list(StockId)))
        f_stockname.write('\n'.join(list(StockName)))
        f_concept.write('\n'.join(list(Concept)))
        f_conceptleading.write('\n'.join(list(ConceptLeading)))
        f_industry.write('\n'.join(list(Industry)))
        f_indextype.write('\n'.join(list(IndexType)))
        f_equityscale.write('\n'.join(list(EquityScale)))
        f_markettype.write('\n'.join(list(MarketType)))
        f_controller.write('\n'.join(list(Controller)))
        f_buysignal.write('\n'.join(list(BuySignal)))
        f_sellsignal.write('\n'.join(list(SellSignal)))
        f_techform.write('\n'.join(list(TechForm)))
        f_movement.write('\n'.join(list(Movement)))
        f_topmanager.write('\n'.join(list(TopManager)))
        f_title.write('\n'.join(list(Title)))
        f_gender.write('\n'.join(list(Gender)))
        f_nationality.write('\n'.join(list(Nationality)))
        f_educationbg.write('\n'.join(list(EducationBg)))
        f_school.write('\n'.join(list(School)))
        f_mainbusiness.write('\n'.join(list(MainBusiness)))
        f_province.write('\n'.join(list(Province)))
        f_city.write('\n'.join(list(City)))

        f_stockid.close()
        f_stockname.close()
        f_concept.close()
        f_conceptleading.close()
        f_industry.close()
        f_indextype.close()
        f_equityscale.close()
        f_markettype.close()
        f_controller.close()
        f_buysignal.close()
        f_sellsignal.close()
        f_techform.close()
        f_movement.close()
        f_topmanager.close()
        f_title.close()
        f_gender.close()
        f_nationality.close()
        f_educationbg.close()
        f_school.close()
        f_mainbusiness.close()
        f_province.close()
        f_city.close()

        return



if __name__ == '__main__':
    pDate = '20190121'
    #StockGraph(pDate).export_data()
    #StockGraph(pDate).create_graphnodes()
    StockGraph(pDate).create_graphrels()
    #StockGraph('test').create_node('test', ['test0', 'test1', 'test2'])

