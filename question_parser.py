#!/usr/bin/env python3
# coding: utf-8
# File: question_parser.py
# Author: https://github.com/shinezai
# Date: 19-02

class QuestionPaser():
    def __init__(self, pDate):
        self.pDate = 'Stock' + pDate

    '''构建实体节点'''
    def build_entitydict(self, args):
        entity_dict = {}
        for arg, types in args.items():
            for type in types:
                if type not in entity_dict:
                    entity_dict[type] = [arg]
                else:
                    entity_dict[type].append(arg)

        return entity_dict

    '''解析主函数'''
    def parser_main(self, res_classify):
        args = res_classify['args']
        entity_dict = self.build_entitydict(args)
        question_types = res_classify['question_types']
        sqls = []
        for question_type in question_types:
            sql_ = {}
            sql_['question_type'] = question_type
            sql = []

            if question_type == 'zxpan_cancept_stockget':
                sql = self.sql_transfer(question_type, entity_dict.get('concept'))

            elif question_type == 'equityscale_concept_stockget':
                sql = self.sql_transfer(question_type, entity_dict.get('equityscale') + entity_dict.get('concept'))

            elif question_type == 'stockid_conceptget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockid'))

            elif question_type == 'stockname_conceptget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockname'))

            elif question_type == 'concept_stockget':
                sql = self.sql_transfer(question_type, entity_dict.get('concept'))

            elif question_type == 'conceptleading_stockget':
                sql = self.sql_transfer(question_type, entity_dict.get('conceptleading'))

            elif question_type == 'stockid_controllerget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockid'))

            elif question_type == 'stockname_controllerget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockname'))

            elif question_type == 'controller_stockget':
                sql = self.sql_transfer(question_type, entity_dict.get('controller'))

            elif question_type == 'stockid_industryget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockid'))

            elif question_type == 'stockname_industryget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockname'))

            elif question_type == 'industry_stockget':
                sql = self.sql_transfer(question_type, entity_dict.get('industry'))

            elif question_type == 'stockid_indextypeget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockid'))

            elif question_type == 'stockname_indextypeget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockname'))

            elif question_type == 'indextype_stockget':
                sql = self.sql_transfer(question_type, entity_dict.get('indextype'))

            elif question_type == 'stockid_equityscaleget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockid'))

            elif question_type == 'stockname_equityscaleget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockname'))

            elif question_type == 'equityscale_stockget':
                sql = self.sql_transfer(question_type, entity_dict.get('equityscale'))

            elif question_type == 'stockid_markettypeget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockid'))

            elif question_type == 'stockname_markettypeget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockname'))

            elif question_type == 'markettype_stockget':
                sql = self.sql_transfer(question_type, entity_dict.get('markettype'))

            elif question_type == 'stockid_buysignalget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockid'))

            elif question_type == 'stockname_buysignalget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockname'))

            elif question_type == 'buysignal_stockget':
                sql = self.sql_transfer(question_type, entity_dict.get('buysignal'))

            elif question_type == 'stockid_sellignalget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockid'))

            elif question_type == 'stockname_sellsignalget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockname'))

            elif question_type == 'sellsignal_stockget':
                sql = self.sql_transfer(question_type, entity_dict.get('sellsignal'))

            elif question_type == 'stockid_techformget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockid'))

            elif question_type == 'stockname_techformget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockname'))

            elif question_type == 'techform_stockget':
                sql = self.sql_transfer(question_type, entity_dict.get('techform'))

            elif question_type == 'stockid_movementget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockid'))

            elif question_type == 'stockname_movementget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockname'))

            elif question_type == 'movement_stockget':
                sql = self.sql_transfer(question_type, entity_dict.get('movement'))

            elif question_type == 'stockid_scoreget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockid'))

            elif question_type == 'stockname_scoreget':
                sql = self.sql_transfer(question_type, entity_dict.get('stockname'))

            if sql:
                sql_['sql'] = sql

                sqls.append(sql_)

        return sqls

    '''针对不同的问题，分开进行处理'''
    def sql_transfer(self, question_type, entities):
        if not entities:
            print('in not entities\n')
            return []

       # print(entities)

        # 查询语句
        sql = []

        #概念+股本规模询股
        if question_type == 'zxpan_concept_stockget':
            sql = ["MATCH (m:{0})-[r:EquityScaleIs]->(n:EquityScale) where n.name in ['中盘股', '小盘股'] return m"]

        elif question_type == 'equityscale_concept_stockget':
            sql1 = ["MATCH (m:{0})-[r:EquityScaleIs]->(n:EquityScale) where n.name = '{1}' return m.stock_id, m.stock_name, r.name, n.name".format(self.pDate, i) for i in entities]
            sql2 = ["MATCH (m:{0})-[r:ConceptInvolved]->(n:Concept) where n.name = '{1}' return m.stock_id, m.stock_name, r.name, n.name".format(self.pDate, i) for i in entities]
            sql = sql1 + sql2

        #按股票代码查询所属概念
        elif question_type == 'stockid_conceptget':
            sql = ["MATCH (m:{0})-[r:ConceptInvolved]->(n:Concept) where m.stock_id = '{1}' return m.stock_id, m.sotck_name, r.name, n.name".format(self.pDate, i) for i in entities]

        #按股票名称查询所属概念
        elif question_type == 'stockname_conceptget':
            sql = ["MATCH (m:{0})-[r:ConceptInvolved]->(n:Concept) where m.stock_name = '{1}' return m.stock_id, m.stock_name, r.name, n.name".format(self.pDate, i) for i in entities]

        #根据概念查股
        elif question_type == 'concept_stockget':
            length = len(entities)
            part1_list = []
            part2_list = []
            part3_list = []
            for i in range(length):
                part1_list.append("(m:{0})-[r#i#:ConceptInvolved]->(n#i#:Concept)".format(self.pDate).replace('#i#', str(i)))
                part2_list.append("n#i#.name = '{0}'".format(entities[i]).replace('#i#', str(i)))
                part3_list.append("n#i#.name".replace('#i#', str(i)))
            part1 = ",".join(part1_list)
            part2 = " and ".join(part2_list)
            part3 = ','.join(part3_list)
            sql = ["MATCH #part1# where #part2# return m.stock_id, m.stock_name, #part3#".replace('#part1#', part1).replace('#part2#', part2).replace('#part3#', part3)]
            #sql = ["MATCH (m:{0})-[r:ConceptInvolved]->(n:Concept) where n.name = '{1}' return m.stock_id, m.stock_name, n.name".format(self.pDate, i) for i in entities]

        #按股票代码查询实际控制人
        elif question_type == 'stockid_controllerget':
            sql = ["MATCH (m:{0})-[r:IsControlledBy]->(n:Controller) where m.stock_id = '{1}' return m.stock_id, m.sotck_name, r.name, n.name, n.type".format(self.pDate, i) for i in entities]

        #按股票名称查询实际控制人
        elif question_type == 'stockname_controllerget':
            sql = ["MATCH (m:{0})-[r:IsControlledBy]->(n:Controller) where m.stock_name = '{1}' return m.stock_id, m.stock_name, r.name, n.name, n.type".format(self.pDate, i) for i in entities]

        #根据实际控制人查股
        elif question_type == 'controller_stockget':
            sql = ["MATCH (m:{0})-[r:IsControlledBy]->(n:Controller) where n.name = '{1}' return m.stock_id, m.stock_name, n.name".format(self.pDate, i) for i in entities]

        #根据概念查龙头股
        elif question_type == 'conceptleading_stockget':
            length = len(entities)
            part1_list = []
            part2_list = []
            part3_list = []
            for i in range(length):
                part1_list.append("(m:{0})-[r#i#:ConceptLeadingInvolved]->(n#i#:ConceptLeading)".format(self.pDate).replace('#i#', str(i)))
                part2_list.append("n#i#.name = '{0}'".format(entities[i]).replace('#i#', str(i)))
                part3_list.append("n#i#.name".replace('#i#', str(i)))
            part1 = ",".join(part1_list)
            part2 = " and ".join(part2_list)
            part3 = ','.join(part3_list)
            sql = ["MATCH #part1# where #part2# return m.stock_id, m.stock_name, #part3#".replace('#part1#', part1).replace('#part2#', part2).replace('#part3#', part3)]
            #sql = ["MATCH (m:{0})-[r:ConceptLeadingInvolved]->(n:ConceptLeading) where n.name = '{1}' return m.stock_id, m.stock_name, n.name".format(self.pDate, i) for i in entities]

        #按股票代码查询所属行业
        elif question_type == 'stockid_industryget':
            sql = ["MATCH (m:{0})-[r:IndustryInvolved]->(n:Industry) where m.stock_id = '{1}' return m.stock_id, m.stock_name, r.name, n.name".format(self.pDate, i) for i in entities]

        #按股票名称查询所属行业
        elif question_type == 'stockname_industryget':
            sql = ["MATCH (m:{0})-[r:IndustryInvolved]->(n:Industry) where m.stock_id = '{1}' return m.stock_id, m.stock_name, r.name, n.name".format(self.pDate, i) for i in entities]

        #按行业查询股票
        elif question_type == 'industry_stockget':
            sql = ["MATCH (m:{0})-[r:IndustryInvolved]->(n:Industry) where n.name = '{1}' return m.stock_name, n.name".format(self.pDate, i) for i in entities]

        #查询指数类
        elif question_type == 'stockid_indextypeget':
            sql = ["MATCH (m:{0})-[r:IndexTypeIs]->(n:IndexType) where m.stock_id = '{1}' return m.stock_id, m.stock_name, n.name".format(self.pDate, i) for i in entities]

        #查询指数类
        elif question_type == 'stockname_indextypeget':
            sql = ["MATCH (m:{0})-[r:IndexTypeIs]->(n:IndexType) where m.stock_name = '{1}' return m.stock_id, m.stock_name, n.name".format(self.pDate, i) for i in entities]

        #根据指数类查股
        elif question_type == 'indextype_stockget':
            length = len(entities)
            part1_list = []
            part2_list = []
            part3_list = []
            for i in range(length):
                part1_list.append(
                    "(m:{0})-[r#i#:IndexTypeIs]->(n#i#:IndexType)".format(self.pDate).replace('#i#', str(i)))
                part2_list.append("n#i#.name = '{0}'".format(entities[i]).replace('#i#', str(i)))
                part3_list.append("n#i#.name".replace('#i#', str(i)))
            part1 = ",".join(part1_list)
            part2 = " and ".join(part2_list)
            part3 = ','.join(part3_list)
            sql = ["MATCH #part1# where #part2# return m.stock_id, m.stock_name, #part3#".replace('#part1#', part1).replace('#part2#', part2).replace('#part3#', part3)]
            #sql = ["MATCH (m:{0})-[r:IndexTypeIs]->(n:IndexType) where n.name = '{1}' return m.stock_id, m.stock_name, n.name".format(self.pDate, i) for i in entities]

        #查股本规模
        elif question_type == 'stockid_equityscaleget':
            sql = ["MATCH (m:{0})-[r:EquityScaleIs]->(n:EquityScale) where m.stock_id = '{1}' return m.stock_id, " \
                   "m.stock_name, n.name, m.capital_total, m.capital_flow".format(self.pDate, i) for i in entities]

        elif question_type == 'stockname_equityscaleget':
            sql = ["MATCH (m:{0})-[r:EquityScaleIs]->(n:EquityScale) where m.stock_name = '{1}' return m.stock_id, " \
                   "m.stock_name, n.name, m.capital_total, m.capital_flow".format(self.pDate, i) for i in entities]

        elif question_type == 'equityscale_stockget':
            sql = ["MATCH (m:{0})-[r:EquityScaleIs]->(n:EquityScale) where n.name = '{1}' return m.stock_id, m.stock_name, " \
                   "n.name".format(self.pDate, i) for i in entities]

        # 查市场类型
        elif question_type == 'stockid_markettypeget':
            sql = ["MATCH (m:{0})-[r:MarketTypeIs]->(n:MarketType) where m.stock_id = '{1}' return m.stock_id, " \
                   "m.stock_name, n.name".format(self.pDate, i) for i in entities]

        elif question_type == 'stockname_markettypeget':
            sql = ["MATCH (m:{0})-[r:MarketTypeIs]->(n:MarketType) where m.stock_name = '{1}' return m.stock_id, " \
                   "m.stock_name, n.name".format(self.pDate, i) for i in entities]

        elif question_type == 'markettype_stockget':
            length = len(entities)
            part1_list = []
            part2_list = []
            part3_list = []
            for i in range(length):
                part1_list.append("(m:{0})-[r#i#:MarketTypeIs]->(n#i#:MarketType)".format(self.pDate).replace('#i#', str(i)))
                part2_list.append("n#i#.name = '{0}'".format(entities[i]).replace('#i#', str(i)))
                part3_list.append("n#i#.name".replace('#i#', str(i)))
            part1 = ",".join(part1_list)
            part2 = " and ".join(part2_list)
            part3 = ','.join(part3_list)
            sql = ["MATCH #part1# where #part2# return m.stock_id, m.stock_name, #part3#".replace('#part1#', part1).replace('#part2#', part2).replace('#part3#', part3)]
            #sql = ["MATCH (m:{0})-[r:MarketTypeIs]->(n:MarketType) where n.name = '{1}' return m.stock_id, m.stock_name, " \
            #    "n.name".format(self.pDate, i) for i in entities]

        #查买入信号
        elif question_type == 'stockid_buysignalget':
            sql = ["MATCH (m:{0})-[r:BuySignalIs]->(n:BuySignal) where m.stock_id = '{1}' return m.stock_id, " \
                   "m.stock_name, n.name".format(self.pDate, i) for i in entities]

        elif question_type == 'stockname_buysignalget':
            sql = ["MATCH (m:{0})-[r:BuySignalIs]->(n:BuySignal) where m.stock_name = '{1}' return m.stock_id, " \
                   "m.stock_name, n.name".format(self.pDate, i) for i in entities]

        elif question_type == 'buysignal_stockget':
            length = len(entities)
            part1_list = []
            part2_list = []
            part3_list = []
            for i in range(length):
                part1_list.append("(m:{0})-[r#i#:BuySignalIs]->(n#i#:BuySignal)".format(self.pDate).replace('#i#', str(i)))
                part2_list.append("n#i#.name = '{0}'".format(entities[i]).replace('#i#', str(i)))
                part3_list.append("n#i#.name".replace('#i#', str(i)))
            part1 = ",".join(part1_list)
            part2 = " and ".join(part2_list)
            part3 = ','.join(part3_list)
            sql = ["MATCH #part1# where #part2# return m.stock_id, m.stock_name, #part3#".replace('#part1#', part1).replace('#part2#', part2).replace('#part3#', part3)]
            #sql = ["MATCH (m:{0})-[r:BuySignalIs]->(n:BuySignal) where n.name = '{1}' return m.stock_id, m.stock_name, " \
            #    "n.name".format(self.pDate, i) for i in entities]

        # 查卖出信号
        elif question_type == 'stockid_sellsignalget':
            sql = ["MATCH (m:{0})-[r:SellSignalIs]->(n:SellSignal) where m.stock_id = '{1}' return m.stock_id, " \
               "m.stock_name, n.name".format(self.pDate, i) for i in entities]

        elif question_type == 'stockname_sellsignalget':
            sql = ["MATCH (m:{0})-[r:SellSignalIs]->(n:SellSignal) where m.stock_name = '{1}' return m.stock_id, " \
                   "m.stock_name, n.name".format(self.pDate, i) for i in entities]

        elif question_type == 'sellsignal_stockget':
            length = len(entities)
            part1_list = []
            part2_list = []
            part3_list = []
            for i in range(length):
                part1_list.append("(m:{0})-[r#i#:SellSignalIs]->(n#i#:SellSignal)".format(self.pDate).replace('#i#', str(i)))
                part2_list.append("n#i#.name = '{0}'".format(entities[i]).replace('#i#', str(i)))
                part3_list.append("n#i#.name".replace('#i#', str(i)))
            part1 = ",".join(part1_list)
            part2 = " and ".join(part2_list)
            part3 = ','.join(part3_list)
            sql = ["MATCH #part1# where #part2# return m.stock_id, m.stock_name, #part3#".replace('#part1#', part1).replace('#part2#', part2).replace('#part3#', part3)]
            #sql = ["MATCH (m:{0})-[r:SellSignalIs]->(n:SellSignal) where n.name = '{1}' return m.stock_id, m.stock_name, " \
            #    "n.name".format(self.pDate, i) for i in entities]

        # 查技术形态
        elif question_type == 'stockid_techformget':
            sql = ["MATCH (m:{0})-[r:TechFormIs]->(n:TechForm) where m.stock_id = '{1}' return m.stock_id, " \
                   "m.stock_name, n.name".format(self.pDate, i) for i in entities]

        elif question_type == 'stockname_techformget':
            sql = ["MATCH (m:{0})-[r:TechFormIs]->(n:TechForm) where m.stock_name = '{1}' return m.stock_id, " \
                   "m.stock_name, n.name".format(self.pDate, i) for i in entities]

        elif question_type == 'techform_stockget':
            length = len(entities)
            part1_list = []
            part2_list = []
            part3_list = []
            for i in range(length):
                part1_list.append("(m:{0})-[r#i#:TechFormIs]->(n#i#:TechForm)".format(self.pDate).replace('#i#', str(i)))
                part2_list.append("n#i#.name = '{0}'".format(entities[i]).replace('#i#', str(i)))
                part3_list.append("n#i#.name".replace('#i#', str(i)))
            part1 = ",".join(part1_list)
            part2 = " and ".join(part2_list)
            part3 = ','.join(part3_list)
            sql = ["MATCH #part1# where #part2# return m.stock_id, m.stock_name, #part3#".replace('#part1#', part1).replace('#part2#', part2).replace('#part3#', part3)]
            #sql = ["MATCH (m:{0})-[r:TechFormIs]->(n:TechForm) where n.name = '{1}' return m.stock_id, m.stock_name, " \
            #    "n.name".format(self.pDate, i) for i in entities]

        #查选股动向
        elif question_type == 'stockid_movementget':
            sql = ["MATCH (m:{0})-[r:MovementIs]->(n:Movement) where m.stock_id = '{1}' return m.stock_id, " \
                   "m.stock_name, n.name".format(self.pDate, i) for i in entities]

        elif question_type == 'stockname_movementget':
            sql = ["MATCH (m:{0})-[r:MovementIs]->(n:Movement) where m.stock_name = '{1}' return m.stock_id, " \
                   "m.stock_name, n.name".format(self.pDate, i) for i in entities]

        elif question_type == 'movement_stockget':
            length = len(entities)
            part1_list = []
            part2_list = []
            part3_list = []
            for i in range(length):
                part1_list.append("(m:{0})-[r#i#:MovementIs]->(n#i#:Movement)".format(self.pDate).replace('#i#', str(i)))
                part2_list.append("n#i#.name = '{0}'".format(entities[i]).replace('#i#', str(i)))
                part3_list.append("n#i#.name".replace('#i#', str(i)))
            part1 = ",".join(part1_list)
            part2 = " and ".join(part2_list)
            part3 = ','.join(part3_list)
            sql = ["MATCH #part1# where #part2# return m.stock_id, m.stock_name, #part3#".replace('#part1#', part1).replace('#part2#', part2).replace('#part3#', part3)]
            #sql = ["MATCH (m:{0})-[r:MovementIs]->(n:Movement) where n.name = '{1}' return m.stock_id, m.stock_name, " \
            #    "n.name".format(self.pDate, i) for i in entities]

        #按股票代码查询诊股得分
        elif question_type == 'stockid_scoreget':
            sql = ["MATCH (m:{0}) where m.stock_id = '{1}' return m.stock_id, m.stock_name, m.score".format(self.pDate, i) for i in entities]

        elif question_type == 'stockname_scoreget':
            sql = ["MATCH (m:{0}) where m.stock_name = '{1}' return m.stock_id, m.stock_name, m.score".format(self.pDate, i) for i in entities]

        return sql

if __name__ == '__main__':
    handler = QuestionPaser()
