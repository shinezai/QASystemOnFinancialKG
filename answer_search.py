#!/usr/bin/env python3
# coding: utf-8
# File: answer_search.py
# Author: https://github.com/shinezai
# Date: 19-02

from graph_database import GRAPH_DB_CONN

class AnswerSearcher:
    def __init__(self):
        self.g = GRAPH_DB_CONN
        self.num_limit = 20

    '''执行cypher查询，并返回相应结果'''
    def search_main(self, sqls):
        final_answers = []
        for sql_ in sqls:
            question_type = sql_['question_type']
            queries = sql_['sql']
            answers = []
            for query in queries:
                ress = self.g.run(query).data()
                answers += ress
            print(answers)
            final_answer = self.answer_prettify(question_type, answers)
            if final_answer:
                final_answers.append(final_answer)
        return final_answers

    '''根据对应的qustion_type，调用相应的回复模板'''
    def answer_prettify(self, question_type, answers):
        final_answer = []
        if not answers:
            return ''

        elif question_type == 'equityscale_concept_stockget':
            desc = []

        elif question_type == 'stockid_conceptget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}的所属概念是：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockname_conceptget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}的所属概念是：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'concept_stockget':
            desc = [i['m.stock_id'] + ' ' + i['m.stock_name'] for i in answers]
            length = len(answers[0])
            list_subject = []
            if length > 2:
                for i in range(2, length):
                    list_subject.append(answers[0]['n#i#.name'.replace('#i#', str(i-2))])
            subject = "、".join(list_subject)
            final_answer = '属于{0}概念的股票有：{1}'.format(subject, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockid_controllerget':
            desc = [i['n.name'] + '(' + i['n.type'] + ')' for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}的实际控制人是：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockname_controllerget':
            desc = [i['n.name'] + '(' + i['n.type'] + ')' for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}的实际控制人是：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'controller_stockget':
            desc = [i['m.stock_id'] + ' ' + i['m.stock_name'] for i in answers]
            subject = answers[0]['n.name']
            final_answer = '{0}作为实际控制人的股票有：{1}'.format(subject, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'conceptleading_stockget':
            desc = [i['m.stock_id'] + ' ' + i['m.stock_name'] for i in answers]
            length = len(answers[0])
            list_subject = []
            if length > 2:
                for i in range(2, length):
                    list_subject.append(answers[0]['n#i#.name'.replace('#i#', str(i-2))])
            subject = "、".join(list_subject)
            final_answer = '属于{0}概念的龙头股票有：{1}'.format(subject, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockid_industryget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}的所属行业是：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockname_industryget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}的所属行业是：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'industry_stockget':
            desc = [i['m.stock_id'] + ' ' + i['m.stock_name'] for i in answers]
            subject = answers[0]['n.name']
            final_answer = '属于{0}行业的股票有：{1}'.format(subject, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockid_indextypeget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '将{0} {1}纳入的指数类有：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockname_indextypeget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '将{0} {1}纳入的指数类有：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'indextype_stockget':
            desc = [i['m.stock_id'] + ' ' + i['m.stock_name'] for i in answers]
            length = len(answers[0])
            list_subject = []
            if length > 2:
                for i in range(2, length):
                    list_subject.append(answers[0]['n#i#.name'.replace('#i#', str(i-2))])
            subject = "、".join(list_subject)
            final_answer = '纳入{0}的股票有：{1}'.format(subject, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockid_equityscaleget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            subject3 = answers[0]['m.capital_total']
            subject4 = answers[0]['m.capital_flow']
            final_answer = '{0} {1}是一支{2}，总股本为{3}股，A股流通{4}股'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]), subject3, subject4)

        elif question_type == 'stockname_equityscaleget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            subject3 = answers[0]['m.capital_total']
            subject4 = answers[0]['m.capital_flow']
            final_answer = '{0} {1}是一支{2}，总股本为{3}股，A股流通{4}股'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]), subject3, subject4)

        elif question_type == 'equityscale_stockget':
            desc = [i['m.stock_id'] + ' ' + i['m.stock_name'] for i in answers]
            subject = answers[0]['n.name']
            final_answer = '属于{0}的股票有：{1}'.format(subject, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockid_markettypeget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}的市场类型是：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockname_markettypeget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}的市场类型是：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'markettype_stockget':
            desc = [i['m.stock_id'] + ' ' + i['m.stock_name'] for i in answers]
            length = len(answers[0])
            list_subject = []
            if length > 2:
                for i in range(2, length):
                    list_subject.append(answers[0]['n#i#.name'.replace('#i#', str(i-2))])
            subject = "、".join(list_subject)
            final_answer = '属于{0}的股票有：{1}'.format(subject, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockid_buysignalget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}释放的买入信号有：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockname_buysignalget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}释放的买入信号有：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'buysignal_stockget':
            desc = [i['m.stock_id'] + ' ' + i['m.stock_name'] for i in answers]
            length = len(answers[0])
            list_subject = []
            if length > 2:
                for i in range(2, length):
                    list_subject.append(answers[0]['n#i#.name'.replace('#i#', str(i-2))])
            subject = "、".join(list_subject)
            final_answer = '释放出{0}买入信号的股票有：{1}'.format(subject, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockid_sellsignalget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}释放的卖出入信号有：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockname_sellsignalget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}释放的卖出信号有：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'sellsignal_stockget':
            desc = [i['m.stock_id'] + ' ' + i['m.stock_name'] for i in answers]
            length = len(answers[0])
            list_subject = []
            if length > 2:
                for i in range(2, length):
                    list_subject.append(answers[0]['n#i#.name'.replace('#i#', str(i-2))])
            subject = "、".join(list_subject)
            final_answer = '释放出{0}卖出信号的股票有：{1}'.format(subject, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockid_techformget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}表现出的技术形态是：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockname_techformget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}表现出的技术形态是：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'techform_stockget':
            desc = [i['m.stock_id'] + ' ' + i['m.stock_name'] for i in answers]
            length = len(answers[0])
            list_subject = []
            if length > 2:
                for i in range(2, length):
                    list_subject.append(answers[0]['n#i#.name'.replace('#i#', str(i-2))])
            subject = "、".join(list_subject)
            final_answer = '表现出{0}技术形态的股票有：{1}'.format(subject, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockid_movementget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '参考{0} {1}的这些指标：{2}，再决定是否建仓吧！'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockname_movementget':
            desc = [i['n.name'] for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '参考{0} {1}的这些指标：{2}，再决定是否建仓吧！'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'movement_stockget':
            desc = [i['m.stock_id'] + ' ' + i['m.stock_name'] for i in answers]
            length = len(answers[0])
            list_subject = []
            if length > 2:
                for i in range(2, length):
                    list_subject.append(answers[0]['n#i#.name'.replace('#i#', str(i-2))])
            subject = "、".join(list_subject)
            final_answer = '{0}的股票有：{1}'.format(subject, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockid_scoreget':
            desc = [str(i['m.score']) for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}的诊股得分是：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        elif question_type == 'stockname_scoreget':
            desc = [str(i['m.score']) for i in answers]
            subject1 = answers[0]['m.stock_id']
            subject2 = answers[0]['m.stock_name']
            final_answer = '{0} {1}的诊股得分是：{2}'.format(subject1, subject2, '；'.join(list(set(desc))[:self.num_limit]))

        return final_answer


if __name__ == '__main__':
    searcher = AnswerSearcher()

    #g = Graph("bolt://127.0.0.1:7687", user="neo4j", password="stayalive")
    #query = "MATCH (m:Stock20190111)-[r:ConceptLeadingInvolved]->(n:ConceptLeading) where n.name = '融资融券' return m.stock_name,n.name"
    #ress = g.run(query).data()
    #print(ress)