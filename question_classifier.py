#!/usr/bin/env python3
# coding: utf-8
# File: question_classifier.py
# Author: https://github.com/shinezai
# Date: 19-02

import os
import ahocorasick

class QuestionClassifier:
    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])

        # 股票特征词路径
        self.stockid_path = os.path.join(cur_dir, 'stock_dict/stockid.txt')
        self.stockname_path = os.path.join(cur_dir, 'stock_dict/stockname.txt')
        self.concept_path = os.path.join(cur_dir, 'stock_dict/concept.txt')
        self.conceptleading_path = os.path.join(cur_dir, 'stock_dict/conceptleading.txt')
        self.controller_path = os.path.join(cur_dir, 'stock_dict/controller.txt')
        self.industry_path = os.path.join(cur_dir, 'stock_dict/industry.txt')
        self.indextype_path = os.path.join(cur_dir, 'stock_dict/indextype.txt')
        self.equityscale_path = os.path.join(cur_dir, 'stock_dict/equityscale.txt')
        self.marketype_path = os.path.join(cur_dir, 'stock_dict/marketype.txt')
        self.buysignal_path = os.path.join(cur_dir, 'stock_dict/buysignal.txt')
        self.sellsignal_path = os.path.join(cur_dir, 'stock_dict/sellsignal.txt')
        self.techform_path = os.path.join(cur_dir, 'stock_dict/techform.txt')
        self.movement_path = os.path.join(cur_dir, 'stock_dict/movement.txt')
        self.sensitive_path = os.path.join(cur_dir, 'stock_dict/sensitive.txt')
        self.gender_path = os.path.join(cur_dir, 'stock_dict/gender.txt')
        self.educationgb_path = os.path.join(cur_dir, 'stock_dict/educationbg.txt')
        self.nationality_path = os.path.join(cur_dir, 'stock_dict/nationality.txt')
        self.school_path = os.path.join(cur_dir, 'stock_dict/school.txt')
        self.title_path = os.path.join(cur_dir, 'stock_dict/title.txt')
        self.topmanager_path = os.path.join(cur_dir, 'stock_dict/topmanager.txt')
        #self.stock_deny_path = os.path.join(cur_dir, 'stock_dict/stockdeny.txt')

        #股票
        # 加载特征词
        self.stockid_wds = [i.strip() for i in open(self.stockid_path, encoding='utf-8') if i.strip()]
        self.stockname_wds = [i.strip() for i in open(self.stockname_path, encoding='utf-8') if i.strip()]
        self.concept_wds = [i.strip() for i in open(self.concept_path, encoding='utf-8') if i.strip()]
        self.conceptleading_wds = [i.strip() for i in open(self.conceptleading_path, encoding='utf-8') if i.strip()]
        self.controller_wds = [i.strip() for i in open(self.controller_path, encoding='utf-8') if i.strip()]
        self.industry_wds = [i.strip() for i in open(self.industry_path, encoding='utf-8') if i.strip()]
        self.indextype_wds = [i.strip() for i in open(self.indextype_path, encoding='utf-8') if i.strip()]
        self.equityscale_wds = [i.strip() for i in open(self.equityscale_path, encoding='utf-8') if i.strip()]
        self.markettype_wds = [i.strip() for i in open(self.marketype_path, encoding='utf-8') if i.strip()]
        self.buysignal_wds = [i.strip() for i in open(self.buysignal_path, encoding='utf-8') if i.strip()]
        self.sellsignal_wds = [i.strip() for i in open(self.sellsignal_path, encoding='utf-8') if i.strip()]
        self.techform_wds = [i.strip() for i in open(self.techform_path, encoding='utf-8') if i.strip()]
        self.movement_wds = [i.strip() for i in open(self.sellsignal_path, encoding='utf-8') if i.strip()]
        self.sensitive_wds = [i.strip() for i in open(self.sensitive_path, encoding='utf-8') if i.strip()]
        self.gender_wds = [i.strip() for i in open(self.gender_path, encoding='utf-8') if i.strip()]
        self.educationbg_wds = [i.strip() for i in open(self.educationgb_path, encoding='utf-8') if i.strip()]
        self.nationality_wds = [i.strip() for i in open(self.nationality_path, encoding='utf-8') if i.strip()]
        self.school_wds = [i.strip() for i in open(self.school_path, encoding='utf-8') if i.strip()]
        self.title_wds = [i.strip() for i in open(self.title_path, encoding='utf-8') if i.strip()]
        self.topmanager_wds = [i.strip() for i in open(self.topmanager_path, encoding='utf-8') if i.strip()]
        self.stock_region_words = set(self.stockid_wds + self.stockname_wds +
            self.concept_wds + self.conceptleading_wds + self.controller_wds + self.industry_wds + self.indextype_wds +
            self.equityscale_wds + self.markettype_wds + self.buysignal_wds + self.sellsignal_wds + self.techform_wds +
            self.movement_wds + self.sensitive_wds + self.gender_wds + self.educationbg_wds + self.nationality_wds +
            self.school_wds + self.title_wds + self.topmanager_wds)
        #self.stock_deny_words = [i.strip() for i in open(self.stock_deny_path, encoding='utf-8') if i.strip()]
        # 构造领域actree
        self.stock_region_tree = self.build_actree(list(self.stock_region_words))
        # 构建词典
        self.stock_wdtype_dict = self.build_wdtype_stock_dict()
        # 问句疑问词
        #【0】概念
        self.concept_qwds = ['所属概念', '什么概念', '概念类别', '概念是什么', '啥概念', '概念是啥', '嘛概念', '神马概念', '概念']
        #【1】龙头概念
        self.conceptleading_qwds = ['龙头', '龙头股']
        #【2】实际控制人
        self.controller_qwds = ['是谁', '控制', '大股东', '股东', '老板', '控股人']
        #【3】行业
        self.industry_qwds = ['所属行业', '什么行业', '行业是什么', '行业是啥', '啥行业', '嘛行业', '神马行业', '行业类别', '行业']
        #【4】指数类型
        self.indextype_qwds = ['所属指数', '什么指数', '指数类', '指数']
        #【5】股本规模
        self.equityscale_qwds = ['股本规模', '什么盘子', '盘子大小', '盘子', '股本大小']
        #【6】市场类型
        self.marketype_qwds = ['市场类型', '股票市场'] #属于什么市场
        #【7】买入信号
        self.buysignal_qwds = ['买入信号', '买入']
        #【8】卖出信号
        self.sellsignal_qwds = ['卖出信号', '卖出']
        #【9】技术形态
        self.techform_qwds = ['技术形态', 'K线形态', 'K线走势', '走势', '技术']
        #【10】选股动向
        self.movement_qwds = ['动向', '选择', '选股', '建仓']
        #【11】性别
        self.gender_qwds = ['性别']
        #【12】学历
        self.educationgb_qwds = ['学历']
        #【13】国籍
        self.nationality_qwds = ['国籍']
        #【14】学校
        self.school_qwds = ['毕业', '学校']
        #【15】职务
        self.title_qwds = ['职位', '职务']
        #【16】高管
        self.topmanager_qwds = ['高管']

        self.stock_belong_qwds = ['属于', '所属', '拥有', '包含', '含有']
        self.performance_qwds = ['好不好', '好吗', '牛逼', '牛', '厉害', '蓝筹', '牛股', '怎样', '如何', '好么', '牛不', '屌']
        self.common_qwds = ['指标', '特性', '形态', '表现']

        self.sub_equityscale_qwds = ['超大盘', '大盘', '中盘', '小盘', '中小盘']

        print('model init finished ......')

        return

    '''分类主函数'''
    def classify(self, question):
        data = {}
        stock_dict = self.check_stock(question)
        if not stock_dict:
            return {}
        data['args'] = stock_dict
        #收集问句当中所涉及到的实体类型
        types = []

        for type_ in stock_dict.values():
            types += type_

        question_type = 'others'

        question_types = []

        #股票问答
        #概念+股本规模查股
        '''
        if self.check_words(self.equityscale_qwds+self.sub_equityscale_qwds+self.concept_qwds, question) and 'concept' in types:
            zxpan_status = self.check_words(['中小盘'], question)
            if zxpan_status == True:
                question_type = 'zxpan_concept_stockget'
            else:
                question_type = 'equityscale_concept_stockget'
            question_types.append(question_type)'''

        #查询概念
        if self.check_words(self.concept_qwds, question) and 'stockid' in types:
            question_type = 'stockid_conceptget'
            question_types.append(question_type)

        if self.check_words(self.concept_qwds, question) and 'stockname' in types:
            question_type = 'stockname_conceptget'
            question_types.append(question_type)

        #根据概念查股
        if self.check_words(self.concept_qwds, question) and 'concept' in types:
            question_type = 'concept_stockget'
            question_types.append(question_type)

        #查询概念龙头股
        if self.check_words(self.conceptleading_qwds, question) and 'conceptleading' in types:
            question_type = 'conceptleading_stockget'
            #print(question_type)
            question_types.append(question_type)

        #控制人查询
        if self.check_words(self.controller_qwds, question) and 'stockid' in types:
            question_type = 'stockid_controllerget'
            question_types.append(question_type)

        if self.check_words(self.controller_qwds, question) and 'stockname' in types:
            question_type = 'stockname_controllerget'
            question_types.append(question_type)

        # 根据控制人查股
        if self.check_words(self.controller_qwds, question) and 'controller' in types:
            question_type = 'controller_stockget'
            question_types.append(question_type)

        #查询行业
        if self.check_words(self.industry_qwds, question) and 'stockid' in types:
            question_type = 'stockid_industryget'
            question_types.append(question_type)

        if self.check_words(self.industry_qwds, question) and 'stockname' in types:
            question_type = 'stockname_industryget'
            question_types.append(question_type)

        #根据行业查询股票
        if self.check_words(self.industry_qwds+self.stock_belong_qwds, question) and 'industry' in types:
            question_type = 'industry_stockget'
            question_types.append(question_type)

        #查询指数
        if self.check_words(self.indextype_qwds, question) and 'stockid' in types:
            question_type = 'stockid_indextypeget'
            question_types.append(question_type)

        if self.check_words(self.indextype_qwds, question) and 'stockname' in types:
            question_type = 'stockname_indextypeget'
            question_types.append(question_type)

        #根据指数查询股票
        if self.check_words(self.indextype_qwds+self.stock_belong_qwds, question) and 'indextype' in types:
            question_type = 'indextype_stockget'
            print(question_type)
            question_types.append(question_type)

        #查询股本规模
        if self.check_words(self.equityscale_qwds, question) and 'stockid' in types:
            question_type = 'stockid_equityscaleget'
            question_types.append(question_type)

        if self.check_words(self.equityscale_qwds, question) and 'stockname' in types:
            question_type = 'stockname_equityscaleget'
            question_types.append(question_type)

        #根据股本规模查股
        if self.check_words(self.equityscale_qwds+self.stock_belong_qwds, question) and 'equityscale' in types:
            question_type = 'equityscale_stockget'
            question_types.append(question_type)

        # 查询市场类型
        if self.check_words(self.marketype_qwds, question) and 'stockid' in types:
            question_type = 'stockid_markettypeget'
            question_types.append(question_type)

        if self.check_words(self.marketype_qwds, question) and 'stockname' in types:
            question_type = 'stockname_markettypeget'
            question_types.append(question_type)

        # 根据市场类型查股
        if self.check_words(self.marketype_qwds + self.stock_belong_qwds, question) and 'markettype' in types:
            question_type = 'markettype_stockget'
            question_types.append(question_type)

        #查询买入信号
        if self.check_words(self.buysignal_qwds, question) and 'stockid' in types:
            question_type = 'stockid_buysignalget'
            question_types.append(question_type)

        if self.check_words(self.buysignal_qwds, question) and 'stockname' in types:
            question_type = 'stockname_buysignalget'
            question_types.append(question_type)

        #根据买入信号查询股
        if self.check_words(self.buysignal_qwds + self.stock_belong_qwds + self.common_qwds, question) and 'buysignal' in types:
            question_type = 'buysignal_stockget'
            question_types.append(question_type)

        # 查询卖出信号
        if self.check_words(self.sellsignal_qwds, question) and 'stockid' in types:
            question_type = 'stockid_sellsignalget'
            question_types.append(question_type)

        if self.check_words(self.sellsignal_qwds, question) and 'stockname' in types:
            question_type = 'stockname_sellsignalget'
            question_types.append(question_type)

        # 根据卖出信号查询股
        if self.check_words(self.sellsignal_qwds + self.stock_belong_qwds + self.common_qwds, question) and 'sellsignal' in types:
            question_type = 'sellsignal_stockget'
            question_types.append(question_type)

        # 查询技术形态
        if self.check_words(self.techform_qwds+self.common_qwds, question) and 'stockid' in types:
            question_type = 'stockid_techformget'
            question_types.append(question_type)

        if self.check_words(self.techform_qwds+self.common_qwds, question) and 'stockname' in types:
            question_type = 'stockname_techformget'
            question_types.append(question_type)

        # 根据技术形态查询股
        if self.check_words(self.techform_qwds + self.stock_belong_qwds + self.common_qwds,
                            question) and 'techform' in types:
            question_type = 'techform_stockget'
            question_types.append(question_type)

        # 查询选股动向
        if self.check_words(self.movement_qwds, question) and 'stockid' in types:
            question_type = 'stockid_movementget'
            question_types.append(question_type)

        if self.check_words(self.movement_qwds, question) and 'stockname' in types:
            question_type = 'stockname_movementget'
            question_types.append(question_type)

        # 根据选股动向询股
        if self.check_words(self.movement_qwds + self.stock_belong_qwds + self.common_qwds,
                            question) and 'movement' in types:
            question_type = 'movement_stockget'
            question_types.append(question_type)

        #诊股得分
        if self.check_words(self.performance_qwds, question) and 'stockid' in types:
            question_type = 'stockid_scoreget'
            question_types.append(question_type)

        if self.check_words(self.performance_qwds, question) and 'stockname' in types:
            question_type = 'stockname_scoreget'
            question_types.append(question_type)

        # 高管姓名查询
        if self.check_words(self.topmanager_wds, question) and 'stockid' in types:
            question_type = 'stockid_topmanagerget'
            question_types.append(question_type)

        if self.check_words(self.topmanager_wds, question) and 'stockname' in types:
            question_type = 'stockname_topmanagerget'
            question_types.append(question_type)

        if question_types == [] and 'sensitive' in types:
            question_types = ['sensitive']

        # 将多个分类结果进行合并处理，组装成一个字典
        data['question_types'] = question_types

        return data

    '''构造词对应的类型'''

    def build_wdtype_stock_dict(self):
        wd_dict = dict()
        for wd in self.stock_region_words:
            wd_dict[wd] = []
            if wd in self.stockid_wds:
                wd_dict[wd].append('stockid')
            if wd in self.stockname_wds:
                wd_dict[wd].append('stockname')
            if wd in self.concept_wds:
                wd_dict[wd].append('concept')
            if wd in self.conceptleading_wds: #概念龙头需排在概念后面
                wd_dict[wd].append('conceptleading')
            if wd in self.controller_wds:
                wd_dict[wd].append('controller')
            if wd in self.industry_wds:
                wd_dict[wd].append('industry')
            if wd in self.indextype_wds:
                wd_dict[wd].append('indextype')
            if wd in self.equityscale_wds:
                wd_dict[wd].append('equityscale')
            if wd in self.markettype_wds:
                wd_dict[wd].append('markettype')
            if wd in self.buysignal_wds:
                wd_dict[wd].append('buysignal')
            if wd in self.sellsignal_wds:
                wd_dict[wd].append('sellsignal')
            if wd in self.techform_wds:
                wd_dict[wd].append('techform')
            if wd in self.movement_wds:
                wd_dict[wd].append('movement')
            if wd in self.sensitive_wds:
                wd_dict[wd].append('sensitive')
            if wd in self.gender_wds:
                wd_dict[wd].append('gender')
            if wd in self.educationbg_wds:
                wd_dict[wd].append('educationbg')
            if wd in self.school_wds:
                wd_dict[wd].append('school')
            if wd in self.nationality_wds:
                wd_dict[wd].append('nationality')
            if wd in self.topmanager_wds:
                wd_dict[wd].append('topmanager')
            if wd in self.title_wds:
                wd_dict[wd].append('title')

        return wd_dict

    '''构造actree，加速过滤'''
    def build_actree(self, wordlist):
        actree = ahocorasick.Automaton()
        for index, word in enumerate(wordlist):
            actree.add_word(word, (index, word))
        actree.make_automaton()
        return actree

    '''股票问句过滤'''
    def check_stock(self, question):
        region_wds = []
        for i in self.stock_region_tree.iter(question):
            wd = i[1][1]
            region_wds.append(wd)
        stop_wds = []
        for wd1 in region_wds:
            for wd2 in region_wds:
                if wd1 in wd2 and wd1 != wd2:
                    stop_wds.append(wd1) #停用词
        final_wds = [i for i in region_wds if i not in stop_wds]
        final_dict = {i: self.stock_wdtype_dict.get(i) for i in final_wds}

        return final_dict

    '''基于特征词进行分类'''
    def check_words(self, wds, sent):
        for wd in wds:
            if wd in sent:
                return True
        return False


if __name__ == '__main__':
    handler = QuestionClassifier()
    while 1:
        question = input('input an question:')
        data = handler.classify(question)
        print(data)