#!/usr/bin/env python3
# coding: utf-8
# File: chatbot_graph.py
# Author: https://github.com/shinezai
# Date: 19-02

from question_classifier import *
from question_parser import *
from answer_search import *

'''问答类'''
class ChatBotGraph():
    def __init__(self, pDate):
        self.classifier = QuestionClassifier()
        self.parser = QuestionPaser(pDate)
        self.searcher = AnswerSearcher()

    def chat_kg_main(self, sent):
        answer = '嘤嘤嘤，你还真问倒我了呢，尼玛！'
        res_classify = self.classifier.classify(sent)

        #print(res_classify)

        if not res_classify:
            return '超出尼玛的知识范围了，我知道你现在想呼唤我的名字'

        if res_classify['question_types'] == ['sensitive']:
            return '我也是个暴脾气，信不信我骂回去'

        res_sql = self.parser.parser_main(res_classify)
        final_answers = self.searcher.search_main(res_sql)
        if not final_answers:
            return answer
        else:
            return '\n'.join(final_answers)

if __name__ == '__main__':
    #选择当天时间
    pDate = "20190123"
    handler = ChatBotGraph(pDate)
    print('童尼玛:', '您好，我是童尼玛，欢迎向我提问，如果答不上来，请呼唤我的名字尼玛！')
    while 1:
        question = input('用户:')
        USE_KG = True
        if USE_KG:
            answer = handler.chat_kg_main(question)
        else:
            answer = handler.chat_interface_main(question)
        print('童尼玛:', answer)

