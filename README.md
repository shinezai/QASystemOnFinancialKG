# QABasedOnKnowledgeGraph
self-implement of stock centered Finance graph from zero to full and sever as question answering base. 从无到有搭建一个以股票为中心的一定规模金融领域知识图谱，并以该知识图谱完成自动问答与分析服务。

# 项目介绍

本项目参考了刘焕勇老师的知识图谱项目：https://github.com/liuhuanyong/QASystemOnMedicalKG
知识图谱是目前自然语言处理的一个热门方向，关于较全面的参考资料，可以查看刘焕勇老师的ccks2018参会总结(https://github.com/liuhuanyong/CCKS2018Summary )。
与知识图谱相关的另一种形态，即事理图谱，可参考：(https://github.com/liuhuanyong/ComplexEventExtraction )
本项目将包括以下两部分的内容：
1) 基于问财网站数据的金融知识图谱构建
2) 基于金融知识图谱的自动问答

# 项目最终效果
实现模板问答

# 项目运行方式
1、配置要求：要求配置neo4j数据库及相应的python依赖包。neo4j数据库用户名密码记住，并修改相应文件。
2、知识图谱数据导入：python build_stockgraph.py，导入的数据较多，估计需要几个小时。
3、启动问答：python chat_graph.py

# 以下介绍详细方案
# 一、金融知识图谱构建
# 1.1 业务驱动的知识图谱构建框架
![image](https://github.com/liuhuanyong/QABasedOnMedicalKnowledgeGraph/blob/master/img/kg_route.png)

# 1.2 脚本目录
prepare_data/build_stock_data.py：问财api获取数据生成json或者csv脚本
prepare_data/max_cut.py：基于词典的最大向前/向后切分脚本
build_stockgraph.py：知识图谱入库脚本    　　

# 1.3 金融领域知识图谱规模
1.3.1 neo4j图数据库存储规模
![image](https://github.com/liuhuanyong/QABasedOnMedicalKnowledgeGraph/blob/master/img/graph_summary.png)

1.3.2 知识图谱实体类型

| 实体类型 | 中文含义 | 实体数量 |举例 |
| :--- | :---: | :---: | :--- |
| Stock#PDATE# | 每一天的股票 | 3,359 | 平安银行;同花顺 |
| Concept | 概念 | 1121 | 迪士尼;芯片概念 |
| ConceptLeading | 概念龙头 | 205 | 迪士尼;芯片概念 |
| Controller | 实际控制人 | 2,433 | 王妙玉;李德敏 |
| Industry | 行业 | 66 | 通信服务;纺织制造 |
| IndexType | 指数类型 | 75 | 创业板50;公共指数 |
| EquityScale | 股本规模 | 4 | 小盘股;大盘股|
| MarketType | 市场类型 | 34 | 全部AB股;上证50 |
| BuySignal | 买入信号 | 36 | bias买入信号;boll突破下轨 |
| SellSignal | 卖出信号 | 24 | boll跌破上轨;kdj超买 |
| TechForm | 技术形态 | 69 | 一阳二线;横盘|
| Movement | 选股动向 | 75 | 破净;持续5天放量|
| Gender | 性别 | 2 | 男;女 |
| EducationBg | 学历 | 8 | 本科;中专 |
| Nationality | 国籍 | 25 | 中国;日本 |
| School | 学校 | 906 | 中国空军第二飞行学院;英国诺丁汉大学 |
| Title | 职务 | 1005 | 事业部总经理;技术开发总监|
| Person（包含topmanager） | 人物 | 24554 | 戴海平;杨锡洪 |
| Total | 总计 | 44,111 | 约4.4万实体量级|


1.3.3 知识图谱实体关系类型

| 实体关系类型 | 中文含义 | 关系数量 | 举例|
| :--- | :---: | :---: | :--- |
| ConceptInvolved | 所属概念 | 8,844| <平安银行,属于,转融券标的>|
| ConceptLeadingInvolved | 概念龙头 | 14,649 | <赛意信息,属于,华为概念>|
| IndustryInvolved |所属行业 | 22,238| |
| IndexTypeIs |  所属指数类 | 17,315| |
| EquityScaleIs | 股本规模 | 39,422| |
| MarketTypeIs | 股票市场类型 | 22,247| |
| TechFormIs | 技术形态 | 59,467 | |
| MovementIs | 选股动向 | 40,221 | |
| BuySignalIs | 买入信号 | 5,998 |  |
| SellSignalIs | 卖出信号 | 12,029 | |
| IsControlledBy | 实际控制人 | 294,149 | |
| TopManagerIs | 高管 | | |
| MainBusinessIs | 主营产品 | | |
| ProvinceIs | 省份 | | |
| CityIs | 城市 | | |
| SchoolIs | 毕业学校 | | |
| EducationBgIs | 学历 | | |
| NationalityIs | 国籍 | | |
| GenderIs | 性别 | | |
| TitleIs | 高管职务 | | |
| Total | 总计 | | |


1.3.4 知识图谱属性类型

| 属性类型 | 中文含义 | 举例 |
| :--- | :---: | :---: |
| stock_id | 股票6位代码 | 000001 |
| stock_code | 股票完整代码 | 000001.SZ |
| stock_name | 股票名称 | 平安银行 |
| industry_ths | 同花顺行业 | 交运设备-交运设备服务-汽车服务 |
| open | 开盘价 | 9.39 |
| close | 收盘价 | 9.39 |
| high | 最高价 | 9.5 |
| low | 最低价 | 9.37 |
| volume_rate | 量比 | 0.86 |


# 二、基于金融知识图谱的自动问答
# 2.1 技术架构
![image](https://github.com/liuhuanyong/QABasedOnMedicalKnowledgeGraph/blob/master/img/qa_route.png)

# 2.2 脚本结构
question_classifier.py：问句类型分类脚本
question_parser.py：问句解析脚本
chatbot_graph.py：问答程序脚本

# 2.3　支持问答类型

| 问句类型 | 中文含义 | 问句举例 |
| :--- | :---: | :---: |
| stockid_conceptget | 查询概念| 平安银行有哪些概念？ |
| 太多了，作者会写吐的 |


# 问答结果展示

        用户:平安银行有哪些概念？
        童尼玛: 000001 平安银行的所属概念是：巴拉巴拉
