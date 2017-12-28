import Public
import pymongo as pm
import cx_Oracle as co
import datetime as dt
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

def Profit():
    connStr = Public.GetPara('connStr')
    conn = co.connect(connStr)
    mongoConn = Public.GetPara('mongoConn')
    mc = pm.MongoClient(mongoConn)
    db = mc['factor']

    # factor
    configDictList = [{'facName': 'NP_Q', 'column': 12, 'qfData': 2},        # 季度净利润
                      {'facName': 'NP_QG', 'column': 12, 'qfData': 5},       # 季度净利润增长率
                      {'facName': 'NP_12M', 'column': 12, 'qfData': 3},      # 12个月净利润
                      {'facName': 'NP_12MG', 'column': 12, 'qfData': 6},     # 12个月净利润增长率

                      {'facName': 'NPAPC_Q', 'column': 13, 'qfData': 2},     # 季度归母净利润
                      {'facName': 'NPAPC_QG', 'column': 13, 'qfData': 5},    # 季度归母净利润增长率
                      {'facName': 'NPAPC_12M', 'column': 13, 'qfData': 3},   # 12个月归母净利润
                      {'facName': 'NPAPC_12MG', 'column': 13, 'qfData': 6},  # 12个月归母净利润增长率

                      {'facName': 'OTR_Q', 'column': 3, 'qfData': 2},        # 季度营业总收入
                      {'facName': 'OTR_QG', 'column': 3, 'qfData': 5},       # 季度营业总收入增长率
                      {'facName': 'OTR_12M', 'column': 3, 'qfData': 3},      # 12个月营业总收入
                      {'facName': 'OTR_12MG', 'column': 3, 'qfData': 6},     # 12个月营业总收入增长率

                      {'facName': 'OR_Q', 'column': 4, 'qfData': 2},         # 季度营业收入
                      {'facName': 'OR_QG', 'column': 4, 'qfData': 5},        # 季度营业收入增长率
                      {'facName': 'OR_12M', 'column': 4, 'qfData': 3},       # 12个月营业收入
                      {'facName': 'OR_12MG', 'column': 4, 'qfData': 6},      # 12个月营业收入增长率

                      {'facName': 'OP_Q', 'column': 9, 'qfData': 2},         # 季度营业利润
                      {'facName': 'OP_QG', 'column': 9, 'qfData': 5},        # 季度营业利润增长率
                      {'facName': 'OP_12M', 'column': 9, 'qfData': 3},       # 12个月营业利润
                      {'facName': 'OP_12MG', 'column': 9, 'qfData': 6},      # 12个月营业利润增长率

                      {'facName': 'TP_Q', 'column': 10, 'qfData': 2},        # 季度利润总额
                      {'facName': 'TP_QG', 'column': 10, 'qfData': 5},       # 季度利润总额增长率
                      {'facName': 'TP_12M', 'column': 10, 'qfData': 3},      # 12个月利润总额
                      {'facName': 'TP_12MG', 'column': 10, 'qfData': 6},     # 12个月利润总额增长率

                      {'facName': 'OTC_Q', 'column': 6, 'qfData': 2},        # 季度营业总成本
                      {'facName': 'OTC_12M', 'column': 6, 'qfData': 3},      # 12个月营业总成本
                      {'facName': 'OC_Q', 'column': 7, 'qfData': 2},         # 季度营业成本
                      {'facName': 'OC_12M', 'column': 7, 'qfData': 3},       # 12个月营业成本
                      {'facName': 'ITE_Q', 'column': 11, 'qfData': 2},       # 季度所得税费用
                      {'facName': 'ITE_12M', 'column': 11, 'qfData': 3}      # 12个月所得税费用
                        ]
    for configDict in configDictList:
        facName = configDict['facName']
        print('==== ' + facName)
        lastUpdateDate = Public.GetLastUpdateDate(facName, mc)
        dataList = Public.GetDataList(Public.sqlPrf,
                                      lastUpdateDate - dt.timedelta(days=365 * 3),
                                      dt.datetime.now(),
                                      conn)
        qfDataTuple = Public.QuarterFormat(dataList)
        column = configDict['column']
        dataDict = qfDataTuple[configDict['qfData']]
        facDict = {}
        for symbol, data in dataDict.items():
            facDict[symbol] = data[:, [0, 1, 2, column]]
        Public.ToDB(facDict, facName, endDate=None, updateReportDate=True, mongoClient=mc)


    ddd = 0


Profit()
