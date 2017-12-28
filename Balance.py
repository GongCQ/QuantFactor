import Public
import pymongo as pm
import cx_Oracle as co
import datetime as dt
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

def Balance():
    connStr = Public.GetPara('connStr')
    conn = co.connect(connStr)
    mongoConn = Public.GetPara('mongoConn')
    mc = pm.MongoClient(mongoConn)
    db = mc['factor']

    # factor
    configDictList = [{'facName': 'MC', 'column': 3, 'qfData': 1},        # 货币资金
                      {'facName': 'LA', 'column': 4, 'qfData': 1},        # 流动资产
                      {'facName': 'FA', 'column': 5, 'qfData': 1},        # 固定资产
                      {'facName': 'IA', 'column': 6, 'qfData': 1},        # 无形资产
                      {'facName': 'TNLA', 'column': 7, 'qfData': 1},      # 非流动资产合计
                      {'facName': 'TA', 'column': 8, 'qfData': 1},        # 资产总计
                      {'facName': 'STB', 'column': 9, 'qfData': 1},       # 短期借款
                      {'facName': 'NLL1Y', 'column': 10, 'qfData': 1},    # 一年内到期的非流动负债
                      {'facName': 'TLL', 'column': 11, 'qfData': 1},      # 流动负债合计
                      {'facName': 'LTB', 'column': 12, 'qfData': 1},      # 长期借款
                      {'facName': 'TNLL', 'column': 13, 'qfData': 1},     # 非流动负债合计
                      {'facName': 'TL', 'column': 14, 'qfData': 1},       # 负债合计
                      {'facName': 'PUC', 'column': 15, 'qfData': 1},      # 实收资本(或股本)
                      {'facName': 'UDP', 'column': 16, 'qfData': 1},      # 未分配利润
                      {'facName': 'TOEAPC', 'column': 17, 'qfData': 1},   # 归母权益合计
                      {'facName': 'MSE', 'column': 18, 'qfData': 1},      # 少数股东权益
                      {'facName': 'TOE', 'column': 19, 'qfData': 1},      # 所有者权益合计
                      {'facName': 'TLOE', 'column': 20, 'qfData': 1},     # 负债和所有者权益合计
                      {'facName': 'IVT', 'column': 21, 'qfData': 1},      # 存货

                      {'facName': 'TA_G', 'column': 8, 'qfData': 4},      # 总资产增长率
                      {'facName': 'TL_G', 'column': 14, 'qfData': 4},     # 总负债增长率
                      {'facName': 'TOEAPC_G', 'column': 17, 'qfData': 4}, # 归母权益增长率
                      {'facName': 'TOE_G', 'column': 19, 'qfData': 4},    # 所有者权益增长率

                      {'facName': 'TA_QA', 'column': 8, 'qfData': 7},     # 季度总资产平均值
                      {'facName': 'TOEAPC_QA', 'column': 17, 'qfData': 7},# 季度归母权益平均值
                      {'facName': 'TOE_QA', 'column': 19, 'qfData': 7},   # 季度所有者权益平均值
                      {'facName': 'IVT_QA', 'column': 21, 'qfData': 7},   # 季度存货平均值
                      {'facName': 'TA_12MA', 'column': 8, 'qfData': 8},   # 12个月总资产平均值
                      {'facName': 'TOEAPC_12MA', 'column': 17, 'qfData': 8},# 12个月归母权益平均值
                      {'facName': 'TOE_12MA', 'column': 19, 'qfData': 8}, # 12个月所有者权益平均值
                      {'facName': 'IVT_12MA', 'column': 21, 'qfData': 8}  # 12个月存货平均值
                    ]
    for configDict in configDictList:
        facName = configDict['facName']
        print('==== ' + facName)
        lastUpdateDate = Public.GetLastUpdateDate(facName, mc)
        dataList = Public.GetDataList(Public.sqlBlc,
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


Balance()
