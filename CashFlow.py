import Public
import pymongo as pm
import cx_Oracle as co
import datetime as dt
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

def CashFlow():
    connStr = Public.GetPara('connStr')
    conn = co.connect(connStr)
    mongoConn = Public.GetPara('mongoConn')
    mc = pm.MongoClient(mongoConn)
    db = mc['factor']

    # factor
    configDictList = [{'facName': 'OCF_Q', 'column': 5, 'qfData': 2},        # 季度经营活动现金流
                      {'facName': 'OCF_QG', 'column': 5, 'qfData': 5},       # 季度经营活动现金流增长率
                      {'facName': 'OCF_12M', 'column': 5, 'qfData': 3},      # 12个月经营活动现金流
                      {'facName': 'OCE_12MG', 'column': 5, 'qfData': 6},     # 12个月经营活动现金流增长率

                      {'facName': 'ICF_Q', 'column': 8, 'qfData': 2},        # 季度投资活动现金流
                      {'facName': 'ICF_QG', 'column': 8, 'qfData': 5},       # 季度投资活动现金流增长率
                      {'facName': 'ICF_12M', 'column': 8, 'qfData': 3},      # 12个月投资活动现金流
                      {'facName': 'ICE_12MG', 'column': 8, 'qfData': 6},     # 12个月投资活动现金流增长率

                      {'facName': 'FCF_Q', 'column': 11, 'qfData': 2},       # 季度筹资活动现金流
                      {'facName': 'FCF_QG', 'column': 11, 'qfData': 5},      # 季度筹资活动现金流增长率
                      {'facName': 'FCF_12M', 'column': 11, 'qfData': 3},     # 12个月筹资活动现金流
                      {'facName': 'FCE_12MG', 'column': 11, 'qfData': 6},    # 12个月筹资活动现金流增长率
                     ]
    for configDict in configDictList:
        facName = configDict['facName']
        print('==== ' + facName)
        lastUpdateDate = Public.GetLastUpdateDate(facName, mc)
        dataList = Public.GetDataList(Public.sqlCash,
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


CashFlow()
