import fileinput as fi
import gongcq.Public as Public
import os
import datetime as dt
import warnings
import numpy as np
import time
import cx_Oracle as co
import pymongo as pm
import sys

MIN_DATE = dt.datetime(2004, 12, 31)
MIN_QUARTER = dt.datetime(2004, 12, 31)

# 0.截止日期 1.股票代码 2.公告日期
# 3.营业总收入(非金融类) 4.营业收入 5.其他营业收入(非金融类)
# 6.营业总成本(非金融类) 7.营业成本(非金融类) 8.其他营业成本
# 9.营业利润 10.利润总额 11.减：所得税费用
# 12.净利润 13.归属于母公司所有者的净利润 14.少数股东损益
sqlPrf =  "SELECT P.END_DATE, CONCAT(I.STK_CODE, '_CS'), P.INFO_PUB_DATE,  " \
          "       P.IS_10001/*营业总收入(非金融类)*/, P.IS_10000/*营业收入*/, P.IS_10004/*其他营业收入(非金融类)*/,  " \
          "       P.IS_20005/*营业总成本(非金融类)*/, P.IS_20000/*营业成本(非金融类)*/, P.IS_20006/*其他营业成本*/,  " \
          "       P.IS_30000/*营业利润*/, P.IS_40000/*利润总额*/, P.IS_40001/*减：所得税费用*/,  " \
          "       P.IS_50000/*净利润*/, P.IS_50001/*归属于母公司所有者的净利润*/, P.IS_50004/*少数股东损益*/  " \
          "FROM UPCENTER.FIN_INCO_SHORT P JOIN UPCENTER.STK_BASIC_INFO I " \
          "     ON P.COM_UNI_CODE = I.COM_UNI_CODE AND P.ISVALID=1 AND I.ISVALID=1  " \
          "WHERE (I.SEC_MAR_PAR BETWEEN 1 AND 2) AND P.END_DATE BETWEEN DATE'{BEGIN_DATE}' AND DATE'{END_DATE}' "
# 0.截止日期 1.股票代码 2.公告日期
# 3.货币资金及现金或存放中央银行款项 4.流动资产合计
# 5.固定资产 6.无形资产 7.非流动资产合计 8.资产总计
# 9.短期借款 10.一年内到期的非流动负债 11.流动负债合计
# 12.长期借款 13.非流动负债合计 14.负债合计
# 15.实收资本(或股本) 16.未分配利润 17.归属母公司权益合计
# 18.少数股东权益 19.所有者权益合计 20.负债和所有者权益合计
# 21.存货
sqlBlc = "SELECT B.END_DATE, CONCAT(I.STK_CODE, '_CS'), B.INFO_PUB_DATE,  " \
	     "       B.BS_11001/*货币资金及现金或存放中央银行款项*/, B.BS_11000/*流动资产合计*/,  " \
	     "       B.BS_12001/*固定资产*/, B.BS_12046/*无形资产*/, B.BS_12000/*非流动资产合计*/, B.BS_10000/*资产总计*/, " \
	     "       B.BS_21003/*短期借款*/, B.BS_21097/*一年内到期的非流动负债*/, B.BS_21000/*流动负债合计*/,  " \
	     "       B.BS_22001/*长期借款*/, B.BS_22000/*非流动负债合计*/, B.BS_20000/*负债合计*/,  " \
	     "       B.BS_30001/*实收资本(或股本)*/, B.BS_30004/*未分配利润*/, B.BS_31000/*归属母公司权益合计*/,  " \
	     "       B.BS_32000/*少数股东权益*/, B.BS_30000/*所有者权益合计*/, B.BS_40000/*负债和所有者权益合计*/, " \
         "       B.BS_11003/*存货*/ " \
         "FROM UPCENTER.FIN_BALA_SHORT B JOIN UPCENTER.STK_BASIC_INFO I " \
         "     ON B.COM_UNI_CODE = I.COM_UNI_CODE AND B.ISVALID=1 AND I.ISVALID=1  " \
         "WHERE (I.SEC_MAR_PAR BETWEEN 1 AND 2) AND B.END_DATE BETWEEN DATE'{BEGIN_DATE}' AND DATE'{END_DATE}' "
# 0.截止日期 1.股票代码 2.公告日期
# 3.经营活动现金流入小计 4.经营活动现金流出小计 5.经营活动现金流量净额
# 6.投资活动现金流入小计 7.投资活动现金流出小计 8.投资活动现金流量净额
# 9.筹资活动现金流入小计 10.筹资活动现金流出小计 11.筹资活动现金流量净额
# 12.现金及等价物净增加额 13.加：期初现金及现金等价物余额 14.期末现金及现金等价物余额
sqlCash = "SELECT C.END_DATE, CONCAT(I.STK_CODE, '_CS'), C.INFO_PUB_DATE, " \
	      "       C.CS_11000/*经营活动现金流入小计*/, C.CS_12000/*经营活动现金流出小计*/, C.CS_10000/*经营活动现金流量净额*/,  " \
	      "       C.CS_21000/*投资活动现金流入小计*/, C.CS_22000/*投资活动现金流出小计*/, C.CS_20000/*投资活动现金流量净额*/,  " \
	      "       C.CS_31000/*筹资活动现金流入小计*/, C.CS_32000/*筹资活动现金流出小计*/, C.CS_30000/*筹资活动现金流量净额*/,  " \
	      "       C.CS_40000/*现金及等价物净增加额*/, C.CS_50001/*加：期初现金及现金等价物余额*/, C.CS_50000/*期末现金及现金等价物余额*/  " \
          "FROM UPCENTER.FIN_CASH_SHORT C JOIN UPCENTER.STK_BASIC_INFO I " \
          "     ON C.COM_UNI_CODE = I.COM_UNI_CODE AND C.ISVALID=1 AND I.ISVALID=1  " \
          "WHERE (I.SEC_MAR_PAR BETWEEN 1 AND 2) AND C.END_DATE BETWEEN DATE'{BEGIN_DATE}' AND DATE'{END_DATE}' "

def GetPara(paraName=None):
    paraDict = Public.GetPara(os.path.join('.', 'config', 'para.txt'))
    if paraName is None:
        return paraDict
    else:
        return paraDict[paraName]

def GetQuarters(date, num):
    '''
    get a quarter list in which all quarters are earlier than a special date, sorted descendingly
    :param date: a special date
    :param num: the number of quarters that you want to get
    :return: a quarter list, which is sorted descendingly
    '''
    endDayList = [31, 31, 30, 30]
    monthList = [12, 3, 6, 9]
    monthSeq = int((date.month - 1) / 3)
    month = monthList[monthSeq]
    year = date.year if month != 12 else (date.year - 1)
    day = endDayList[monthSeq]
    quarter = dt.datetime(year, month, day)
    quarterList = [quarter]
    if num > 1:
        prior = GetQuarters(quarter, num - 1)
        quarterList.extend(prior)
        return quarterList
    else:
        return quarterList

def ValidQuarter(date):
    if date.month == 3 and date.day == 31 or \
       date.month == 6 and date.day == 30 or \
       date.month == 9 and date.day == 30 or \
       date.month == 12 and date.day == 31:
        return True
    else:
        return False

def GetYears(date, num):
    '''
    get a year list in which all years are earlier than a special date, sorted descendingly
    :param date: a special date
    :param num: the number of years that you want to get
    :return: a year list, which is sorted descendingly
    '''
    yearList = [dt.datetime(date.year - 1, 12, 31)]
    while len(yearList) < num:
        yearList.append(dt.datetime(yearList[-1].year - 1, 12, 31))
    return yearList

def Growth(X0, X1) :
    g = np.nan * np.zeros([len(X0)])
    for i in range(max(len(X0), len(X1))):
        r = -np.inf if X1[i] < 0 else ((X1[i] / X0[i] - 1) if X0[i] > 0 else np.inf)
        g[i] = r
    return g

def GetDataList(sql, beginDate, endDate, conn = None):
    if conn is None:
        connStr = GetPara('connStr')
        conn = co.connect(connStr)
    cursor = conn.cursor()
    cursor.execute(sql.replace('{BEGIN_DATE}', beginDate.strftime('%Y-%m-%d')).replace('{END_DATE}', endDate.strftime('%Y-%m-%d')))
    dataList = cursor.fetchall()
    return dataList

def QuarterFormat(dataList):
    '''
    sort the data by quarter format for each symbol
    :param dataList: a 2D list, in which the first column is a quarter date, and the second column is a symbol.
    :return: a dict, which's key is symbol and value is a list which's element is sorted by quarter date.
    '''

    maxQ = dt.datetime.min
    minQ = dt.datetime.max
    for data in dataList:
        if ValidQuarter(data[0]):
            maxQ = data[0] if (data[0] > maxQ and ValidQuarter(data[0])) else maxQ
            minQ = data[0] if (data[0] < minQ and ValidQuarter(data[0])) else minQ
    if maxQ < minQ:
        warnings.warn('can not find any valid data in dataList, return None')
        return None, None, None, None, None, None, None

    SpanQ = lambda qua0, qua1 : ((qua1.year - qua0.year) * 12 + (qua1.month - qua0.month)) / 3 + 1
    sq = SpanQ(minQ, maxQ)
    if int(sq) != sq:
        raise Exception('invalid span between max quarter and min quarter, '
                        'it seems that some invalid quarter have  sneaked.')
    sq = int(sq)
    quaList = GetQuarters(maxQ + dt.timedelta(days=1), sq)
    quaList.reverse()
    if sq != len(quaList):
        raise Exception('invalid length of quaList, which must be equal to sq.')

    GetNanArr = lambda : np.nan * np.zeros([sq, len(dataList[0])])

    # original data
    dataDict = {}
    for row in dataList:
        if not ValidQuarter(row[0]):
            warnings.warn('record will be ignored because of invalid quarter ' +
                          str(row[0]) + ', symbol is ' + str(row[1]))
            continue
        if row[2] is None:
            warnings.warn('record will be ignored because of invalid public date ' +
                          str(row[2]) + ', symbol is ' + str(row[1]))
            continue
        symbol = row[1]
        quarter = row[0]
        seqQ = int(SpanQ(minQ, quarter)) - 1
        if symbol not in dataDict.keys():
            dataDict[symbol] = GetNanArr()
        rowCopy = [time.mktime(row[0].timetuple()), np.nan, time.mktime(row[2].timetuple())]
        rowCopy.extend(row[3 : ])
        dataDict[symbol][seqQ] = np.array(rowCopy)

    # quarter data
    dataDictQ = {}
    for symbol, data in dataDict.items():
        dataDictQ[symbol] = GetNanArr()
        dataQ = dataDictQ[symbol]
        for d in range(1, len(data)):
            if data[d] is None or np.isnan(data[d][0]):
                continue
            quarter = dt.datetime.fromtimestamp(data[d][0])
            if quarter.month == 3:
                dataQ[d] = data[d]
            else:
                dataQ[d] = data[d] - data[d - 1]
                dataQ[d][0] = data[d][0]
                dataQ[d][2] = data[d][2]

    # 12 month data
    dataDict12M = {}
    for symbol, dataQ in dataDictQ.items():
        dataDict12M[symbol] = GetNanArr()
        data12M = dataDict12M[symbol]
        for d in range(3, len(dataQ)):
            data12M[d] = sum(dataQ[d - 3 : d + 1, :])
            data12M[d][0] = dataQ[d][0]
            data12M[d][2] = dataQ[d][2]

    # original data growth
    dataDictG = {}
    for symbol, data in dataDict.items():
        dataDictG[symbol] = GetNanArr()
        dataG = dataDictG[symbol]
        for d in range(4, len(data)):
            dataG[d] = Growth(data[d - 4], data[d]) # data[d] / data[d - 4] - 1
            dataG[d][0] = data[d][0]
            dataG[d][2] = data[d][2]


    # quarter data growth
    dataDictQG = {}
    for symbol, dataQ in dataDictQ.items():
        dataDictQG[symbol] = GetNanArr()
        dataQG = dataDictQG[symbol]
        for d in range(4, len(dataQ)):
            dataQG[d] = Growth(dataQ[d - 4], dataQ[d]) # dataQ[d] / dataQ[d - 4] - 1
            dataQG[d][0] = dataQ[d][0]
            dataQG[d][2] = dataQ[d][2]

    # 12 month data growth
    dataDict12MG = {}
    for symbol, data12M in dataDict12M.items():
        dataDict12MG[symbol] = GetNanArr()
        data12MG = dataDict12MG[symbol]
        for d in range(4, len(data12M)):
            data12MG[d] = Growth(data12M[d - 4], data12M[d]) # data12M[d] / data12M[d - 4] - 1
            data12MG[d][0] = data12M[d][0]
            data12MG[d][2] = data12M[d][2]

    # quarter data average
    dataDictQA = {}
    for symbol, data in dataDict.items():
        dataDictQA[symbol] = GetNanArr()
        dataQA = dataDictQA[symbol]
        for d in range(1, len(data)):
            dataQA[d] = np.nanmean(data[[d, d - 1], :], axis=0)
            dataQA[d][0] = data[d][0]
            dataQA[d][2] = data[d][2]

    # 12 month data average
    dataDict12MA = {}
    for symbol, data in dataDict.items():
        dataDict12MA[symbol] = GetNanArr()
        data12MA = dataDict12MA[symbol]
        for d in range(4, len(data)):
            data12MA[d] = np.nanmean(data[[d, d - 1, d - 2, d - 3, d - 4], :], axis=0)
            data12MA[d][0] = data[d][0]
            data12MA[d][2] = data[d][2]

    return quaList, dataDict, dataDictQ, dataDict12M, dataDictG, dataDictQG, dataDict12MG, dataDictQA, dataDict12MA

def ToDB(dataDict, facName, endDate, updateReportDate=True, mongoClient=None):
    if endDate is None:
        dtNow = dt.datetime.now()
        endDate = dt.datetime(dtNow.year, dtNow.month, dtNow.day)
    else:
        endDate = dt.datetime(endDate.year, endDate.month, endDate.day)
    if mongoClient is None:
        mongoConn = GetPara('mongoConn')
        mongoClient = pm.MongoClient(mongoConn)
    db = mongoClient['factor']
    lastUpdateDate = GetLastUpdateDate(facName, mongoClient)
    lastUpdateTick = lastUpdateDate.timestamp()

    # combine all symbols
    lastRecord = db[facName].find_one({'_id': lastUpdateDate})
    if lastRecord is not None:
        sysFieldList = []
        for key in lastRecord.keys():
            if key[0] == '_':
                sysFieldList.append(key)
        for sysField in sysFieldList:
            lastRecord.pop(sysField)
    symbolSet = (lastRecord.keys() | dataDict.keys()) if lastRecord is not None else dataDict.keys()

    # reformat all data
    invalidDataArr = np.array([[dt.datetime(2099, 12, 31).timestamp(), np.nan, sys.maxsize, np.nan]])
    lastReportRecord = db['reportDate'].find_one({'_id': lastUpdateDate})
    symbolList = []
    dataList = []
    cursorList = []
    for symbol in symbolSet:
        symbolList.append(symbol)
        cursorList.append(0)
        lastDataArr = np.array([[lastReportRecord[symbol].timestamp()
                                     if (lastReportRecord is not None and symbol in lastReportRecord.keys())
                                     else MIN_QUARTER.timestamp(),
                                 np.nan,
                                 lastUpdateTick,
                                 lastRecord[symbol] if (lastRecord is not None and symbol in lastRecord.keys()) else np.nan]])
        if symbol in dataDict.keys():
            newDataArr = dataDict[symbol]
            newBeginRow = len(newDataArr)
            for i in range(len(newDataArr)):
                if newDataArr[i][0] > lastDataArr[0][0]:
                    newBeginRow = i
                    break
            newDataArr = newDataArr[newBeginRow : ]
            dataList.append(np.vstack([lastDataArr, newDataArr, invalidDataArr]))
        else:
            dataList.append(np.vstack([lastDataArr, invalidDataArr]))

    # save to db day by day
    tradingDateSet = GetCalendar(lastUpdateDate, endDate)
    currentDate = lastUpdateDate + dt.timedelta(days=1)
    while currentDate <= endDate:
        print(facName + ' ' + str(currentDate))
        currentTick = currentDate.timestamp()
        isTrade = (currentDate in tradingDateSet)
        # locate cursor and fill invalid data passingly
        valueList = []
        reportDateList = []
        for s in range(len(symbolList)):
            symbol = symbolList[s]
            data = dataList[s]
            for d in range(cursorList[s], len(data) - 1):
                # filled by previous data if invalid
                if np.isnan(data[d + 1][3]):
                    data[d + 1][3] = data[d][3]
                # =====================================================================
                if data[d][2] < currentTick:  # an available record
                    if d > cursorList[s]:  # an available and new record
                        repDate = dt.datetime.fromtimestamp(data[d][0])
                        pubDate = currentDate # dt.datetime.fromtimestamp(data[d][2])
                        value = data[d][3]
                        db[facName + '_report'].update({'_id': repDate},
                                                       {'$set': {symbol: value,
                                                                 symbol + '_pubDate': pubDate,
                                                                 '_updateTime': dt.datetime.now()}},
                                                       upsert=True, multi=False)
                    if data[d + 1][2] >= currentTick:  # stop fetch
                        cursorList[s] = d
                        break
                #.................................................
                # # fetch to a new report record
                # if d > cursorList[s]:
                #     repDate = dt.datetime.fromtimestamp(data[d][0])
                #     pubDate = dt.datetime.fromtimestamp(data[d][2])
                #     value = data[d][3]
                #     db[facName + '_report'].update({'_id': repDate},
                #                                    {'$set': {symbol: value,
                #                                              symbol + '_pubDate': pubDate,
                #                                              '_updateTime': dt.datetime.now()}},
                #                                    upsert=True, multi=False)
                # # locate cursor
                # if (data[d][2] < currentTick <= data[d + 1][2]) or (data[d] < currentTick and d + 1 == len(data) - 1):
                #     if cursorList[s] != d:  # fetch to a new available record, save publish date to db.
                #         repDate = dt.datetime.fromtimestamp(data[d][0])
                #         pubDate = currentDate
                #         db['publishDate'].update({'_id': repDate},
                #                                  {'$set': {symbol: pubDate, '_updateTime': dt.datetime.now()}},
                #                                  upsert=True, multi=False)
                #     cursorList[s] = d
                #     break
                # =====================================================================

            cursor = cursorList[s]
            value = data[cursor][3]
            reportDate = dt.datetime.fromtimestamp(data[cursor][0])
            valueList.append(value)
            reportDateList.append(reportDate)
        # construct mongo document and save to db
        mongoFacDoc = {'_id': currentDate, '_isTrade': isTrade, '_updateTime': dt.datetime.now()}
        mongoRepDoc = {'_id': currentDate, '_isTrade': isTrade, '_updateTime': dt.datetime.now()}
        for s in range(len(symbolList)):
            symbol = symbolList[s]
            mongoFacDoc[symbol] = valueList[s]
            mongoRepDoc[symbol] = reportDateList[s]
        db[facName].save(mongoFacDoc)
        if updateReportDate:
            db['reportDate'].save(mongoRepDoc)

        currentDate += dt.timedelta(days=1)

    db.cfgUpdate.save({'_id': facName, 'lastUpdateDate': endDate})

def GetLastUpdateDate(facName, mongoClient = None):
    if mongoClient is None:
        mongoConn = GetPara('mongoConn')
        mongoClient = pm.MongoClient(mongoConn)
    db = mongoClient['factor']

    updateInfo = db.cfgUpdate.find_one({'_id': facName})
    if updateInfo is None:
        db.cfgUpdate.insert({'_id': facName, 'lastUpdateDate': MIN_DATE})
        lastUpdateDate = MIN_DATE
    else:
        lastUpdateDate = updateInfo['lastUpdateDate']
    return lastUpdateDate

def GetCalendar(beginDate, endDate, conn = None):
    if conn is None:
        connStr = GetPara('connStr')
        conn = co.connect(connStr)
    sql = "SELECT C.END_DATE " \
          "FROM UPCENTER.PUB_EXCH_CALE C " \
          "WHERE C.ISVALID = 1 AND C.IS_TRADE_DATE = 1 AND C.SEC_MAR_PAR = 1 AND " \
          "      C.END_DATE BETWEEN DATE'{BEGIN_DATE}' AND DATE'{END_DATE}'  " \
          "ORDER BY END_DATE"
    cursor = conn.cursor()
    cursor.execute(sql.replace('{BEGIN_DATE}', beginDate.strftime('%Y-%m-%d')).replace('{END_DATE}', endDate.strftime('%Y-%m-%d')))
    calTupleList = cursor.fetchall()
    calList = [calTuple[0] for calTuple in calTupleList]
    return set(calList)
