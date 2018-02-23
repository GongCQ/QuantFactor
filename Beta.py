import Public
import pymongo as pm
import cx_Oracle as co
import datetime as dt
import numpy as np
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def Beta(betaPrefix, betaDaysList, indexSymbolDict, indexSql, stockSql, endDate=None):
    if endDate is None:
        dtNow = dt.datetime.now() - dt.timedelta(days=1)
        endDate = dt.datetime(dtNow.year, dtNow.month, dtNow.day)
    else:
        endDate = dt.datetime(endDate.year, endDate.month, endDate.day)
        if endDate.date() >= dt.datetime.now().date():
            endDate = dt.datetime(dt.datetime.now().year, dt.datetime.now().month, dt.datetime.now().day) - \
                      dt.timedelta(days=1)

    connStr = Public.GetPara('connStr')
    conn = co.connect(connStr)
    cursor = conn.cursor()
    mongoConn = Public.GetPara('mongoConn')
    mc = pm.MongoClient(mongoConn)
    db = mc['factor']

    for days in betaDaysList:
        for indexCode, indexSymbol in indexSymbolDict.items():
            facName = betaPrefix + '_' + indexSymbol + '_' + str(days)
            lastUpdateDate = Public.GetLastUpdateDate(facName, mc)
            tradingDateSet = Public.GetCalendar(lastUpdateDate, endDate)
            currentDate = lastUpdateDate - dt.timedelta(days=days)
            savedDate = lastUpdateDate
            indexRtnDict = {}
            stockRtnDict = {}
            while currentDate + dt.timedelta(days=1) <= endDate:
                currentDate += dt.timedelta(days=1)
                # if currentDate not in tradingDateSet:
                #     continue

                # get data
                for stockSymbol, stockRtnList in stockRtnDict.items():
                    stockRtnList.append(np.nan)
                for indexSymbol, indexRtnList in indexRtnDict.items():
                    indexRtnList.append(np.nan)

                cursor.execute(stockSql.replace('{TRADE_DATE}', currentDate.strftime('%Y-%m-%d')))
                stockRtnRecordSet = cursor.fetchall()
                for stockRtnRecord in stockRtnRecordSet:
                    symbol = stockRtnRecord[1]
                    rtn = stockRtnRecord[4]
                    if symbol not in stockRtnDict.keys():
                        stockRtnDict[symbol] = [np.nan]
                    stockRtnDict[symbol][-1] = rtn if rtn is not None else np.nan

                cursor.execute(indexSql.replace('{TRADE_DATE}', currentDate.strftime('%Y-%m-%d')).replace('{INDEX_CODE}', str(indexCode)))
                indexRtnRecordSet = cursor.fetchall()
                for indexRtnRecord in indexRtnRecordSet:
                    symbol = indexRtnRecord[1]
                    if symbol != indexSymbol:
                        raise Exception('index code is not fit to index symbol!')
                    rtn = indexRtnRecord[4]
                    if symbol not in indexRtnDict.keys():
                        indexRtnDict[symbol] = [np.nan]
                    indexRtnDict[symbol][-1] = rtn if rtn is not None else np.nan

                if currentDate <= lastUpdateDate:
                    continue

                # evaluate beta and save to db
                if indexSymbol not in indexRtnDict.keys() or len(indexRtnDict[indexSymbol]) < days / 2:
                    continue
                mongoDoc = {'_id': currentDate, '_updateTime': dt.datetime.now(),
                            '_isTrade': (currentDate in tradingDateSet)}
                indexArr = np.array(indexRtnDict[indexSymbol][max(0, len(indexRtnDict[indexSymbol]) - days) : ], dtype=float)
                for stockSymbol, stockRtnList in stockRtnDict.items():
                    stockArr = np.array(stockRtnList[max(0, len(stockRtnList) - days) : ], dtype=float)
                    if len(stockArr) != len(indexArr):
                        continue
                    vld = np.isfinite(stockArr) * np.isfinite((indexArr))
                    if np.sum(vld) < days / 3:  # data is not sufficient
                        continue
                    const = np.ones(len(stockArr))
                    X = np.vstack([indexArr[vld], const[vld]]).T
                    y = stockArr[vld]
                    c, res, rank, s = np.linalg.lstsq(X, y)
                    beta = c[0]
                    mongoDoc[stockSymbol] = beta
                db[facName].save(mongoDoc)

                savedDate = currentDate
                print(facName + ' ' + str(currentDate))

            db.cfgUpdate.save({'_id': facName, 'lastUpdateDate': savedDate})

indexSql =  "SELECT TRADE_DATE, CONCAT(I.IND_CODE, '_IDX'), CLOSE_PRICE, OPEN_PRICE, CHAN_RATE / 100 " \
            "FROM UPCENTER.IND_BASIC_MQ B JOIN UPCENTER.IND_CODE_INFO I " \
            "               ON B.IND_UNI_CODE = I.IND_UNI_CODE AND B.ISVALID = 1 AND I.ISVALID = 1 " \
            "WHERE TRADE_DATE = TO_DATE('{TRADE_DATE}', 'YYYY-MM-DD') " \
            "AND B.IND_UNI_CODE = {INDEX_CODE} "
stockSql = "SELECT M.TRADE_DATE, CONCAT(I.STK_CODE, '_CS'), CLOSE_PRICE_RE, OPEN_PRICE_RE, RISE_DROP_RANGE_RE / 100   " \
             "FROM UPCENTER.STK_BASIC_PRICE_MID M JOIN  UPCENTER.STK_BASIC_INFO I " \
             "		ON M.STK_UNI_CODE = I.STK_UNI_CODE AND M.ISVALID = 1 AND I.ISVALID = 1 " \
             "WHERE M.TRADE_VOL > 0 AND M.TRADE_DATE = M.END_DATE AND M.TRADE_DATE = TO_DATE('{TRADE_DATE}', 'YYYY-MM-DD') "
indexSymbolDict = {2060002285: '000903_IDX', 2060002287: '000904_IDX', 2060002289: '000905_IDX', 2060005124: '000852_IDX',
                   2060002188: '000300_IDX', 2060002293: '000906_IDX' }

betaDaysList = [30, 60, 90, 180, 360]
Beta('DAY_BETA', betaDaysList, indexSymbolDict, indexSql, stockSql, endDate=None)