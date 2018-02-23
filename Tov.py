import Public
import pymongo as pm
import cx_Oracle as co
import datetime as dt
import numpy as np
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def Beta(tovPrefix, tovDaysList, stockSql, endDate=None):
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

    for days in tovDaysList:
        facName = tovPrefix + '_' + str(days)
        lastUpdateDate = Public.GetLastUpdateDate(facName, mc)
        tradingDateSet = Public.GetCalendar(lastUpdateDate, endDate)
        currentDate = lastUpdateDate - dt.timedelta(days=days)
        savedDate = lastUpdateDate
        stockRtnDict = {}
        while currentDate + dt.timedelta(days=1) <= endDate:
            currentDate += dt.timedelta(days=1)
            # if currentDate not in tradingDateSet:
            #     continue

            # get data
            for stockSymbol, stockRtnList in stockRtnDict.items():
                stockRtnList.append(np.nan)

            cursor.execute(stockSql.replace('{TRADE_DATE}', currentDate.strftime('%Y-%m-%d')))
            stockRtnRecordSet = cursor.fetchall()
            for stockRtnRecord in stockRtnRecordSet:
                symbol = stockRtnRecord[1]
                tov = stockRtnRecord[4]
                if symbol not in stockRtnDict.keys():
                    stockRtnDict[symbol] = [np.nan]
                stockRtnDict[symbol][-1] = tov if tov is not None else np.nan

            if currentDate <= lastUpdateDate:
                continue

            # evaluate beta and save to db
            mongoDoc = {'_id': currentDate, '_updateTime': dt.datetime.now(),
                        '_isTrade': (currentDate in tradingDateSet)}
            for stockSymbol, stockRtnList in stockRtnDict.items():
                stockArr = np.array(stockRtnList[max(0, len(stockRtnList) - days) : ], dtype=float)
                tov = np.nanmean(stockArr)
                mongoDoc[stockSymbol] = tov
            db[facName].save(mongoDoc)

            savedDate = currentDate
            print(facName + ' ' + str(currentDate))

        db.cfgUpdate.save({'_id': facName, 'lastUpdateDate': savedDate})

stockSql = "SELECT M.TRADE_DATE, CONCAT(I.STK_CODE, '_CS'), CLOSE_PRICE_RE, OPEN_PRICE_RE, TURNOVER_RATE " \
             "FROM UPCENTER.STK_BASIC_PRICE_MID M JOIN  UPCENTER.STK_BASIC_INFO I " \
             "		ON M.STK_UNI_CODE = I.STK_UNI_CODE AND M.ISVALID = 1 AND I.ISVALID = 1 " \
             "WHERE M.TRADE_VOL > 0 AND M.TRADE_DATE = M.END_DATE AND M.TRADE_DATE = TO_DATE('{TRADE_DATE}', 'YYYY-MM-DD') "

tovDaysList = [30, 60, 90, 180, 360]
Beta('DAY_TOV', tovDaysList, stockSql, endDate=None)