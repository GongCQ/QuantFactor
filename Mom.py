import Public
import pymongo as pm
import cx_Oracle as co
import datetime as dt
import numpy as np
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

def Mom(priceColName, momPrefix, momDaysList, endDate=None):
    if endDate is None:
        dtNow = dt.datetime.now() - dt.timedelta(days=1)
        endDate = dt.datetime(dtNow.year, dtNow.month, dtNow.day)
    else:
        endDate = dt.datetime(endDate.year, endDate.month, endDate.day)
        if endDate.date() >= dt.datetime.now().date():
            endDate = dt.datetime(dt.datetime.now().year, dt.datetime.now().month, dt.datetime.now().day) - \
                      dt.timedelta(days=1)
    mongoConn = Public.GetPara('mongoConn')
    mc = pm.MongoClient(mongoConn)
    db = mc['factor']

    for days in momDaysList:
        facName = momPrefix + '_' + str(days)
        lastUpdateDate = Public.GetLastUpdateDate(facName, mc)
        tradingDateSet = Public.GetCalendar(lastUpdateDate, endDate)
        currentDate = lastUpdateDate - dt.timedelta(days=days)
        savedDate = lastUpdateDate
        priceDict = {}

        while currentDate + dt.timedelta(days=1) <= endDate:
            currentDate += dt.timedelta(days=1)
            # if currentDate not in tradingDateSet:
            #     continue
            # get data
            record = db[priceColName].find_one({'_id': currentDate})
            if record is None:
                continue
            for symbol, priceList in priceDict.items():
                priceList.append(np.nan)
            for symbol, price in record.items():
                if symbol[0] == '_':
                    continue
                if symbol not in priceDict.keys():
                    priceDict[symbol] = [np.nan]
                priceDict[symbol][-1] = price
            if currentDate <= lastUpdateDate:
                continue

            # evaluate momentum and save to db
            mongoDoc = {'_id': currentDate, '_updateTime': dt.datetime.now(), '_isTrade': (currentDate in tradingDateSet)}
            for symbol, priceList in priceDict.items():
                arr = np.array(priceList[max(0, len(priceList) - days) : ])
                if np.sum(np.isfinite(arr)) < days / 3: # data is not sufficient
                    continue
                mom = arr[-1] / np.nanmean(arr[0 : len(arr) - 1])
                mongoDoc[symbol] = mom
            db[facName].save(mongoDoc)

            savedDate = currentDate
            print(facName + ' ' + str(currentDate))

        db.cfgUpdate.save({'_id': facName, 'lastUpdateDate': savedDate})


priceColName = 'DAY_CLOSE'
momPrefix = 'DAY_MOM'
momDaysList = [30, 60, 90, 180, 360]
Mom(priceColName, momPrefix, momDaysList, endDate=None)