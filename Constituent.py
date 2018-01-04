import Public
import pymongo as pm
import cx_Oracle as co
import datetime as dt
import numpy as np
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def Day(facNameList, sqlPrc, endDate):
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

    # for each factor
    for f in range(len(facNameList)):
        facName = facNameList[f]
        sql = sqlList[f]
        lastUpdateDate = Public.GetLastUpdateDate(facName, mc)
        tradingDateSet = Public.GetCalendar(lastUpdateDate, endDate)
        mongoDoc = db[facName].find_one({'_id': lastUpdateDate})
        mongoDoc = {} if mongoDoc is None else mongoDoc
        # save data day by day.
        currentDate = lastUpdateDate + dt.timedelta(days=1)
        while currentDate <= endDate:
            cursor.execute(sql.replace('{TRADE_DATE}', currentDate.strftime('%Y-%m-%d')))
            dataList = cursor.fetchall()
            isTrade = (currentDate in tradingDateSet)
            # mongoDoc = {}  # if this statement is executed, invalid data will not be filled by previous value
            mongoDoc['_id'] = currentDate
            mongoDoc['_isTrade'] = isTrade
            mongoDoc['_updateTime'] = dt.datetime.now()
            for record in dataList:
                symbol = record[1]
                data = record[3]
                if data is not None and len(data) > 0:
                    mongoDoc[symbol] = data
            db[facName].save(mongoDoc)
            print(facName + ' ' + str(currentDate))
            currentDate += dt.timedelta(days=1)
        db.cfgUpdate.save({'_id': facName, 'lastUpdateDate': endDate})

    ddd = 0


sqlSwi = "SELECT DATE'{TRADE_DATE}', CONCAT(I.STK_CODE, '_CS'), DATE'{TRADE_DATE}', IC1.INDU_NAME " \
         "FROM UPCENTER.PUB_COM_INDU_CHAN C  " \
         "        JOIN UPCENTER.STK_BASIC_INFO I " \
         "            ON C.ISVALID = 1 AND I.ISVALID = 1 AND C.COM_UNI_CODE = I.COM_UNI_CODE AND C.INDU_SYS_PAR = 15 " \
         "        JOIN UPCENTER.PUB_INDU_CODE IC  " \
         "            ON C.ISVALID = 1 AND IC.ISVALID = 1 AND C.INDU_UNI_CODE = IC.INDU_UNI_CODE  " \
         "        JOIN UPCENTER.PUB_INDU_CODE IC1  " \
         "            ON IC1.ISVALID = 1 AND IC.ISVALID = 1 AND IC1.INDU_UNI_CODE = IC.FST_INDU_UNI_CODE " \
         "WHERE C.SUB_START_DATE <= DATE'{TRADE_DATE}' AND (C.SUB_END_DATE > DATE'{TRADE_DATE}' OR C.SUB_END_DATE IS NULL)"
facNameList = ['CON_SWI1']
sqlList = [sqlSwi]
Day(facNameList, sqlSwi, endDate=dt.datetime.now())