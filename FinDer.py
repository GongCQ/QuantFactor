import Public
import pymongo as pm
import cx_Oracle as co
import datetime as dt
import numpy as np
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

def FinDer(configDictList, endDate):
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
    # for each factor
    for configDict in configDictList:
        facName = configDict['facName']
        depFacNameList = configDict['depFacNameList']
        EvalFun = configDict['EvalFun']

        lastUpdateDate = Public.GetLastUpdateDate(facName, mc)
        tradingDateSet = Public.GetCalendar(lastUpdateDate, endDate)
        currentDate = lastUpdateDate + dt.timedelta(days=1)
        savedDate = lastUpdateDate
        while currentDate <= endDate:
            deficientData = False
            # get all dependent factor
            symbolSet = set()
            depFacListDict = {}
            for d in range(len(depFacNameList)):
                depFacName = depFacNameList[d]
                record = db[depFacName].find_one({'_id': currentDate})
                if record is None:
                    deficientData = True
                    break
                for symbol, value in record.items():
                    if symbol[0] == '_':
                        continue
                    if symbol not in depFacListDict.keys():
                        depFacListDict[symbol] = np.nan * np.zeros([len(depFacNameList)])
                    depFacListDict[symbol][d] = value if value is not None else np.nan
                symbolSet = symbolSet | record.keys()
            if deficientData:
                break
            # evaluate factor value and save to db
            mongoDoc = {'_id': currentDate, '_isTrade': (currentDate in tradingDateSet), '_updateTime': dt.datetime.now()}
            for symbol in symbolSet:
                if symbol[0] == '_': # system field
                    continue
                facValue = EvalFun(depFacListDict[symbol])
                mongoDoc[symbol] = facValue
            db[facName].save(mongoDoc)
            savedDate = currentDate

            print(facName + ' ' + str(currentDate))
            currentDate += dt.timedelta(days=1)

        db.cfgUpdate.save({'_id': facName, 'lastUpdateDate': savedDate})


configDictList = [
                  {'facName': 'FFD_ROE_Q',       # ROE（季度）
                   'depFacNameList': ['FIN_NP_Q', 'FIN_TOE_QA'],
                   'EvalFun': lambda x : x[0] / x[1]},

                  {'facName': 'FFD_ROE_12M',     # ROE（12个月）
                   'depFacNameList': ['FIN_NP_12M', 'FIN_TOE_12MA'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_ROEAPC_Q',  # 归母ROE（季度）
                   'depFacNameList': ['FIN_NPAPC_Q', 'FIN_TOEAPC_QA'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_ROEAPC_12M',  # 归母ROE（12个月）
                   'depFacNameList': ['FIN_NPAPC_12M', 'FIN_TOEAPC_12MA'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_ROA_Q',       # ROA（季度）
                   'depFacNameList': ['FIN_NP_Q', 'FIN_TA_QA'],
                   'EvalFun': lambda x : x[0] / x[1]},

                  {'facName': 'FFD_ROA_12M',     # ROA（12个月）
                   'depFacNameList': ['FIN_NP_12M', 'FIN_TA_12MA'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_ROAAPC_Q',  # 归母ROA（季度）
                   'depFacNameList': ['FIN_NPAPC_Q', 'FIN_TA_QA'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_ROAAPC_12M',  # 归母ROA（12个月）
                   'depFacNameList': ['FIN_NPAPC_12M', 'FIN_TA_12MA'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_OCFPR_Q',  # 经营性现金流/净利润（季度）
                   'depFacNameList': ['FIN_OCF_Q', 'FIN_NP_Q'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_OCFPR_12M',  # 经营性现金流/净利润（12个月）
                   'depFacNameList': ['FIN_OCF_12M', 'FIN_NP_12M'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_PSR_Q',  # 归母净利润/总营业收入（季度）
                   'depFacNameList': ['FIN_NPAPC_Q', 'FIN_OTR_Q'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_PSR_12M',  # 归母净利润/总营业收入（12个月）
                   'depFacNameList': ['FIN_NPAPC_12M', 'FIN_OTR_12M'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_OCFTAR_Q',  # 经营性现金流/平均总资产（季度）
                   'depFacNameList': ['FIN_OCF_Q', 'FIN_TA_QA'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_OCFTAR_12M',  # 经营性现金流/平均总资产（12个月）
                   'depFacNameList': ['FIN_OCF_12M', 'FIN_TA_12MA'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_OSR_Q',  # 营业利润/营业收入（季度）
                   'depFacNameList': ['FIN_OP_Q', 'FIN_OR_Q'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_OSR_12M',  # 营业利润/营业收入（12个月）
                   'depFacNameList': ['FIN_OP_12M', 'FIN_OR_12M'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_OSA_Q',  # 营业收入/平均总资产（季度）
                   'depFacNameList': ['FIN_OR_Q', 'FIN_TA_QA'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_OSA_12M',  # 营业收入/平均总资产（12个月）
                   'depFacNameList': ['FIN_OR_12M', 'FIN_TA_12MA'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_CPR_Q',  # 毛利率（季度）
                   'depFacNameList': ['FIN_OR_Q', 'FIN_OC_Q'],
                   'EvalFun': lambda x: (x[0] / x[1] - 1)},

                  {'facName': 'FFD_CPR_12M',  # 毛利率（12个月）
                   'depFacNameList': ['FIN_OR_12M', 'FIN_OC_12M'],
                   'EvalFun': lambda x: (x[0] / x[1] - 1)},

                  {'facName': 'FFD_NPR_Q',  # 净利润/营业收入（季度）
                   'depFacNameList': ['FIN_NP_Q', 'FIN_OR_Q'],
                   'EvalFun': lambda x: x[0] / x[1] },

                  {'facName': 'FFD_NPR_12M',  # 净利润/营业收入（12个月）
                   'depFacNameList': ['FIN_NP_12M', 'FIN_OR_12M'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_OPR_Q',  # 营业利润/营业收入（季度）
                   'depFacNameList': ['FIN_OP_Q', 'FIN_OR_Q'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_OPR_12M',  # 营业利润/营业收入（12个月）
                   'depFacNameList': ['FIN_OP_12M', 'FIN_OR_12M'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_CPTA_Q',  # (营业收入 - 营业成本) / 总资产 （季度）
                   'depFacNameList': ['FIN_OR_Q', 'FIN_OC_Q', 'FIN_TA_QA'],
                   'EvalFun': lambda x: (x[0] - x[1]) / x[2]},

                  {'facName': 'FFD_CPTA_12M',  # (营业收入 - 营业成本) / 总资产（12个月）
                   'depFacNameList': ['FIN_OR_12M', 'FIN_OC_12M', 'FIN_TA_12MA'],
                   'EvalFun': lambda x: (x[0] - x[1]) / x[2]},

                  {'facName': 'FFD_IT_Q',  # 营业成本/平均存货余额 （季度）
                   'depFacNameList': ['FIN_OC_Q', 'FIN_IVT_QA'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FFD_IT_12M',  # 营业成本/平均存货余额 （12个月）
                   'depFacNameList': ['FIN_OC_12M', 'FIN_IVT_12MA'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FDD_PB',  # 总市值/所有者权益合计
                   'depFacNameList': ['DAY_TV', 'FIN_TOE'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FDD_PE',   # 总市值/12个月净利润
                   'depFacNameList': ['DAY_TV', 'FIN_NP_12M'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FDD_PE_EBIT',   # 总市值/12个月税前净利润
                   'depFacNameList': ['DAY_TV', 'FIN_NP_12M', 'FIN_ITE_12M'],
                   'EvalFun': lambda x: x[0] / (x[1] + np.nan_to_num(x[2]))},

                  {'facName': 'FDD_PS',   # 总市值/12个月营业收入
                   'depFacNameList': ['DAY_TV', 'FIN_OR_12M'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FDD_PC',   # 总市值/12个月经营性现金流
                   'depFacNameList': ['DAY_TV', 'FIN_OCF_12M'],
                   'EvalFun': lambda x: x[0] / x[1]},

                  {'facName': 'FDD_PEPGR',   # 总市值/12个月净利润/12个月净利润增长率
                   'depFacNameList': ['DAY_TV', 'FIN_NP_12M', 'FIN_NP_12MG'],
                   'EvalFun': lambda x: x[0] / x[1] / x[2]},

                  {'facName': 'FDD_EV',   # 总市值+非流动负债合计-货币资金
                   'depFacNameList': ['DAY_TV', 'FIN_TNLL', 'FIN_MC'],
                   'EvalFun': lambda x: (x[0] + x[1] - x[2])}

                  # {'facName': '',   # 总市值/12个月
                  #  'depFacNameList': ['', ''],
                  #  'EvalFun': lambda x: },
                  #
                  # {'facName': '',   # 总市值/12个月
                  #  'depFacNameList': ['', ''],
                  #  'EvalFun': lambda x: },
                  ]

FinDer(configDictList, endDate=dt.datetime.now())

