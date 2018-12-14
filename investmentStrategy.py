#investment strategy
import pandas as pd

fileName = "C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/merged_month.csv"
dataSet = pd.read_csv(fileName,usecols=["PERMNO","ret_1","ret_2","yearmonth","RP_ENTITY_ID","monthly_ESS"])
months = sorted(pd.unique(dataSet["yearmonth"].values.ravel())) #list of months in form 201201
stockIDs = pd.unique(dataSet["RP_ENTITY_ID"].values.ravel()) #list of unique RP_ENTITY_IDs
target = pd.DataFrame(index=months,columns=stockIDs)
myStocks = []
totalReturns = 0
totalRank4Returns = 0
totalRank1Returns = 0
totalRank4Stocks = 0
totalRank1Stocks = 0

for monthID in target.index:
    tempData = dataSet.loc[(dataSet["yearmonth"] == monthID)]
    tempData = tempData.sort_values("monthly_ESS")
    tempLength = int(tempData.shape[0])
    rankLength = int(tempLength/4)
    quartile1 = rankLength
    quartile2 = rankLength*2
    quartile3 = rankLength*3
    tempData["ranking"] = 0
    tempData.iloc[0:quartile1,6] = 1
    tempData.iloc[quartile1:quartile2,6] = 2
    tempData.iloc[quartile2:quartile3,6] = 3
    tempData.iloc[quartile3:tempLength,6] = 4
    #keep track of average rank 4 stock returns
    for index in tempData.loc[(tempData["ranking"] == 4)].index: #for stocks ranked 4
        if tempData.loc[(tempData["ranking"] == 4)]["ret_2"][index] >= 0 or tempData.loc[(tempData["ranking"] == 4)]["ret_2"][index] < 0: #ignore nan values
            totalRank4Returns = totalRank4Returns + float(tempData.loc[(tempData["ranking"] == 4)]["ret_2"][index])
            totalRank4Stocks = totalRank4Stocks + 1
    #keep track of average rank 1 stock returns
    for index in tempData.loc[(tempData["ranking"] == 1)].index: #for stocks ranked 1
        if tempData.loc[(tempData["ranking"] == 1)]["ret_2"][index] >= 0 or tempData.loc[(tempData["ranking"] == 1)]["ret_2"][index] < 0: #ignore nan values
            totalRank1Returns = totalRank1Returns + float(tempData.loc[(tempData["ranking"] == 1)]["ret_2"][index])
            totalRank1Stocks = totalRank1Stocks + 1
    #keep track of stocks bought & sold and total returns-- buy the ones ranked 4, sell the ones ranked 1
    for index in tempData.loc[(tempData["ranking"] == 4)].index: #for stocks ranked 4
        if tempData.loc[(tempData["ranking"] == 4)]["RP_ENTITY_ID"][index] not in myStocks:
            if tempData.loc[(tempData["ranking"] == 4)]["ret_1"][index] >= 0 or tempData.loc[(tempData["ranking"] == 4)]["ret_1"][index] < 0: #ignore nan values
                totalReturns = totalReturns - float(tempData.loc[(tempData["ranking"] == 4)]["ret_1"][index])
                myStocks.append(tempData.loc[(tempData["ranking"] == 4)]["RP_ENTITY_ID"][index])
        if tempData.loc[(tempData["ranking"] == 4)]["ret_2"][index] >= 0 or tempData.loc[(tempData["ranking"] == 4)]["ret_2"][index] < 0: #ignore nan values
            totalReturns = totalReturns + float(tempData.loc[(tempData["ranking"] == 4)]["ret_2"][index])
    for index in tempData.loc[(tempData["ranking"] == 1)].index: #for stocks ranked 1
        if tempData.loc[(tempData["ranking"] == 1)]["RP_ENTITY_ID"][index] in myStocks:
            if tempData.loc[(tempData["ranking"] == 1)]["ret_1"][index] >= 0 or tempData.loc[(tempData["ranking"] == 1)]["ret_1"][index] < 0: #ignore nan values
                totalReturns = totalReturns + float(tempData.loc[(tempData["ranking"] == 1)]["ret_1"][index])
                myStocks.remove(tempData.loc[(tempData["ranking"] == 1)]["RP_ENTITY_ID"][index])
    for index in tempData.loc[(tempData["ranking"] == 2)]["RP_ENTITY_ID"].index: #for stocks ranked 2
        if tempData.loc[(tempData["ranking"] == 2)]["RP_ENTITY_ID"][index] in myStocks:
            if tempData.loc[(tempData["ranking"] == 2)]["ret_2"][index] >= 0 or tempData.loc[(tempData["ranking"] == 2)]["ret_2"][index] < 0: #ignore nan values
                totalReturns = totalReturns + float(tempData.loc[(tempData["ranking"] == 2)]["ret_2"][index])
    for index in tempData.loc[(tempData["ranking"] == 3)]["RP_ENTITY_ID"].index: #for stocks ranked 3
        if tempData.loc[(tempData["ranking"] == 3)]["RP_ENTITY_ID"][index] in myStocks:
            if tempData.loc[(tempData["ranking"] == 3)]["ret_2"][index] >= 0 or tempData.loc[(tempData["ranking"] == 3)]["ret_2"][index] < 0: #ignore nan values
                totalReturns = totalReturns + float(tempData.loc[(tempData["ranking"] == 3)]["ret_2"][index])

print("total return from all stocks:", totalReturns)
#total return from all stocks: 682.9754904771424
print("average rank 4 stock return:",float(totalRank4Returns)/totalRank4Stocks)
#average rank 4 stock return: 0.010776724289487687
print("average rank 1 stock return:",float(totalRank1Returns)/totalRank1Stocks)
#average rank 1 stock return: 0.005755269212271871