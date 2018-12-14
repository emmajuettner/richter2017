#investment strategy
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import math

#fileName = "C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/merged_month.csv"
fileName = "C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/merged_day.csv"
dataSet = pd.read_csv(fileName,usecols=["PERMNO","ret_1","ret_2","date","permno","daily_ESS"])
days = sorted(pd.unique(dataSet["date"].values.ravel())) #list of unique days
stockIDs = pd.unique(dataSet["PERMNO"].values.ravel()) #list of unique RP_ENTITY_IDs
#target = pd.DataFrame(index=days,columns=stockIDs)
totalRank4Returns = 0
totalRank1Returns = 0
totalRank4Stocks = 0
totalRank1Stocks = 0
dayAverages = []
dataSet["ranking"] = 0

for dayID in days:
    tempData = dataSet.loc[(dataSet["date"] == dayID)]
    tempData = tempData.sort_values("daily_ESS")
    tempLength = int(tempData.shape[0])
    rankLength = int(tempLength/4)
    quartile1 = rankLength
    quartile2 = rankLength*2
    quartile3 = rankLength*3
    tempData.iloc[0:quartile1,6] = 1
    tempData.iloc[quartile1:quartile2,6] = 2
    tempData.iloc[quartile2:quartile3,6] = 3
    tempData.iloc[quartile3:tempLength,6] = 4
    #set ranking in dataSet when you set it in tempData
    for index in tempData.loc[(tempData["ranking"] == 4)].index: #for stocks ranked 4
        dataSet.loc[index,"ranking"] = 4
    for index in tempData.loc[(tempData["ranking"] == 1)].index: #for stocks ranked 1
        dataSet.loc[index,"ranking"] = 1
    dayRank4Returns = 0
    dayRank1Returns = 0
    dayRank4Stocks = 0
    dayRank1Stocks = 0
    #print(tempData)
    #calculate number of items in each rank per day
    dayRank4Stocks = tempData.loc[(tempData["ranking"] == 4)].shape[0]
    dayRank1Stocks = tempData.loc[(tempData["ranking"] == 1)].shape[0]
    #calculate daily average
    dayRank4Average = np.nanmean(tempData.loc[(tempData["ranking"] == 4)]["ret_1"])*365
    dayRank1Average = np.nanmean(tempData.loc[(tempData["ranking"] == 1)]["ret_1"])*365
    #calculate daily std dev
    dayRank4StdDev = np.nanstd(tempData.loc[(tempData["ranking"] == 4)]["ret_1"])*365
    dayRank1StdDev = np.nanstd(tempData.loc[(tempData["ranking"] == 1)]["ret_1"])*365
    #calculate daily returns
    dayReturns = (dayRank4Average - dayRank1Average)
    print("daily average for rank 4 stocks for", dayID, "is", dayRank4Average, "with standard deviation", dayRank4StdDev)
    print("daily average for rank 1 stocks for", dayID, "is", dayRank1Average, "with standard deviation", dayRank1StdDev)
    print("daily returns:", dayReturns)
    if dayReturns >= 0 or dayReturns < 0:
        dayAverages.append(dayReturns)

totalRank4Stocks = dataSet.loc[(dataSet["ranking"] == 4)].shape[0]
totalRank1Stocks = dataSet.loc[(dataSet["ranking"] == 1)].shape[0]
#calculate total average
totalRank4Average = np.nanmean(dataSet.loc[(dataSet["ranking"] == 4)]["ret_1"])
totalRank1Average = np.nanmean(dataSet.loc[(dataSet["ranking"] == 1)]["ret_1"])
#calculate total std dev
totalRank4StdDev = np.nanstd(dataSet.loc[(dataSet["ranking"] == 4)]["ret_1"])
totalRank1StdDev = np.nanstd(dataSet.loc[(dataSet["ranking"] == 1)]["ret_1"])
#calculate overall return
totalReturns = totalRank4Average - totalRank1Average
print("total average for rank 4 stocks is", totalRank4Average, "with standard deviation", totalRank4StdDev)
print("total average for rank 1 stocks is", totalRank1Average, "with standard deviation", totalRank1StdDev)
print("overall returns from investment strategy:", totalReturns)
totalStdDev = math.sqrt(((totalRank4StdDev)**2/totalRank4Stocks)+((totalRank1StdDev)**2/totalRank1Stocks))
print("overall standard deviation:", totalStdDev)

#create histogram
#dayAverages.remove(dayAverages[-1])
print(dayAverages)
plt.title("Returns for Investment Strategy - Daily")
plt.hist(x=dayAverages, color="blue");
plt.show()