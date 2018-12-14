#investment strategy
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import math

#fileName = "C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/merged_month.csv"
fileName = "C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/merged_month.csv"
dataSet = pd.read_csv(fileName,usecols=["PERMNO","ret_1","ret_2","yearmonth","permno","monthly_ESS"])
months = sorted(pd.unique(dataSet["yearmonth"].values.ravel())) #list of months in form 201201
stockIDs = pd.unique(dataSet["PERMNO"].values.ravel()) #list of unique RP_ENTITY_IDs
target = pd.DataFrame(index=months,columns=stockIDs)
totalRank4Returns = 0
totalRank1Returns = 0
totalRank4Stocks = 0
totalRank1Stocks = 0
monthlyAverages = []
dataSet["ranking"] = 0

for monthID in target.index:
    tempData = dataSet.loc[(dataSet["yearmonth"] == monthID)]
    tempData = tempData.sort_values("monthly_ESS")
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
    monthRank4Returns = 0
    monthRank1Returns = 0
    monthRank4Stocks = 0
    monthRank1Stocks = 0
    #print(tempData)
    #calculate number of items in each rank per month
    monthRank4Stocks = tempData.loc[(tempData["ranking"] == 4)].shape[0]
    monthRank1Stocks = tempData.loc[(tempData["ranking"] == 1)].shape[0]
    #calculate monthly average
    monthRank4Average = np.nanmean(tempData.loc[(tempData["ranking"] == 4)]["ret_1"])*12
    monthRank1Average = np.nanmean(tempData.loc[(tempData["ranking"] == 1)]["ret_1"])*12
    #calculate monthly std dev
    monthRank4StdDev = np.nanstd(tempData.loc[(tempData["ranking"] == 4)]["ret_1"])*12
    monthRank1StdDev = np.nanstd(tempData.loc[(tempData["ranking"] == 1)]["ret_1"])*12
    #calculate monthly returns
    monthReturns = (monthRank4Average - monthRank1Average)
    print("monthly average for rank 4 stocks for", monthID, "is", monthRank4Average, "with standard deviation", monthRank4StdDev)
    print("monthly average for rank 1 stocks for", monthID, "is", monthRank1Average, "with standard deviation", monthRank1StdDev)
    print("monthly returns:", monthReturns)
    monthlyAverages.append(monthReturns)

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
print("total num of rank 4 stocks:", totalRank4Stocks)
print("total num of rank 1 stocks:", totalRank1Stocks)

#create histogram
monthlyAverages.remove(monthlyAverages[-1])
print(monthlyAverages)
plt.title("Returns for Investment Strategy - All Data")
plt.hist(x=monthlyAverages, color="blue");
plt.show()