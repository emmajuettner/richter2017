import pandas as pd

#import the data
industryFileName = "D:/crsp_msf.csv"
industryFile = pd.read_csv(industryFileName,usecols=["PERMNO","DATE","yearmonth","HSICCD","ret_2"])

print("done importing data")

#get the values of yearmonth and HSICCD that you'll use in your target dataframes
yearmonths = sorted(pd.unique(industryFile["yearmonth"].values.ravel())) #list of months in form 201201
HSICCDs = pd.unique(industryFile["HSICCD"].values.ravel()) #list of unique industries (HSICCD)

print("done getting values of yearmonth & HSICCD")

#set up the target dataframes and initialize values to zero
aveReturnsPerMonthByIndustry = pd.DataFrame(index=yearmonths, columns=HSICCDs)
aveReturnsPerMonthByIndustry = aveReturnsPerMonthByIndustry.fillna(0)
totalStocksPerMonthByIndustry = pd.DataFrame(index=yearmonths, columns=HSICCDs)
totalStocksPerMonthByIndustry = totalStocksPerMonthByIndustry.fillna(0)

print("done initializing target dataframes")

#keep running total of returns & number of data points for each industry/month
for index, row in industryFile.iterrows():
    #keep count of the total of average returns for each industry
    currentIndustry = row["HSICCD"]
    currentMonth = row["yearmonth"]
    aveReturnsPerMonthByIndustry.loc[currentMonth,currentIndustry] += row["ret_2"]
    #keep count of total number of stocks for each industry
    totalStocksPerMonthByIndustry.loc[currentMonth,currentIndustry] += 1

print("done counting total returns")

#calculate averages
for month in yearmonths:
    for industry in HSICCDs:
        if totalStocksPerMonthByIndustry[industry][month] > 0:
            aveReturnsPerMonthByIndustry.loc[month,industry] = float(aveReturnsPerMonthByIndustry[industry][month]) / totalStocksPerMonthByIndustry[industry][month]

print("done calculating averages")

#save results as csv
aveReturnsPerMonthByIndustry.to_csv("D:/ave_returns_per_month_by_industry_hsiccd.csv")