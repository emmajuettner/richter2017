import pandas as pd

monthlyFile = "D:/Richter Project/merged_month.csv"
dataSet = pd.read_csv(monthlyFile,usecols=["PERMNO","ret_1","ret_2","yearmonth","RP_ENTITY_ID","monthly_ESS"])
months = sorted(pd.unique(dataSet["yearmonth"].values.ravel())) #list of months in form 201201
stockIDs = pd.unique(dataSet["RP_ENTITY_ID"].values.ravel()) #list of unique RP_ENTITY_IDs


#create a dictionary that associates industries (HSICCD) with each RP_ENTITY_ID

fileName = "D:/industry.csv"
industryFile = pd.read_csv(fileName,usecols=["PERMNO","DATE","yearmonth","HSICCD"])
industryList = {} #dictionary with RP_ENTITY_IDs as keys and TOPICs as values
for index, row in industryFile.iterrows():
    if row["HSICCD"] != "" and row["HSICCD"] == row["HSICCD"]: #filter out empty and nan values
        if row["PERMNO"] not in industryList:
            industryList[row["PERMNO"]] = row["HSICCD"]
            
uniqueIndustries = set(industryList.values()) #list of unique values for industry
uniqueIndustries.add("unknown")


#for item in uniqueIndustries:
#    print(item)

#calculate average returns & ESS for each industry for each month
#calculate total average returns & ESS for each month

totalStocks = pd.DataFrame(0,index=months,columns=uniqueIndustries) #total stocks associated with each industry
totalReturns = pd.DataFrame(0,index=months,columns=uniqueIndustries) #total returns per month for each industry
totalESS = pd.DataFrame(0,index=months,columns=uniqueIndustries) #total ESS per month for each industry

for index, row in dataSet.iterrows():
    #keep count of the total of average returns for each industry
    if row["PERMNO"] in industryList:
        currentIndustry = industryList[row["PERMNO"]]
    else:
        currentIndustry = "unknown"
    #print(row["ret_2"])
    #print(row["monthly_ESS"])
    totalReturns[currentIndustry][row["yearmonth"]] = totalReturns[currentIndustry][row["yearmonth"]] + row["ret_2"]
    #keep count of total number of stocks for each industry
    totalStocks[currentIndustry][row["yearmonth"]] = totalStocks[currentIndustry][row["yearmonth"]] + 1
    #keep count of total ESS for each industry
    totalESS[currentIndustry][row["yearmonth"]] = totalESS[currentIndustry][row["yearmonth"]] + row["monthly_ESS"]


totalReturns.to_csv("D:/Richter Project/monthly_total_returns_by_industry.csv")
totalESS.to_csv("D:/Richter Project/monthly_total_ESS_by_industry.csv")
totalStocks.to_csv("D:/Richter Project/monthly_total_stocks_by_industry.csv")



#totalReturns = pd.DataFrame.from_csv("D:/Richter Project/monthly_total_returns_by_industry.csv")
#totalESS = pd.DataFrame.from_csv("D:/Richter Project/monthly_total_ESS_by_industry.csv")
#totalStocks = pd.DataFrame.from_csv("D:/Richter Project/monthly_total_stocks_by_industry.csv")

#calculate averages
averageReturns = pd.DataFrame(0,index=months,columns=uniqueIndustries)
averageESS = pd.DataFrame(0,index=months,columns=uniqueIndustries)

for month in months:
    for industry in uniqueIndustries:
        if totalStocks[industry][month] > 0:
            averageReturns.loc[month,industry] = float(totalReturns[industry][month]) / totalStocks[industry][month]
            averageESS.loc[month,industry] = float(totalESS[industry][month]) / totalStocks[industry][month]
        
averageReturns.to_csv("D:/Richter Project/monthly_ave_returns_by_industry.csv")
averageESS.to_csv("D:/Richter Project/monthly_ave_ESS_by_industry.csv")