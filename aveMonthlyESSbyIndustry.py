import pandas as pd

monthlyFile = "D:/Richter Project/merged_month.csv"
dataSet = pd.read_csv(monthlyFile,usecols=["PERMNO","ret_1","ret_2","yearmonth","RP_ENTITY_ID","monthly_ESS"])
months = sorted(pd.unique(dataSet["yearmonth"].values.ravel())) #list of months in form 201201
stockIDs = pd.unique(dataSet["RP_ENTITY_ID"].values.ravel()) #list of unique RP_ENTITY_IDs


#create a dictionary that associates industries (TOPIC) with each RP_ENTITY_ID
#(to use a different variable to determine industry, just swap out TOPIC for the new variable name)

#fileName = "D:/Richter Project/2000-01-equities.csv"
fileName = "D:/five years data/five_years_data.csv"
industryList = {} #dictionary with RP_ENTITY_IDs as keys and TOPICs as values
for chunk in pd.read_csv(fileName,encoding='latin1',comment="TIMESTAMP_UTC",chunksize=10**6,engine="python",header=None,
                         names=["TIMESTAMP_UTC","RP_ENTITY_ID","ENTITY_TYPE","ENTITY_NAME","POSITION_NAME",
                                "RP_POSITION_ID","COUNTRY_CODE","RELEVANCE","TOPIC","GROUP","TYPE","SUB_TYPE",
                                "PROPERTY","EVALUATION_METHOD","MATURITY","CATEGORY","ESS","AES","AEV","ENS",
                                "ENS_SIMILARITY_GAP","ENS_KEY","ENS_ELAPSED","G_ENS","G_ENS_SIMILARITY_GAP",
                                "G_ENS_KEY","G_ENS_ELAPSED","EVENT_SIMILARITY_KEY","NEWS_TYPE","SOURCE",
                                "RP_STORY_ID","RP_STORY_EVENT_INDEX","RP_STORY_EVENT_COUNT","PRODUCT","COMPANY",
                                "ISIN","CSS","NIP","PEQ","BEE","BMQ","BAM","BCA","BER","ANL_CHG","MCQ"]):
    for index, row in chunk.iterrows():
        if row["TOPIC"] != "" and row["TOPIC"] == row["TOPIC"]: #filter out empty and nan values
            if row["RP_ENTITY_ID"] not in industryList:
                industryList[row["RP_ENTITY_ID"]] = row["TOPIC"]

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
    if row["RP_ENTITY_ID"] in industryList:
        currentIndustry = industryList[row["RP_ENTITY_ID"]]
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