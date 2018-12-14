import pandas as pd
import datetime as dt
from dateutil.parser import parse




def averageESS(allData):
    index = pd.date_range(parse("2012-01-01"), periods=59, freq='M') #list of months
    stockIDFile = "C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/items_per_stock.csv"
    columns = pd.read_csv(stockIDFile)["Stock ID"].values #list of Stock IDs
    target = pd.DataFrame(index=index, columns=columns)
    target = target.fillna(0)
    monthList = []
    for item in target.index:
        monthList.append(item)
    ESSitemsPerStock = pd.DataFrame(index=index,columns=columns)
    ESSitemsPerStock = ESSitemsPerStock.fillna(0)
    monthIndex = 0
    for chunk in allData:
        for i, row in chunk.iterrows():
            date = parse(row["TIMESTAMP_UTC"])
            if date > target.index[monthIndex]:
                monthIndex = monthIndex + 1
            if row["ESS"] >= 0 and row["ESS"] <= 100:
                try:
                    target[row["RP_ENTITY_ID"]][monthIndex] = target[row["RP_ENTITY_ID"]][monthIndex] + row["ESS"]
                    ESSitemsPerStock[row["RP_ENTITY_ID"]][monthIndex] = ESSitemsPerStock[row["RP_ENTITY_ID"]][monthIndex] + 1
                except KeyError:
                    target[row["RP_ENTITY_ID"]] = pd.Series([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],index=target.index)
                    ESSitemsPerStock[row["RP_ENTITY_ID"]] = pd.Series([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],index=target.index)
                    target[row["RP_ENTITY_ID"]][monthIndex] = row["ESS"]
                    ESSitemsPerStock[row["RP_ENTITY_ID"]][monthIndex] = 1
    for stockID in target:
        for monthID in target.index:
            #print(target.loc[monthID,stockID])
            target.loc[monthID,stockID] = float(target.loc[monthID,stockID]) / ESSitemsPerStock.loc[monthID,stockID]
    return target

#fiveYearFileName = "C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/2000-01-equities.csv"
fiveYearFileName = "C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/five years data/five_years_data.csv"
fiveYearFile = pd.read_csv(fiveYearFileName,encoding='latin1',comment="TIMESTAMP_UTC",chunksize=10**6,engine="python",header=None,names=["TIMESTAMP_UTC","RP_ENTITY_ID","ENTITY_TYPE","ENTITY_NAME","POSITION_NAME","RP_POSITION_ID","COUNTRY_CODE","RELEVANCE","TOPIC","GROUP","TYPE","SUB_TYPE","PROPERTY","EVALUATION_METHOD","MATURITY","CATEGORY","ESS","AES","AEV","ENS","ENS_SIMILARITY_GAP","ENS_KEY","ENS_ELAPSED","G_ENS","G_ENS_SIMILARITY_GAP","G_ENS_KEY","G_ENS_ELAPSED","EVENT_SIMILARITY_KEY","NEWS_TYPE","SOURCE","RP_STORY_ID","RP_STORY_EVENT_INDEX","RP_STORY_EVENT_COUNT","PRODUCT","COMPANY","ISIN","CSS","NIP","PEQ","BEE","BMQ","BAM","BCA","BER","ANL_CHG","MCQ"])
aveESSperMonth = pd.DataFrame.from_dict(averageESS(fiveYearFile))
print(aveESSperMonth)
aveESSperMonth.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/unsorted_ave_ess_per_month.csv")

aveESSperMonth = aveESSperMonth.sort_values(0)
print(aveESSperMonth)
aveESSperMonth.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/sorted_ave_ess_per_month.csv")


def rankByESS(sortedDF,rankingNum):
    newDF = sortedDF
    for monthID in sortedDF.index:
        rank = 1
        count = 0
        for column in newDF.columns:
            newDF.loc[monthID,column] = rank
            count = count + 1
            if count > int(len(newDF.columns)/rankingNum):
                rank = rank + 1
                count = 0
    return newDF

quartilesAveESSPerMonth = rankByESS(aveESSperMonth,4)
print(quartilesAveESSPerMonth)
quartilesAveESSPerMonth.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/quartilesFiveYears.csv")
quintilesAveESSPerMonth = rankByESS(aveESSperMonth,5)
quintilesAveESSPerMonth.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/quintilesFiveYears.csv")
decilesAveESSPerMonth = rankByESS(aveESSperMonth,10)
decilesAveESSPerMonth.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/decilesFiveYears.csv")


#---testing---
#keep track of stocks bought & sold and total returns-- buy the ones ranked 4, sell the ones ranked 1
    '''
    for index in tempData.loc[(tempData["ranking"] == 4)].index:
        if tempData.loc[(tempData["ranking"] == 4)]["RP_ENTITY_ID"][index] not in myStocks:
            if tempData.loc[(tempData["ranking"] == 4)]["ret_1"][index] >= 0 or tempData.loc[(tempData["ranking"] == 4)]["ret_1"][index] < 0: #ignore nan values
                totalReturns = totalReturns - float(tempData.loc[(tempData["ranking"] == 4)]["ret_1"][index])
                myStocks.append(tempData.loc[(tempData["ranking"] == 4)]["RP_ENTITY_ID"][index])
        if tempData.loc[(tempData["ranking"] == 4)]["ret_2"][index] >= 0 or tempData.loc[(tempData["ranking"] == 4)]["ret_2"][index] < 0: #ignore nan values
            totalReturns = totalReturns + float(tempData.loc[(tempData["ranking"] == 4)]["ret_2"][index])
    for index in tempData.loc[(tempData["ranking"] == 1)].index:
        if tempData.loc[(tempData["ranking"] == 1)]["RP_ENTITY_ID"][index] in myStocks:
            if tempData.loc[(tempData["ranking"] == 1)]["ret_1"][index] >= 0 or tempData.loc[(tempData["ranking"] == 1)]["ret_1"][index] < 0: #ignore nan values
                totalReturns = totalReturns + float(tempData.loc[(tempData["ranking"] == 1)]["ret_1"][index])
                myStocks.remove(tempData.loc[(tempData["ranking"] == 1)]["RP_ENTITY_ID"][index])
    for index in tempData.loc[(tempData["ranking"] == 2)]["RP_ENTITY_ID"].index:
        if tempData.loc[(tempData["ranking"] == 2)]["RP_ENTITY_ID"][index] in myStocks:
            if tempData.loc[(tempData["ranking"] == 2)]["ret_2"][index] >= 0 or tempData.loc[(tempData["ranking"] == 2)]["ret_2"][index] < 0: #ignore nan values
                totalReturns = totalReturns + float(tempData.loc[(tempData["ranking"] == 2)]["ret_2"][index])
    for index in tempData.loc[(tempData["ranking"] == 3)]["RP_ENTITY_ID"].index:
        if tempData.loc[(tempData["ranking"] == 3)]["RP_ENTITY_ID"][index] in myStocks:
            if tempData.loc[(tempData["ranking"] == 3)]["ret_2"][index] >= 0 or tempData.loc[(tempData["ranking"] == 3)]["ret_2"][index] < 0: #ignore nan values
                totalReturns = totalReturns + float(tempData.loc[(tempData["ranking"] == 3)]["ret_2"][index])
'''
print("total return from all stocks:", totalReturns)
#total return from all stocks: 682.9754904771424

