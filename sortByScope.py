import pandas as pd
from dateutil.parser import parse
#import datetime as dt
fileName = "D:/five years data/five_years_data.csv"

#to do:
#find the appropriate category to filter by
#fill in the remaining code to calculate averages (see your additional tasks and/or initial tasks)
#calculate some stats for the last five years, see if you find anything interesting

target = pd.DataFrame(columns=["monthID","totalItems","totalESSItems","meanESS","meanAES","meanRELEVANCE","meanENS",
                               "meanENS_SIMILARITY_GAP","meanENS_ELAPSED","meanG_ENS","meanG_ENS_SIMILARITY_GAP",
                               "meanG_ENS_ELAPSED","totalPosItems","totalNegItems"])
target = target.fillna(0) # with 0s rather than NaNs
rowNum = 0

for chunk in pd.read_csv(fileName,encoding='latin1',comment="TIMESTAMP_UTC",chunksize=10**6,engine="python",header=None,
                         names=["TIMESTAMP_UTC","RP_ENTITY_ID","ENTITY_TYPE","ENTITY_NAME","POSITION_NAME",
                                "RP_POSITION_ID","COUNTRY_CODE","RELEVANCE","TOPIC","GROUP","TYPE","SUB_TYPE",
                                "PROPERTY","EVALUATION_METHOD","MATURITY","CATEGORY","ESS","AES","AEV","ENS",
                                "ENS_SIMILARITY_GAP","ENS_KEY","ENS_ELAPSED","G_ENS","G_ENS_SIMILARITY_GAP",
                                "G_ENS_KEY","G_ENS_ELAPSED","EVENT_SIMILARITY_KEY","NEWS_TYPE","SOURCE",
                                "RP_STORY_ID","RP_STORY_EVENT_INDEX","RP_STORY_EVENT_COUNT","PRODUCT","COMPANY",
                                "ISIN","CSS","NIP","PEQ","BEE","BMQ","BAM","BCA","BER","ANL_CHG","MCQ"]):
    for index, row in chunk.iterrows():
        #only include items in a certain industry
        #(this is the variable that might not be specific enough for what we're trying to do)
        #if row["TOPIC"] == 0:
            
        date = parse(row["TIMESTAMP_UTC"])
        itemMonth = date.month - 1 #a number 1-12, starting with January as 0
        itemYear = date.year - 2000 #a number 0-16, starting with 2000 as 0
        monthID = str(itemYear)+"-"+str(itemMonth) #an id that tells us both month & year, of the form 2000-01
        if monthID not in target["monthID"]:
            target.loc[rowNum]=[0]
            target["monthID"]=monthID
        
        target.loc[rowNum]["totalItems"] = target["totalItems"] + 1
        target.loc[rowNum]["meanAES"] = target["meanAES"] + row["AES"]
        target.loc[rowNum]["meanRELEVANCE"] = target["meanRELEVANCE"] + row["RELEVANCE"]
        ###############
        if row["ESS"] >= 0 or row["ESS"] < 0:
            target.loc[rowNum]["totalESSItems"] = target.loc[rowNum]["totalESSItems"] + 1
            target.loc[rowNum]["meanESS"] = target.loc[rowNum]["meanESS"] + 1
            target.loc[rowNum]["meanENS"] = target.loc[rowNum]["meanENS"] + 1
            target.loc[rowNum]["meanENS_SIMILARITY_GAP"] = target.loc[rowNum]["meanENS_SIMILARITY_GAP"] + 1
            target.loc[rowNum]["meanENS_ELAPSED"] = target.loc[rowNum]["meanENS_ELAPSED"] + 1
            target.loc[rowNum]["meanG_ENS"] = target.loc[rowNum]["meanG_ENS"] + 1
            target.loc[rowNum]["meanG_ENS_SIMILARITY_GAP"] = target.loc[rowNum]["meanG_ENS_SIMILARITY_GAP"] + 1
            target.loc[rowNum]["meanG_ENS_ELAPSED"] = target.loc[rowNum]["meanG_ENS_ELAPSED"] + 1
            
            if row["ESS"] > 50:
                target.loc[rowNum]["totalPosItems"] = target.loc[rowNum]["totalPosItems"] + 1
            elif row["ESS"] < 50:
                target.loc[rowNum]["totalNegItems"] = target.loc[rowNum]["totalNegItems"] + 1

        #only consider items that relate to macro news
        #maybe check for PLCE identifier? relating to a place rather than a specific company?
        #if row["CATEGORY"] == #whatever:
            #add that row's data to your running counts
        
        rowNum = rowNum + 1
        
totalItemsByDayType = [totalItemsByDay[0]+totalItemsByDay[1]+totalItemsByDay[2]+totalItemsByDay[3]+totalItemsByDay[4],totalItemsByDay[5]+totalItemsByDay[6]]
totalESSItemsByDayType = [totalESSItemsByDay[0]+totalESSItemsByDay[1]+totalESSItemsByDay[2]+totalESSItemsByDay[3]+totalESSItemsByDay[4],totalESSItemsByDay[5]+totalESSItemsByDay[6]]
meanESSbyDayType = [float(meanESSbyDay[0]+meanESSbyDay[1]+meanESSbyDay[2]+meanESSbyDay[3]+meanESSbyDay[4])/totalESSItemsByDayType[0],float(meanESSbyDay[5]+meanESSbyDay[6])/totalESSItemsByDayType[1]]
meanAESbyDayType = [float(meanAESbyDay[0]+meanAESbyDay[1]+meanAESbyDay[2]+meanAESbyDay[3]+meanAESbyDay[4])/totalItemsByDayType[0],float(meanAESbyDay[5]+meanAESbyDay[6])/totalItemsByDayType[1]]
meanRELEVANCEbyDayType = [float(meanRELEVANCEbyDay[0]+meanRELEVANCEbyDay[1]+meanRELEVANCEbyDay[2]+meanRELEVANCEbyDay[3]+meanRELEVANCEbyDay[4])/totalItemsByDayType[0],float(meanRELEVANCEbyDay[5]+meanRELEVANCEbyDay[6])/totalItemsByDayType[1]]
meanENSbyDayType = [float(meanENSbyDay[0]+meanENSbyDay[1]+meanENSbyDay[2]+meanENSbyDay[3]+meanENSbyDay[4])/totalESSItemsByDayType[0],float(meanENSbyDay[5]+meanENSbyDay[6])/totalESSItemsByDayType[1]]
meanENS_ELAPSEDbyDayType = [float(meanENS_ELAPSEDbyDay[0]+meanENS_ELAPSEDbyDay[1]+meanENS_ELAPSEDbyDay[2]+meanENS_ELAPSEDbyDay[3]+meanENS_ELAPSEDbyDay[4])/totalESSItemsByDayType[0],float(meanENS_ELAPSEDbyDay[5]+meanENS_ELAPSEDbyDay[6])/totalESSItemsByDayType[1]]
meanENS_SIMILARITY_GAPbyDayType = [float(meanENS_SIMILARITY_GAPbyDay[0]+meanENS_SIMILARITY_GAPbyDay[1]+meanENS_SIMILARITY_GAPbyDay[2]+meanENS_SIMILARITY_GAPbyDay[3]+meanENS_SIMILARITY_GAPbyDay[4])/totalESSItemsByDayType[0],float(meanENS_SIMILARITY_GAPbyDay[5]+meanENS_SIMILARITY_GAPbyDay[6])/totalESSItemsByDayType[1]]
meanG_ENSbyDayType = [float(meanG_ENSbyDay[0]+meanG_ENSbyDay[1]+meanG_ENSbyDay[2]+meanG_ENSbyDay[3]+meanG_ENSbyDay[4])/totalESSItemsByDayType[0],float(meanG_ENSbyDay[5]+meanG_ENSbyDay[6])/totalESSItemsByDayType[1]]
meanG_ENS_ELAPSEDbyDayType = [float(meanG_ENS_ELAPSEDbyDay[0]+meanG_ENS_ELAPSEDbyDay[1]+meanG_ENS_ELAPSEDbyDay[2]+meanG_ENS_ELAPSEDbyDay[3]+meanG_ENS_ELAPSEDbyDay[4])/totalESSItemsByDayType[0],float(meanG_ENS_ELAPSEDbyDay[5]+meanG_ENS_ELAPSEDbyDay[6])/totalESSItemsByDayType[1]]
meanG_ENS_SIMILARITY_GAPbyDayType = [float(meanG_ENS_SIMILARITY_GAPbyDay[0]+meanG_ENS_SIMILARITY_GAPbyDay[1]+meanG_ENS_SIMILARITY_GAPbyDay[2]+meanG_ENS_SIMILARITY_GAPbyDay[3]+meanG_ENS_SIMILARITY_GAPbyDay[4])/totalESSItemsByDayType[0],float(meanG_ENS_SIMILARITY_GAPbyDay[5]+meanG_ENS_SIMILARITY_GAPbyDay[6])/totalESSItemsByDayType[1]]
totalPosItemsbyDayType = [totalPosItemsbyDay[0]+totalPosItemsbyDay[1]+totalPosItemsbyDay[2]+totalPosItemsbyDay[3]+totalPosItemsbyDay[4],totalPosItemsbyDay[5],totalPosItemsbyDay[6]]
totalNegItemsbyDayType = [totalNegItemsbyDay[0]+totalNegItemsbyDay[1]+totalNegItemsbyDay[2]+totalNegItemsbyDay[3]+totalNegItemsbyDay[4],totalNegItemsbyDay[5],totalNegItemsbyDay[6]]
#calculate average measures of sentiment for each category
#print results