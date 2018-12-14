#Initial Tasks, using all data
import pandas as pd
from dateutil.parser import parse
import datetime as dt
fileName = "C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/five years data/five_years_data.csv"
#fileName = "C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/final_dataset.csv"
#fileName = "C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/2000-01-equities.csv"

chunkCount = -1
#task 1 variable initialization
totalItems = 0
#task 2 variable initialization
totalESSItems = 0
meanESS = 0
meanAES = 0
meanRELEVANCE = 0
meanENS = 0
meanENS_SIMILARITY_GAP = 0
meanENS_ELAPSED = 0
meanG_ENS = 0
meanG_ENS_SIMILARITY_GAP = 0
meanG_ENS_ELAPSED = 0
#task 3 variable initialization
#itemsPerStock = {} #create dictionary that will take RP_ENTITY_IDs as keys and number of news items as values
itemsPerStock = pd.read_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/items_per_stock.csv").to_dict()
#task 4 variable initialization
#itemsPerCountry = {}
itemsPerCountry = pd.read_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/items_per_country.csv").to_dict()
#task 5 variable initialization
#itemsPerTopic = {}
itemsPerTopic = pd.read_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/items_per_topic.csv").to_dict()
#itemsPerGroup = {}
itemsPerGroup = pd.read_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/items_per_group.csv").to_dict()
#itemsPerType = {}
itemsPerType = pd.read_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/items_per_type.csv").to_dict()
#itemsPerCategory = {}
itemsPerCategory = pd.read_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/items_per_category.csv").to_dict()
#itemsPerNewsType = {}
itemsPerNewsType = pd.read_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/items_per_news_type.csv").to_dict()
#task 6/7 variable initialization
ESSItemsPerStock = {}
meanESSPerStock = {}
meanAESPerStock = {}
meanRELEVANCEPerStock = {}
meanENSPerStock = {}
meanENS_SIMILARITY_GAPPerStock = {}
meanENS_ELAPSEDPerStock = {}
meanG_ENSPerStock = {}
meanG_ENS_SIMILARITY_GAPPerStock = {}
meanG_ENS_ELAPSEDPerStock = {}
#task 8 variable initialization
ESSItemsPerCountry = {}
meanESSPerCountry = {}
meanAESPerCountry = {}
meanRELEVANCEPerCountry = {}
meanENSPerCountry = {}
meanENS_SIMILARITY_GAPPerCountry = {}
meanENS_ELAPSEDPerCountry = {}
meanG_ENSPerCountry = {}
meanG_ENS_SIMILARITY_GAPPerCountry = {}
meanG_ENS_ELAPSEDPerCountry = {}
#task 9 variable initialization - topic
ESSItemsPerTopic = {}
meanESSPerTopic = {}
meanAESPerTopic = {}
meanRELEVANCEPerTopic = {}
meanENSPerTopic = {}
meanENS_SIMILARITY_GAPPerTopic = {}
meanENS_ELAPSEDPerTopic = {}
meanG_ENSPerTopic = {}
meanG_ENS_SIMILARITY_GAPPerTopic = {}
meanG_ENS_ELAPSEDPerTopic = {}
#task 9 variable initialization - group
ESSItemsPerGroup = {}
meanESSPerGroup = {}
meanAESPerGroup = {}
meanRELEVANCEPerGroup = {}
meanENSPerGroup = {}
meanENS_SIMILARITY_GAPPerGroup = {}
meanENS_ELAPSEDPerGroup = {}
meanG_ENSPerGroup = {}
meanG_ENS_SIMILARITY_GAPPerGroup = {}
meanG_ENS_ELAPSEDPerGroup = {}
#task 9 variable initialization - type
ESSItemsPerType = {}
meanESSPerType = {}
meanAESPerType = {}
meanRELEVANCEPerType = {}
meanENSPerType = {}
meanENS_SIMILARITY_GAPPerType = {}
meanENS_ELAPSEDPerType = {}
meanG_ENSPerType = {}
meanG_ENS_SIMILARITY_GAPPerType = {}
meanG_ENS_ELAPSEDPerType = {}
#task 9 variable initialization - category
ESSItemsPerCategory = {}
meanESSPerCategory = {}
meanAESPerCategory = {}
meanRELEVANCEPerCategory = {}
meanENSPerCategory = {}
meanENS_SIMILARITY_GAPPerCategory = {}
meanENS_ELAPSEDPerCategory = {}
meanG_ENSPerCategory = {}
meanG_ENS_SIMILARITY_GAPPerCategory = {}
meanG_ENS_ELAPSEDPerCategory = {}
#task 9 variable initialization - news type
ESSItemsPerNewsType = {}
meanESSPerNewsType = {}
meanAESPerNewsType = {}
meanRELEVANCEPerNewsType = {}
meanENSPerNewsType = {}
meanENS_SIMILARITY_GAPPerNewsType = {}
meanENS_ELAPSEDPerNewsType = {}
meanG_ENSPerNewsType = {}
meanG_ENS_SIMILARITY_GAPPerNewsType = {}
meanG_ENS_ELAPSEDPerNewsType = {}
#task 10 variable initialization
totalPosItems = 0
totalNegItems = 0
#task 11 variable initialization
posNewsPerStock = {}
negNewsPerStock = {}
#task 12 variable initialization
posNewsPerCountry = {}
negNewsPerCountry = {}
#task 13 variable initialization
posNewsPerTopic = {}
negNewsPerTopic = {}
posNewsPerGroup = {}
negNewsPerGroup = {}
posNewsPerType = {}
negNewsPerType = {}
posNewsPerCategory = {}
negNewsPerCategory = {}
posNewsPerNewsType = {}
negNewsPerNewsType = {}
#task 14 variable initialization
monCount = 0
tueCount = 0
wedCount = 0
thuCount = 0
friCount = 0
satCount = 0
sunCount = 0
#task 15 variable initialization
weekdayCount = 0
weekendCount = 0
#task 16 variable initialization
before330Count = 0
after330Count = 0
time1 = dt.time(hour=15,minute=30)

def sumNums(group): #takes the sum of a numerical column without blank values, returns a float (used instead of .sum() to ignore missing data)
    count = 0
    for item in group:
        try:
            if item >= 0 or item <0:
                count = count + float(item)
        except:
            count = count
    return count

print("done initializing variables")

for chunk in pd.read_csv(fileName,encoding='latin1',comment="TIMESTAMP_UTC",chunksize=10**6,engine="python",header=None,names=["TIMESTAMP_UTC","RP_ENTITY_ID","ENTITY_TYPE","ENTITY_NAME","POSITION_NAME","RP_POSITION_ID","COUNTRY_CODE","RELEVANCE","TOPIC","GROUP","TYPE","SUB_TYPE","PROPERTY","EVALUATION_METHOD","MATURITY","CATEGORY","ESS","AES","AEV","ENS","ENS_SIMILARITY_GAP","ENS_KEY","ENS_ELAPSED","G_ENS","G_ENS_SIMILARITY_GAP","G_ENS_KEY","G_ENS_ELAPSED","EVENT_SIMILARITY_KEY","NEWS_TYPE","SOURCE","RP_STORY_ID","RP_STORY_EVENT_INDEX","RP_STORY_EVENT_COUNT","PRODUCT","COMPANY","ISIN","CSS","NIP","PEQ","BEE","BMQ","BAM","BCA","BER","ANL_CHG","MCQ"]):
    chunkCount = chunkCount + 1
    print("starting chunk",chunkCount)
    
    #task 1 processing
    totalItems = totalItems + chunk["TIMESTAMP_UTC"].count()
    
    #task 2 processing
    totalESSItems = totalESSItems + chunk["ESS"].count()
    meanESS = meanESS + sumNums(chunk["ESS"])
    meanAES = meanAES + sumNums(chunk["AES"])
    meanRELEVANCE = meanRELEVANCE + sumNums(chunk["RELEVANCE"])
    meanENS = meanENS + sumNums(chunk["ENS"])
    meanENS_SIMILARITY_GAP = meanENS_SIMILARITY_GAP + sumNums(chunk["ENS_SIMILARITY_GAP"])
    meanENS_ELAPSED = meanENS_ELAPSED + sumNums(chunk["ENS_ELAPSED"])
    meanG_ENS = meanG_ENS + sumNums(chunk["G_ENS"])
    meanG_ENS_SIMILARITY_GAP = meanG_ENS_SIMILARITY_GAP + sumNums(chunk["G_ENS_SIMILARITY_GAP"])
    meanG_ENS_ELAPSED = meanG_ENS_ELAPSED + sumNums(chunk["G_ENS_ELAPSED"])
    
    #task 3-7 processing
    for index, row in chunk.iterrows():
        '''
        #3: keep count of news items assigned to each RP_ENTITY_ID
        if row["RP_ENTITY_ID"] in itemsPerStock:
            itemsPerStock[row["RP_ENTITY_ID"]] = itemsPerStock[row["RP_ENTITY_ID"]] + 1
        else:
            itemsPerStock[row["RP_ENTITY_ID"]] = 1
        #4: keep count of news items assigned to each COUNTRY_CODE
        if row["COUNTRY_CODE"] in itemsPerCountry:
            itemsPerCountry[row["COUNTRY_CODE"]] = itemsPerCountry[row["COUNTRY_CODE"]] + 1
        else:
            itemsPerCountry[row["COUNTRY_CODE"]] = 1
        #5: keep count of news items assigned to each TOPIC
        if row["TOPIC"] in itemsPerTopic:
            itemsPerTopic[row["TOPIC"]] = itemsPerTopic[row["TOPIC"]] + 1
        else:
            itemsPerTopic[row["TOPIC"]] = 1
        #5: keep count of news items assigned to each GROUP
        if row["GROUP"] in itemsPerGroup:
            itemsPerGroup[row["GROUP"]] = itemsPerGroup[row["GROUP"]] + 1
        else:
            itemsPerGroup[row["GROUP"]] = 1
        #5: keep count of news items assigned to each TYPE
        if row["TYPE"] in itemsPerType:
            itemsPerType[row["TYPE"]] = itemsPerType[row["TYPE"]] + 1
        else:
            itemsPerType[row["TYPE"]] = 1
        #5: keep count of news items assigned to each CATEGORY
        if row["CATEGORY"] in itemsPerCategory:
            itemsPerCategory[row["CATEGORY"]] = itemsPerCategory[row["CATEGORY"]] + 1
        else:
            itemsPerCategory[row["CATEGORY"]] = 1
        #5: keep count of news items assigned to each NEWS_TYPE
        if row["NEWS_TYPE"] in itemsPerNewsType:
            itemsPerNewsType[row["NEWS_TYPE"]] = itemsPerNewsType[row["NEWS_TYPE"]] + 1
        else:
            itemsPerNewsType[row["NEWS_TYPE"]] = 1
            '''
        if row["ESS"] >= 0 or row["ESS"] < 0:
            try:
                #6/7: keep count of news items with an ESS assigned to each RP_ENTITY_ID
                if row["RP_ENTITY_ID"] in ESSItemsPerStock:
                    ESSItemsPerStock[row["RP_ENTITY_ID"]] = ESSItemsPerStock[row["RP_ENTITY_ID"]] + 1
                else:
                    ESSItemsPerStock[row["RP_ENTITY_ID"]] = 1
            except:
                print("error thrown for step 6/7 #1")
            
            try:    
                #8: keep count of news items with an ESS assigned to each COUNTRY_CODE
                if row["COUNTRY_CODE"] in ESSItemsPerCountry:
                    ESSItemsPerCountry[row["COUNTRY_CODE"]] = ESSItemsPerCountry[row["COUNTRY_CODE"]] + 1
                else:
                    ESSItemsPerCountry[row["COUNTRY_CODE"]] = 1
            except:
                print("error thrown for step 8 #1")
                
            try:
                #9: keep count of news items with an ESS assigned to each TOPIC
                if row["TOPIC"] in ESSItemsPerTopic:
                    ESSItemsPerTopic[row["TOPIC"]] = ESSItemsPerTopic[row["TOPIC"]] + 1
                else:
                    ESSItemsPerTopic[row["TOPIC"]] = 1
                #9: keep count of news items with an ESS assigned to each GROUP
                if row["GROUP"] in ESSItemsPerGroup:
                    ESSItemsPerGroup[row["GROUP"]] = ESSItemsPerGroup[row["GROUP"]] + 1
                else:
                    ESSItemsPerGroup[row["GROUP"]] = 1
                #9: keep count of news items with an ESS assigned to each TYPE
                if row["TYPE"] in ESSItemsPerType:
                    ESSItemsPerType[row["TYPE"]] = ESSItemsPerType[row["TYPE"]] + 1
                else:
                    ESSItemsPerType[row["TYPE"]] = 1
                #9: keep count of news items with an ESS assigned to each CATEGORY
                if row["CATEGORY"] in ESSItemsPerCategory:
                    ESSItemsPerCategory[row["CATEGORY"]] = ESSItemsPerCategory[row["CATEGORY"]] + 1
                else:
                    ESSItemsPerCategory[row["CATEGORY"]] = 1
                #9: keep count of news items with an ESS assigned to each NEWS_TYPE
                if row["NEWS_TYPE"] in ESSItemsPerNewsType:
                    ESSItemsPerNewsType[row["NEWS_TYPE"]] = ESSItemsPerNewsType[row["NEWS_TYPE"]] + 1
                else:
                    ESSItemsPerNewsType[row["NEWS_TYPE"]] = 1
            except:
                print("error thrown for step 9 #1")
                
            try:
                #6/7: keep count of total ESS for each RP_ENTITY_ID
                if row["RP_ENTITY_ID"] in meanESSPerStock:
                    meanESSPerStock[row["RP_ENTITY_ID"]] = meanESSPerStock[row["RP_ENTITY_ID"]] + row["ESS"]
                else:
                    meanESSPerStock[row["RP_ENTITY_ID"]] = row["ESS"]
            except:
                print("error thrown for step 6/7 #2")
                
            try:
                #8: keep count of total ESS for each COUNTRY_CODE
                if row["COUNTRY_CODE"] in meanESSPerCountry:
                    meanESSPerCountry[row["COUNTRY_CODE"]] = meanESSPerCountry[row["COUNTRY_CODE"]] + row["ESS"]
                else:
                    meanESSPerCountry[row["COUNTRY_CODE"]] = row["ESS"]
            except:
                print("error thrown for step 8 #2")
             
            try:   
                #9: keep count of total ESS for each TOPIC
                if row["TOPIC"] in meanESSPerTopic:
                    meanESSPerTopic[row["TOPIC"]] = meanESSPerTopic[row["TOPIC"]] + row["ESS"]
                else:
                    meanESSPerTopic[row["TOPIC"]] = row["ESS"]
                #9: keep count of total ESS for each GROUP
                if row["GROUP"] in meanESSPerGroup:
                    meanESSPerGroup[row["GROUP"]] = meanESSPerGroup[row["GROUP"]] + row["ESS"]
                else:
                    meanESSPerGroup[row["GROUP"]] = row["ESS"]
                #9: keep count of total ESS for each TYPE
                if row["TYPE"] in meanESSPerType:
                    meanESSPerType[row["TYPE"]] = meanESSPerType[row["TYPE"]] + row["ESS"]
                else:
                    meanESSPerType[row["TYPE"]] = row["ESS"]
                #9: keep count of total ESS for each CATEGORY
                if row["CATEGORY"] in meanESSPerCategory:
                    meanESSPerCategory[row["CATEGORY"]] = meanESSPerCategory[row["CATEGORY"]] + row["ESS"]
                else:
                    meanESSPerCategory[row["CATEGORY"]] = row["ESS"]
                #9: keep count of total ESS for each NEWS_TYPE
                if row["NEWS_TYPE"] in meanESSPerNewsType:
                    meanESSPerNewsType[row["NEWS_TYPE"]] = meanESSPerNewsType[row["NEWS_TYPE"]] + row["ESS"]
                else:
                    meanESSPerNewsType[row["NEWS_TYPE"]] = row["ESS"]
            except:
                print("error thrown for step 9 #2")
                
            try:
                #6/7: keep count of total ENS for each RP_ENTITY_ID
                if row["RP_ENTITY_ID"] in meanENSPerStock:
                    meanENSPerStock[row["RP_ENTITY_ID"]] = meanENSPerStock[row["RP_ENTITY_ID"]] + row["ENS"]
                else:
                    meanENSPerStock[row["RP_ENTITY_ID"]] = row["ENS"]
            except:
                print("error thrown for step 6/7 #3")
 
            try:               
                #8: keep count of total ENS for each COUNTRY_CODE
                if row["COUNTRY_CODE"] in meanENSPerCountry:
                    meanENSPerCountry[row["COUNTRY_CODE"]] = meanENSPerCountry[row["COUNTRY_CODE"]] + row["ENS"]
                else:
                    meanENSPerCountry[row["COUNTRY_CODE"]] = row["ENS"]
            except:
                print("error thrown for step 8 #3")
                
            try:
                #9: keep count of total ENS for each TOPIC
                if row["TOPIC"] in meanENSPerTopic:
                    meanENSPerTopic[row["TOPIC"]] = meanENSPerTopic[row["TOPIC"]] + row["ENS"]
                else:
                    meanENSPerTopic[row["TOPIC"]] = row["ENS"]
                #9: keep count of total ENS for each GROUP
                if row["GROUP"] in meanENSPerGroup:
                    meanENSPerGroup[row["GROUP"]] = meanENSPerGroup[row["GROUP"]] + row["ENS"]
                else:
                    meanENSPerGroup[row["GROUP"]] = row["ENS"]
                #9: keep count of total ENS for each TYPE
                if row["TYPE"] in meanENSPerType:
                    meanENSPerType[row["TYPE"]] = meanENSPerType[row["TYPE"]] + row["ENS"]
                else:
                    meanENSPerType[row["TYPE"]] = row["ENS"]
                #9: keep count of total ENS for each CATEGORY
                if row["CATEGORY"] in meanENSPerCategory:
                    meanENSPerCategory[row["CATEGORY"]] = meanENSPerCategory[row["CATEGORY"]] + row["ENS"]
                else:
                    meanENSPerCategory[row["CATEGORY"]] = row["ENS"]
                #9: keep count of total ENS for each NEWS_TYPE
                if row["NEWS_TYPE"] in meanENSPerNewsType:
                    meanENSPerNewsType[row["NEWS_TYPE"]] = meanENSPerNewsType[row["NEWS_TYPE"]] + row["ENS"]
                else:
                    meanENSPerNewsType[row["NEWS_TYPE"]] = row["ENS"]
            except:
                print("error thrown for step 9 #3")    
            
            try:
                #6/7: keep count of total ENS_SIMILARITY_GAP for each RP_ENTITY_ID
                if row["RP_ENTITY_ID"] in meanENS_SIMILARITY_GAPPerStock:
                    meanENS_SIMILARITY_GAPPerStock[row["RP_ENTITY_ID"]] = meanENS_SIMILARITY_GAPPerStock[row["RP_ENTITY_ID"]] + row["ENS_SIMILARITY_GAP"]
                else:
                    meanENS_SIMILARITY_GAPPerStock[row["RP_ENTITY_ID"]] = row["ENS_SIMILARITY_GAP"]
            except:
                print("error thrown for step 6/7 #4")
                
            try:
                #8: keep count of total ENS_SIMILARITY_GAP for each COUNTRY_CODE
                if row["COUNTRY_CODE"] in meanENS_SIMILARITY_GAPPerCountry:
                    meanENS_SIMILARITY_GAPPerCountry[row["COUNTRY_CODE"]] = meanENS_SIMILARITY_GAPPerCountry[row["COUNTRY_CODE"]] + row["ENS_SIMILARITY_GAP"]
                else:
                    meanENS_SIMILARITY_GAPPerCountry[row["COUNTRY_CODE"]] = row["ENS_SIMILARITY_GAP"]
            except:
                print("error thrown for step 8 #4")
                
            try:
                #9: keep count of total ENS_SIMILARITY_GAP for each TOPIC
                if row["TOPIC"] in meanENS_SIMILARITY_GAPPerTopic:
                    meanENS_SIMILARITY_GAPPerTopic[row["TOPIC"]] = meanENS_SIMILARITY_GAPPerTopic[row["TOPIC"]] + row["ENS_SIMILARITY_GAP"]
                else:
                    meanENS_SIMILARITY_GAPPerTopic[row["TOPIC"]] = row["ENS_SIMILARITY_GAP"]
                #9: keep count of total ENS_SIMILARITY_GAP for each GROUP
                if row["GROUP"] in meanENS_SIMILARITY_GAPPerGroup:
                    meanENS_SIMILARITY_GAPPerGroup[row["GROUP"]] = meanENS_SIMILARITY_GAPPerGroup[row["GROUP"]] + row["ENS_SIMILARITY_GAP"]
                else:
                    meanENS_SIMILARITY_GAPPerGroup[row["GROUP"]] = row["ENS_SIMILARITY_GAP"]
                #9: keep count of total ENS_SIMILARITY_GAP for each TYPE
                if row["TYPE"] in meanENS_SIMILARITY_GAPPerType:
                    meanENS_SIMILARITY_GAPPerType[row["TYPE"]] = meanENS_SIMILARITY_GAPPerType[row["TYPE"]] + row["ENS_SIMILARITY_GAP"]
                else:
                    meanENS_SIMILARITY_GAPPerType[row["TYPE"]] = row["ENS_SIMILARITY_GAP"]
                #9: keep count of total ENS_SIMILARITY_GAP for each CATEGORY
                if row["CATEGORY"] in meanENS_SIMILARITY_GAPPerCategory:
                    meanENS_SIMILARITY_GAPPerCategory[row["CATEGORY"]] = meanENS_SIMILARITY_GAPPerCategory[row["CATEGORY"]] + row["ENS_SIMILARITY_GAP"]
                else:
                    meanENS_SIMILARITY_GAPPerCategory[row["CATEGORY"]] = row["ENS_SIMILARITY_GAP"]
                #9: keep count of total ENS_SIMILARITY_GAP for each NEWS_TYPE
                if row["NEWS_TYPE"] in meanENS_SIMILARITY_GAPPerNewsType:
                    meanENS_SIMILARITY_GAPPerNewsType[row["NEWS_TYPE"]] = meanENS_SIMILARITY_GAPPerNewsType[row["NEWS_TYPE"]] + row["ENS_SIMILARITY_GAP"]
                else:
                    meanENS_SIMILARITY_GAPPerNewsType[row["NEWS_TYPE"]] = row["ENS_SIMILARITY_GAP"]
            except:
                print("error thrown for step 9 #4")
            
            try:
                #6/7: keep count of total ENS_ELAPSED for each RP_ENTITY_ID
                if row["RP_ENTITY_ID"] in meanENS_ELAPSEDPerStock:
                    meanENS_ELAPSEDPerStock[row["RP_ENTITY_ID"]] = meanENS_ELAPSEDPerStock[row["RP_ENTITY_ID"]] + row["ENS_ELAPSED"]
                else:
                    meanENS_ELAPSEDPerStock[row["RP_ENTITY_ID"]] = row["ENS_ELAPSED"]
            except:
                print("error thrown for step 6/7 #5")
                
            try:
                #8: keep count of total ENS_ELAPSED for each COUNTRY_CODE
                if row["COUNTRY_CODE"] in meanENS_ELAPSEDPerCountry:
                    meanENS_ELAPSEDPerCountry[row["COUNTRY_CODE"]] = meanENS_ELAPSEDPerCountry[row["COUNTRY_CODE"]] + row["ENS_ELAPSED"]
                else:
                    meanENS_ELAPSEDPerCountry[row["COUNTRY_CODE"]] = row["ENS_ELAPSED"]
            except:
                print("error thrown for step 8 #5")
            
            try:
                #9: keep count of total ENS_ELAPSED for each TOPIC
                if row["TOPIC"] in meanENS_ELAPSEDPerTopic:
                    meanENS_ELAPSEDPerTopic[row["TOPIC"]] = meanENS_ELAPSEDPerTopic[row["TOPIC"]] + row["ENS_ELAPSED"]
                else:
                    meanENS_ELAPSEDPerTopic[row["TOPIC"]] = row["ENS_ELAPSED"]
                #9: keep count of total ENS_ELAPSED for each GROUP
                if row["GROUP"] in meanENS_ELAPSEDPerGroup:
                    meanENS_ELAPSEDPerGroup[row["GROUP"]] = meanENS_ELAPSEDPerGroup[row["GROUP"]] + row["ENS_ELAPSED"]
                else:
                    meanENS_ELAPSEDPerGroup[row["GROUP"]] = row["ENS_ELAPSED"]
                #9: keep count of total ENS_ELAPSED for each TYPE
                if row["TYPE"] in meanENS_ELAPSEDPerType:
                    meanENS_ELAPSEDPerType[row["TYPE"]] = meanENS_ELAPSEDPerType[row["TYPE"]] + row["ENS_ELAPSED"]
                else:
                    meanENS_ELAPSEDPerType[row["TYPE"]] = row["ENS_ELAPSED"]
                #9: keep count of total ENS_ELAPSED for each CATEGORY
                if row["CATEGORY"] in meanENS_ELAPSEDPerCategory:
                    meanENS_ELAPSEDPerCategory[row["CATEGORY"]] = meanENS_ELAPSEDPerCategory[row["CATEGORY"]] + row["ENS_ELAPSED"]
                else:
                    meanENS_ELAPSEDPerCategory[row["CATEGORY"]] = row["ENS_ELAPSED"]
                #9: keep count of total ENS_ELAPSED for each NEWS_TYPE
                if row["NEWS_TYPE"] in meanENS_ELAPSEDPerNewsType:
                    meanENS_ELAPSEDPerNewsType[row["NEWS_TYPE"]] = meanENS_ELAPSEDPerNewsType[row["NEWS_TYPE"]] + row["ENS_ELAPSED"]
                else:
                    meanENS_ELAPSEDPerNewsType[row["NEWS_TYPE"]] = row["ENS_ELAPSED"]
            except:
                print("error thrown for step 9 #5")
                
            try:
                #6/7: keep count of total G_ENS for each RP_ENTITY_ID
                if row["RP_ENTITY_ID"] in meanG_ENSPerStock:
                    meanG_ENSPerStock[row["RP_ENTITY_ID"]] = meanG_ENSPerStock[row["RP_ENTITY_ID"]] + row["G_ENS"]
                else:
                    meanG_ENSPerStock[row["RP_ENTITY_ID"]] = row["G_ENS"]
            except:
                print("error thrown for step 6/7 #6")
            
            try:
                #8: keep count of total G_ENS for each COUNTRY_CODE
                if row["COUNTRY_CODE"] in meanG_ENSPerCountry:
                    meanG_ENSPerCountry[row["COUNTRY_CODE"]] = meanG_ENSPerCountry[row["COUNTRY_CODE"]] + row["G_ENS"]
                else:
                    meanG_ENSPerCountry[row["COUNTRY_CODE"]] = row["G_ENS"]
            except:
                print("error thrown for step 8 #6")
            
            try:
                #9: keep count of total G_ENS for each TOPIC
                if row["TOPIC"] in meanG_ENSPerTopic:
                    meanG_ENSPerTopic[row["TOPIC"]] = meanG_ENSPerTopic[row["TOPIC"]] + row["G_ENS"]
                else:
                    meanG_ENSPerTopic[row["TOPIC"]] = row["G_ENS"]
                #9: keep count of total G_ENS for each GROUP
                if row["GROUP"] in meanG_ENSPerGroup:
                    meanG_ENSPerGroup[row["GROUP"]] = meanG_ENSPerGroup[row["GROUP"]] + row["G_ENS"]
                else:
                    meanG_ENSPerGroup[row["GROUP"]] = row["G_ENS"]
                #9: keep count of total G_ENS for each TYPE
                if row["TYPE"] in meanG_ENSPerType:
                    meanG_ENSPerType[row["TYPE"]] = meanG_ENSPerType[row["TYPE"]] + row["G_ENS"]
                else:
                    meanG_ENSPerType[row["TYPE"]] = row["G_ENS"]
                #9: keep count of total G_ENS for each CATEGORY
                if row["CATEGORY"] in meanG_ENSPerCategory:
                    meanG_ENSPerCategory[row["CATEGORY"]] = meanG_ENSPerCategory[row["CATEGORY"]] + row["G_ENS"]
                else:
                    meanG_ENSPerCategory[row["CATEGORY"]] = row["G_ENS"]
                #9: keep count of total G_ENS for each NEWS_TYPE
                if row["NEWS_TYPE"] in meanG_ENSPerNewsType:
                    meanG_ENSPerNewsType[row["NEWS_TYPE"]] = meanG_ENSPerNewsType[row["NEWS_TYPE"]] + row["G_ENS"]
                else:
                    meanG_ENSPerNewsType[row["NEWS_TYPE"]] = row["G_ENS"]
            except:
                print("error thrown for step 9 #6")
            
            try:
                #6/7: keep count of total G_ENS_SIMILARITY_GAP for each RP_ENTITY_ID
                if row["RP_ENTITY_ID"] in meanG_ENS_SIMILARITY_GAPPerStock:
                    meanG_ENS_SIMILARITY_GAPPerStock[row["RP_ENTITY_ID"]] = meanG_ENS_SIMILARITY_GAPPerStock[row["RP_ENTITY_ID"]] + row["G_ENS_SIMILARITY_GAP"]
                else:
                    meanG_ENS_SIMILARITY_GAPPerStock[row["RP_ENTITY_ID"]] = row["G_ENS_SIMILARITY_GAP"]
            except:
                print("error thrown for step 6/7 #7")

            try:                
                #8: keep count of total G_ENS_SIMILARITY_GAP for each COUNTRY_CODE
                if row["COUNTRY_CODE"] in meanG_ENS_SIMILARITY_GAPPerCountry:
                    meanG_ENS_SIMILARITY_GAPPerCountry[row["COUNTRY_CODE"]] = meanG_ENS_SIMILARITY_GAPPerCountry[row["COUNTRY_CODE"]] + row["G_ENS_SIMILARITY_GAP"]
                else:
                    meanG_ENS_SIMILARITY_GAPPerCountry[row["COUNTRY_CODE"]] = row["G_ENS_SIMILARITY_GAP"]
            except:
                print("error thrown for step 8 #7")
                
            try:
                #9: keep count of total G_ENS_SIMILARITY_GAP for each TOPIC
                if row["TOPIC"] in meanG_ENS_SIMILARITY_GAPPerTopic:
                    meanG_ENS_SIMILARITY_GAPPerTopic[row["TOPIC"]] = meanG_ENS_SIMILARITY_GAPPerTopic[row["TOPIC"]] + row["G_ENS_SIMILARITY_GAP"]
                else:
                    meanG_ENS_SIMILARITY_GAPPerTopic[row["TOPIC"]] = row["G_ENS_SIMILARITY_GAP"]
                #9: keep count of total G_ENS_SIMILARITY_GAP for each GROUP
                if row["GROUP"] in meanG_ENS_SIMILARITY_GAPPerGroup:
                    meanG_ENS_SIMILARITY_GAPPerGroup[row["GROUP"]] = meanG_ENS_SIMILARITY_GAPPerGroup[row["GROUP"]] + row["G_ENS_SIMILARITY_GAP"]
                else:
                    meanG_ENS_SIMILARITY_GAPPerGroup[row["GROUP"]] = row["G_ENS_SIMILARITY_GAP"]
                #9: keep count of total G_ENS_SIMILARITY_GAP for each TYPE
                if row["TYPE"] in meanG_ENS_SIMILARITY_GAPPerType:
                    meanG_ENS_SIMILARITY_GAPPerType[row["TYPE"]] = meanG_ENS_SIMILARITY_GAPPerType[row["TYPE"]] + row["G_ENS_SIMILARITY_GAP"]
                else:
                    meanG_ENS_SIMILARITY_GAPPerType[row["TYPE"]] = row["G_ENS_SIMILARITY_GAP"]
                #9: keep count of total G_ENS_SIMILARITY_GAP for each CATEGORY
                if row["CATEGORY"] in meanG_ENS_SIMILARITY_GAPPerCategory:
                    meanG_ENS_SIMILARITY_GAPPerCategory[row["CATEGORY"]] = meanG_ENS_SIMILARITY_GAPPerCategory[row["CATEGORY"]] + row["G_ENS_SIMILARITY_GAP"]
                else:
                    meanG_ENS_SIMILARITY_GAPPerCategory[row["CATEGORY"]] = row["G_ENS_SIMILARITY_GAP"]
                #9: keep count of total G_ENS_SIMILARITY_GAP for each NEWS_TYPE
                if row["NEWS_TYPE"] in meanG_ENS_SIMILARITY_GAPPerNewsType:
                    meanG_ENS_SIMILARITY_GAPPerNewsType[row["NEWS_TYPE"]] = meanG_ENS_SIMILARITY_GAPPerNewsType[row["NEWS_TYPE"]] + row["G_ENS_SIMILARITY_GAP"]
                else:
                    meanG_ENS_SIMILARITY_GAPPerNewsType[row["NEWS_TYPE"]] = row["G_ENS_SIMILARITY_GAP"]
            except:
                print("error thrown for step 9 #7")
                
            try:
                #6/7: keep count of total G_ENS_ELAPSED for each RP_ENTITY_ID
                if row["RP_ENTITY_ID"] in meanG_ENS_ELAPSEDPerStock:
                    meanG_ENS_ELAPSEDPerStock[row["RP_ENTITY_ID"]] = meanG_ENS_ELAPSEDPerStock[row["RP_ENTITY_ID"]] + row["G_ENS_ELAPSED"]
                else:
                    meanG_ENS_ELAPSEDPerStock[row["RP_ENTITY_ID"]] = row["G_ENS_ELAPSED"]
            except:
                print("error thrown for step 6/7 #8")
                
            try:
                #8: keep count of total G_ENS_ELAPSED for each COUNTRY_CODE
                if row["COUNTRY_CODE"] in meanG_ENS_ELAPSEDPerCountry:
                    meanG_ENS_ELAPSEDPerCountry[row["COUNTRY_CODE"]] = meanG_ENS_ELAPSEDPerCountry[row["COUNTRY_CODE"]] + row["G_ENS_ELAPSED"]
                else:
                    meanG_ENS_ELAPSEDPerCountry[row["COUNTRY_CODE"]] = row["G_ENS_ELAPSED"]
            except:
                print("error thrown for step 8 #8")
                
            try:
                #9: keep count of total G_ENS_ELAPSED for each TOPIC
                if row["TOPIC"] in meanG_ENS_ELAPSEDPerTopic:
                    meanG_ENS_ELAPSEDPerTopic[row["TOPIC"]] = meanG_ENS_ELAPSEDPerTopic[row["TOPIC"]] + row["G_ENS_ELAPSED"]
                else:
                    meanG_ENS_ELAPSEDPerTopic[row["TOPIC"]] = row["G_ENS_ELAPSED"]
                #9: keep count of total G_ENS_ELAPSED for each GROUP
                if row["GROUP"] in meanG_ENS_ELAPSEDPerGroup:
                    meanG_ENS_ELAPSEDPerGroup[row["GROUP"]] = meanG_ENS_ELAPSEDPerGroup[row["GROUP"]] + row["G_ENS_ELAPSED"]
                else:
                    meanG_ENS_ELAPSEDPerGroup[row["GROUP"]] = row["G_ENS_ELAPSED"]
                #9: keep count of total G_ENS_ELAPSED for each TYPE
                if row["TYPE"] in meanG_ENS_ELAPSEDPerType:
                    meanG_ENS_ELAPSEDPerType[row["TYPE"]] = meanG_ENS_ELAPSEDPerType[row["TYPE"]] + row["G_ENS_ELAPSED"]
                else:
                    meanG_ENS_ELAPSEDPerType[row["TYPE"]] = row["G_ENS_ELAPSED"]
                #9: keep count of total G_ENS_ELAPSED for each CATEGORY
                if row["CATEGORY"] in meanG_ENS_ELAPSEDPerCategory:
                    meanG_ENS_ELAPSEDPerCategory[row["CATEGORY"]] = meanG_ENS_ELAPSEDPerCategory[row["CATEGORY"]] + row["G_ENS_ELAPSED"]
                else:
                    meanG_ENS_ELAPSEDPerCategory[row["CATEGORY"]] = row["G_ENS_ELAPSED"]
                #9: keep count of total G_ENS_ELAPSED for each NEWS_TYPE
                if row["NEWS_TYPE"] in meanG_ENS_ELAPSEDPerNewsType:
                    meanG_ENS_ELAPSEDPerNewsType[row["NEWS_TYPE"]] = meanG_ENS_ELAPSEDPerNewsType[row["NEWS_TYPE"]] + row["G_ENS_ELAPSED"]
                else:
                    meanG_ENS_ELAPSEDPerNewsType[row["NEWS_TYPE"]] = row["G_ENS_ELAPSED"]
            except:
                print("error thrown for step 9 #8")
        
        try:
            #6/7: keep count of total AES for each RP_ENTITY_ID
            if row["RP_ENTITY_ID"] in meanAESPerStock:
                meanAESPerStock[row["RP_ENTITY_ID"]] = meanAESPerStock[row["RP_ENTITY_ID"]] + row["AES"]
            else:
                meanAESPerStock[row["RP_ENTITY_ID"]] = row["AES"]
        except:
            print("error thrown for step 6/7 #9")
        
        try:
            #8: keep count of total AES for each COUNTRY_CODE
            if row["COUNTRY_CODE"] in meanAESPerCountry:
                meanAESPerCountry[row["COUNTRY_CODE"]] = meanAESPerCountry[row["COUNTRY_CODE"]] + row["AES"]
            else:
                meanAESPerCountry[row["COUNTRY_CODE"]] = row["AES"]
        except:
            print("error thrown for step 8 #9")
        
        try:
            #9: keep count of total AES for each TOPIC
            if row["TOPIC"] in meanAESPerTopic:
                meanAESPerTopic[row["TOPIC"]] = meanAESPerTopic[row["TOPIC"]] + row["AES"]
            else:
                meanAESPerTopic[row["TOPIC"]] = row["AES"]
            #9: keep count of total AES for each GROUP
            if row["GROUP"] in meanAESPerGroup:
                meanAESPerGroup[row["GROUP"]] = meanAESPerGroup[row["GROUP"]] + row["AES"]
            else:
                meanAESPerGroup[row["GROUP"]] = row["AES"]
            #9: keep count of total AES for each TYPE
            if row["TYPE"] in meanAESPerType:
                meanAESPerType[row["TYPE"]] = meanAESPerType[row["TYPE"]] + row["AES"]
            else:
                meanAESPerType[row["TYPE"]] = row["AES"]
            #9: keep count of total AES for each CATEGORY
            if row["CATEGORY"] in meanAESPerCategory:
                meanAESPerCategory[row["CATEGORY"]] = meanAESPerCategory[row["CATEGORY"]] + row["AES"]
            else:
                meanAESPerCategory[row["CATEGORY"]] = row["AES"]
            #9: keep count of total AES for each NEWS_TYPE
            if row["NEWS_TYPE"] in meanAESPerNewsType:
                meanAESPerNewsType[row["NEWS_TYPE"]] = meanAESPerNewsType[row["NEWS_TYPE"]] + row["AES"]
            else:
                meanAESPerNewsType[row["NEWS_TYPE"]] = row["AES"]
        except:
            print("error thrown for step 9 #9")
            
        try:
            #6/7: keep count of total RELEVANCE for each RP_ENTITY_ID
            if row["RP_ENTITY_ID"] in meanRELEVANCEPerStock:
                meanRELEVANCEPerStock[row["RP_ENTITY_ID"]] = meanRELEVANCEPerStock[row["RP_ENTITY_ID"]] + row["RELEVANCE"]
            else:
                meanRELEVANCEPerStock[row["RP_ENTITY_ID"]] = row["RELEVANCE"]
        except:
            print("error thrown for step 6/7 #10")
            
        try:
            #8: keep count of total RELEVANCE for each COUNTRY_CODE
            if row["COUNTRY_CODE"] in meanRELEVANCEPerCountry:
                meanRELEVANCEPerCountry[row["COUNTRY_CODE"]] = meanRELEVANCEPerCountry[row["COUNTRY_CODE"]] + row["RELEVANCE"]
            else:
                meanRELEVANCEPerCountry[row["COUNTRY_CODE"]] = row["RELEVANCE"]
        except:
            print("error thrown for step 8 #10")
            
        try:
            #9: keep count of total RELEVANCE for each TOPIC
            if row["TOPIC"] in meanRELEVANCEPerTopic:
                meanRELEVANCEPerTopic[row["TOPIC"]] = meanRELEVANCEPerTopic[row["TOPIC"]] + row["RELEVANCE"]
            else:
                meanRELEVANCEPerTopic[row["TOPIC"]] = row["RELEVANCE"]
            #9: keep count of total RELEVANCE for each GROUP
            if row["GROUP"] in meanRELEVANCEPerGroup:
                meanRELEVANCEPerGroup[row["GROUP"]] = meanRELEVANCEPerGroup[row["GROUP"]] + row["RELEVANCE"]
            else:
                meanRELEVANCEPerGroup[row["GROUP"]] = row["RELEVANCE"]
            #9: keep count of total RELEVANCE for each GROUP
            if row["GROUP"] in meanRELEVANCEPerGroup:
                meanRELEVANCEPerGroup[row["GROUP"]] = meanRELEVANCEPerGroup[row["GROUP"]] + row["RELEVANCE"]
            else:
                meanRELEVANCEPerGroup[row["GROUP"]] = row["RELEVANCE"]
            #9: keep count of total RELEVANCE for each TYPE
            if row["TYPE"] in meanRELEVANCEPerType:
                meanRELEVANCEPerType[row["TYPE"]] = meanRELEVANCEPerType[row["TYPE"]] + row["RELEVANCE"]
            else:
                meanRELEVANCEPerType[row["TYPE"]] = row["RELEVANCE"]
            #9: keep count of total RELEVANCE for each CATEGORY
            if row["CATEGORY"] in meanRELEVANCEPerCategory:
                meanRELEVANCEPerCategory[row["CATEGORY"]] = meanRELEVANCEPerCategory[row["CATEGORY"]] + row["RELEVANCE"]
            else:
                meanRELEVANCEPerCategory[row["CATEGORY"]] = row["RELEVANCE"]
            #9: keep count of total RELEVANCE for each NEWS_TYPE
            if row["NEWS_TYPE"] in meanRELEVANCEPerNewsType:
                meanRELEVANCEPerNewsType[row["NEWS_TYPE"]] = meanRELEVANCEPerNewsType[row["NEWS_TYPE"]] + row["RELEVANCE"]
            else:
                meanRELEVANCEPerNewsType[row["NEWS_TYPE"]] = row["RELEVANCE"]
        except:
            print("error thrown for step 9 #10")
        
        try:
            #10: keep count of total positive/negative news items
            if row["ESS"]>50:
                totalPosItems = totalPosItems + 1
                if row["RP_ENTITY_ID"] in posNewsPerStock:
                    posNewsPerStock[row["RP_ENTITY_ID"]] = posNewsPerStock[row["RP_ENTITY_ID"]] + 1
                else:
                    posNewsPerStock[row["RP_ENTITY_ID"]] = 1
                if row["COUNTRY_CODE"] in posNewsPerCountry:
                    posNewsPerCountry[row["COUNTRY_CODE"]] = posNewsPerCountry[row["COUNTRY_CODE"]] + 1
                else:
                    posNewsPerCountry[row["COUNTRY_CODE"]] = 1
                if row["TOPIC"] in posNewsPerTopic:
                    posNewsPerTopic[row["TOPIC"]] = posNewsPerTopic[row["TOPIC"]] + 1
                else:
                    posNewsPerTopic[row["TOPIC"]] = 1
                if row["GROUP"] in posNewsPerGroup:
                    posNewsPerGroup[row["GROUP"]] = posNewsPerGroup[row["GROUP"]] + 1
                else:
                    posNewsPerGroup[row["GROUP"]] = 1
                if row["TYPE"] in posNewsPerType:
                    posNewsPerType[row["TYPE"]] = posNewsPerType[row["TYPE"]] + 1
                else:
                    posNewsPerType[row["TYPE"]] = 1
                if row["CATEGORY"] in posNewsPerCategory:
                    posNewsPerCategory[row["CATEGORY"]] = posNewsPerCategory[row["CATEGORY"]] + 1
                else:
                    posNewsPerCategory[row["CATEGORY"]] = 1
                if row["NEWS_TYPE"] in posNewsPerNewsType:
                    posNewsPerNewsType[row["NEWS_TYPE"]] = posNewsPerNewsType[row["NEWS_TYPE"]] + 1
                else:
                    posNewsPerNewsType[row["NEWS_TYPE"]] = 1
            elif row["ESS"]<50:
                totalNegItems = totalNegItems + 1
                if row["RP_ENTITY_ID"] in negNewsPerStock:
                    negNewsPerStock[row["RP_ENTITY_ID"]] = negNewsPerStock[row["RP_ENTITY_ID"]] + 1
                else:
                    negNewsPerStock[row["RP_ENTITY_ID"]] = 1
                if row["COUNTRY_CODE"] in negNewsPerCountry:
                    negNewsPerCountry[row["COUNTRY_CODE"]] = negNewsPerCountry[row["COUNTRY_CODE"]] + 1
                else:
                    negNewsPerCountry[row["COUNTRY_CODE"]] = 1
                if row["TOPIC"] in negNewsPerTopic:
                    negNewsPerTopic[row["TOPIC"]] = negNewsPerTopic[row["TOPIC"]] + 1
                else:
                    negNewsPerTopic[row["TOPIC"]] = 1
                if row["GROUP"] in negNewsPerGroup:
                    negNewsPerGroup[row["GROUP"]] = negNewsPerGroup[row["GROUP"]] + 1
                else:
                    negNewsPerGroup[row["GROUP"]] = 1
                if row["TYPE"] in negNewsPerType:
                    negNewsPerType[row["TYPE"]] = negNewsPerType[row["TYPE"]] + 1
                else:
                    negNewsPerType[row["TYPE"]] = 1
                if row["CATEGORY"] in negNewsPerCategory:
                    negNewsPerCategory[row["CATEGORY"]] = negNewsPerCategory[row["CATEGORY"]] + 1
                else:
                    negNewsPerCategory[row["CATEGORY"]] = 1
                if row["NEWS_TYPE"] in negNewsPerNewsType:
                    negNewsPerNewsType[row["NEWS_TYPE"]] = negNewsPerNewsType[row["NEWS_TYPE"]] + 1
                else:
                    negNewsPerNewsType[row["NEWS_TYPE"]] = 1
        except:
            print("error thrown for positivity stuff")
        '''
        try:
            #14: keep count of total news items for each day of the week
            date = parse(row["TIMESTAMP_UTC"])
            dayOfWeek = date.weekday()
            if dayOfWeek==0:
                monCount = monCount + 1
            elif dayOfWeek==1:
                tueCount = tueCount + 1
            elif dayOfWeek==2:
                wedCount = wedCount + 1
            elif dayOfWeek==3:
                thuCount = thuCount + 1
            elif dayOfWeek==4:
                friCount = friCount + 1
            elif dayOfWeek==5:
                satCount = satCount + 1
            elif dayOfWeek==6:
                sunCount = sunCount + 1
        except:
            print("error thrown for step 14")
        
        try:
            #15: keep count of news items per weekend vs. weekday
            date = parse(row["TIMESTAMP_UTC"])
            dayOfWeek = date.weekday()
            if dayOfWeek >= 0 and dayOfWeek <= 4:
                weekdayCount = weekdayCount + 1
            elif dayOfWeek > 4 and dayOfWeek <= 6:
                weekendCount = weekendCount + 1
        except:
            print("error thrown for step 15")
            
        try:
            #16: keep count of news items before/after 3:30 pm
            date = parse(row["TIMESTAMP_UTC"])
            timeOfDay = date.time()
            dayOfWeek = date.weekday()
            if dayOfWeek >= 0 and dayOfWeek <= 4:
                if timeOfDay < time1:
                    before330Count = before330Count + 1
                elif timeOfDay > time1:
                    after330Count = after330Count + 1
        except:
            print("error thrown for step 16")
            '''
    print("finishing chunk",chunkCount)
print("done with main for loop")

#task 2 final processing
meanESS = float(meanESS)/totalESSItems
meanAES = float(meanAES)/totalItems
meanRELEVANCE = float(meanRELEVANCE)/totalItems
meanENS = float(meanENS)/totalESSItems
meanENS_SIMILARITY_GAP = float(meanENS_SIMILARITY_GAP)/totalESSItems
meanENS_ELAPSED = float(meanENS_ELAPSED)/totalESSItems
meanG_ENS = float(meanG_ENS)/totalESSItems
meanG_ENS_SIMILARITY_GAP = float(meanG_ENS_SIMILARITY_GAP)/totalESSItems
meanG_ENS_ELAPSED = float(meanG_ENS_ELAPSED)/totalESSItems

#task 3 final processing
stockDF = pd.DataFrame.from_dict(itemsPerStock,orient="index") #convert dictionary to pandas DataFrame
stockDF.reset_index(inplace=True)
stockDF.columns = ["Stock ID","News Items"] #add headers
stockDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/items_per_stock.csv") #save to csv file
#task 4 final processing
countryDF = pd.DataFrame.from_dict(itemsPerCountry,orient="index") #convert dictionary to pandas DataFrame
countryDF.reset_index(inplace=True)
countryDF.columns = ["Country ID","News Items"] #add headers
countryDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/items_per_country.csv") #save to csv file
#task 5 final processing
topicDF = pd.DataFrame.from_dict(itemsPerTopic,orient="index") #convert dictionary to pandas DataFrame
topicDF.reset_index(inplace=True)
topicDF.columns = ["Topic","News Items"] #add headers
topicDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/items_per_topic.csv") #save to csv file
groupDF = pd.DataFrame.from_dict(itemsPerGroup,orient="index") #convert dictionary to pandas DataFrame
groupDF.reset_index(inplace=True)
groupDF.columns = ["Group","News Items"] #add headers
groupDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/items_per_group.csv") #save to csv file
typeDF = pd.DataFrame.from_dict(itemsPerType,orient="index") #convert dictionary to pandas DataFrame
typeDF.reset_index(inplace=True)
typeDF.columns = ["Type","News Items"] #add headers
typeDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/items_per_type.csv") #save to csv file
categoryDF = pd.DataFrame.from_dict(itemsPerCategory,orient="index") #convert dictionary to pandas DataFrame
categoryDF.reset_index(inplace=True)
categoryDF.columns = ["Category","News Items"] #add headers
categoryDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/items_per_category.csv") #save to csv file
newsTypeDF = pd.DataFrame.from_dict(itemsPerNewsType,orient="index") #convert dictionary to pandas DataFrame
newsTypeDF.reset_index(inplace=True)
newsTypeDF.columns = ["News Type","News Items"] #add headers
newsTypeDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/items_per_news_type.csv") #save to csv file

#task 6/7 final processing
for stockID in meanESSPerStock:
    meanESSPerStock[stockID] = float(meanESSPerStock[stockID]) / ESSItemsPerStock[stockID] #calculates mean ESS per stock
meanESSDF = pd.DataFrame.from_dict(meanESSPerStock,orient="index") #convert dictionary to pandas DataFrame
meanESSDF.reset_index(inplace=True)
meanESSDF.columns = ["Stock ID","Mean ESS"] #add headers
meanESSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ESS_per_stock.csv") #save to csv file
for stockID in meanAESPerStock:
    meanAESPerStock[stockID] = float(meanAESPerStock[stockID]) / itemsPerStock[stockID] #calculates mean AES per stock
meanAESDF = pd.DataFrame.from_dict(meanAESPerStock,orient="index") #convert dictionary to pandas DataFrame
meanAESDF.reset_index(inplace=True)
meanAESDF.columns = ["Stock ID","Mean AES"] #add headers
meanAESDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_AES_per_stock.csv") #save to csv file
for stockID in meanRELEVANCEPerStock:
    meanRELEVANCEPerStock[stockID] = float(meanRELEVANCEPerStock[stockID]) / itemsPerStock[stockID] #calc mean RELEVANCE per stock
meanRELEVANCEDF = pd.DataFrame.from_dict(meanRELEVANCEPerStock,orient="index") #convert dictionary to pandas DataFrame
meanRELEVANCEDF.reset_index(inplace=True)
meanRELEVANCEDF.columns = ["Stock ID","Mean Relevance"] #add headers
meanRELEVANCEDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_RELEVANCE_per_stock.csv") #save to csv file
for stockID in meanENSPerStock:
    meanENSPerStock[stockID] = float(meanENSPerStock[stockID]) / ESSItemsPerStock[stockID] #calc mean ENS per stock
meanENSDF = pd.DataFrame.from_dict(meanENSPerStock,orient="index") #convert dictionary to pandas DataFrame
meanENSDF.reset_index(inplace=True)
meanENSDF.columns = ["Stock ID","Mean ENS"] #add headers
meanENSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_per_stock.csv") #save to csv file
for stockID in meanENS_SIMILARITY_GAPPerStock:
    meanENS_SIMILARITY_GAPPerStock[stockID] = float(meanENS_SIMILARITY_GAPPerStock[stockID]) / ESSItemsPerStock[stockID] #calc mean ENS_SIMILARITY_GAP per stock
meanENS_SIMILARITY_GAPDF = pd.DataFrame.from_dict(meanENS_SIMILARITY_GAPPerStock,orient="index") #convert dictionary to pandas DataFrame
meanENS_SIMILARITY_GAPDF.reset_index(inplace=True)
meanENS_SIMILARITY_GAPDF.columns = ["Stock ID","Mean ENS_SIMILARITY_GAP"] #add headers
meanENS_SIMILARITY_GAPDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_SIMILARITY_GAP_per_stock.csv") #save to csv file
for stockID in meanENS_ELAPSEDPerStock:
    meanENS_ELAPSEDPerStock[stockID] = float(meanENS_ELAPSEDPerStock[stockID]) / ESSItemsPerStock[stockID] #calc mean ENS_ELAPSED per stock
meanENS_ELAPSEDDF = pd.DataFrame.from_dict(meanENS_ELAPSEDPerStock,orient="index") #convert dictionary to pandas DataFrame
meanENS_ELAPSEDDF.reset_index(inplace=True)
meanENS_ELAPSEDDF.columns = ["Stock ID","Mean ENS_ELAPSED"] #add headers
meanENS_ELAPSEDDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_ELAPSED_per_stock.csv") #save to csv file
for stockID in meanG_ENSPerStock:
    meanG_ENSPerStock[stockID] = float(meanG_ENSPerStock[stockID]) / ESSItemsPerStock[stockID] #calc mean G_ENS per stock
meanG_ENSDF = pd.DataFrame.from_dict(meanG_ENSPerStock,orient="index") #convert dictionary to pandas DataFrame
meanG_ENSDF.reset_index(inplace=True)
meanG_ENSDF.columns = ["Stock ID","Mean G_ENS"] #add headers
meanG_ENSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_per_stock.csv") #save to csv file
for stockID in meanG_ENS_SIMILARITY_GAPPerStock:
    meanG_ENS_SIMILARITY_GAPPerStock[stockID] = float(meanG_ENS_SIMILARITY_GAPPerStock[stockID]) / ESSItemsPerStock[stockID] #calc mean G_ENS_SIMILARITY_GAP per stock
meanG_ENS_SIMILARITY_GAPDF = pd.DataFrame.from_dict(meanG_ENS_SIMILARITY_GAPPerStock,orient="index") #convert dictionary to pandas DataFrame
meanG_ENS_SIMILARITY_GAPDF.reset_index(inplace=True)
meanG_ENS_SIMILARITY_GAPDF.columns = ["Stock ID","Mean G_ENS_SIMILARITY_GAP"] #add headers
meanG_ENS_SIMILARITY_GAPDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_SIMILARITY_GAP_per_stock.csv") #save to csv file
for stockID in meanG_ENS_ELAPSEDPerStock:
    meanG_ENS_ELAPSEDPerStock[stockID] = float(meanG_ENS_ELAPSEDPerStock[stockID]) / ESSItemsPerStock[stockID] #calc mean G_ENS_ELAPSED per stock
meanG_ENS_ELAPSEDDF = pd.DataFrame.from_dict(meanG_ENS_ELAPSEDPerStock,orient="index") #convert dictionary to pandas DataFrame
meanG_ENS_ELAPSEDDF.reset_index(inplace=True)
meanG_ENS_ELAPSEDDF.columns = ["Stock ID","Mean G_ENS_ELAPSED"] #add headers
meanG_ENS_ELAPSEDDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_ELAPSED_per_stock.csv") #save to csv file
#task 8 final processing
for countryID in meanESSPerCountry:
    meanESSPerCountry[countryID] = float(meanESSPerCountry[countryID]) / ESSItemsPerCountry[countryID] #calculates mean ESS per country
meanESSDF = pd.DataFrame.from_dict(meanESSPerCountry,orient="index") #convert dictionary to pandas DataFrame
meanESSDF.reset_index(inplace=True)
meanESSDF.columns = ["Country ID","Mean ESS"] #add headers
meanESSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ESS_per_country.csv") #save to csv file
for countryID in meanAESPerCountry:
    meanAESPerCountry[countryID] = float(meanAESPerCountry[countryID]) / itemsPerCountry[countryID] #calculates mean AES per country
meanAESDF = pd.DataFrame.from_dict(meanAESPerCountry,orient="index") #convert dictionary to pandas DataFrame
meanAESDF.reset_index(inplace=True)
meanAESDF.columns = ["Country ID","Mean AES"] #add headers
meanAESDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_AES_per_country.csv") #save to csv file
for countryID in meanRELEVANCEPerCountry:
    meanRELEVANCEPerCountry[countryID] = float(meanRELEVANCEPerCountry[countryID]) / itemsPerCountry[countryID] #calc mean RELEVANCE per country
meanRELEVANCEDF = pd.DataFrame.from_dict(meanRELEVANCEPerCountry,orient="index") #convert dictionary to pandas DataFrame
meanRELEVANCEDF.reset_index(inplace=True)
meanRELEVANCEDF.columns = ["Country ID","Mean Relevance"] #add headers
meanRELEVANCEDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_RELEVANCE_per_country.csv") #save to csv file
for countryID in meanENSPerCountry:
    meanENSPerCountry[countryID] = float(meanENSPerCountry[countryID]) / ESSItemsPerCountry[countryID] #calc mean ENS per country
meanENSDF = pd.DataFrame.from_dict(meanENSPerCountry,orient="index") #convert dictionary to pandas DataFrame
meanENSDF.reset_index(inplace=True)
meanENSDF.columns = ["Country ID","Mean ENS"] #add headers
meanENSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_per_country.csv") #save to csv file
for countryID in meanENS_SIMILARITY_GAPPerCountry:
    meanENS_SIMILARITY_GAPPerCountry[countryID] = float(meanENS_SIMILARITY_GAPPerCountry[countryID]) / ESSItemsPerCountry[countryID] #calc mean ENS_SIMILARITY_GAP per country
meanENS_SIMILARITY_GAPDF = pd.DataFrame.from_dict(meanENS_SIMILARITY_GAPPerCountry,orient="index") #convert dictionary to pandas DataFrame
meanENS_SIMILARITY_GAPDF.reset_index(inplace=True)
meanENS_SIMILARITY_GAPDF.columns = ["Country ID","Mean ENS_SIMILARITY_GAP"] #add headers
meanENS_SIMILARITY_GAPDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_SIMILARITY_GAP_per_country.csv") #save to csv file
for countryID in meanENS_ELAPSEDPerCountry:
    meanENS_ELAPSEDPerCountry[countryID] = float(meanENS_ELAPSEDPerCountry[countryID]) / ESSItemsPerCountry[countryID] #calc mean ENS_ELAPSED per country
meanENS_ELAPSEDDF = pd.DataFrame.from_dict(meanENS_ELAPSEDPerCountry,orient="index") #convert dictionary to pandas DataFrame
meanENS_ELAPSEDDF.reset_index(inplace=True)
meanENS_ELAPSEDDF.columns = ["Country ID","Mean ENS_ELAPSED"] #add headers
meanENS_ELAPSEDDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_ELAPSED_per_country.csv") #save to csv file
for countryID in meanG_ENSPerCountry:
    meanG_ENSPerCountry[countryID] = float(meanG_ENSPerCountry[countryID]) / ESSItemsPerCountry[countryID] #calc mean G_ENS per country
meanG_ENSDF = pd.DataFrame.from_dict(meanG_ENSPerCountry,orient="index") #convert dictionary to pandas DataFrame
meanG_ENSDF.reset_index(inplace=True)
meanG_ENSDF.columns = ["Country ID","Mean G_ENS"] #add headers
meanG_ENSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_per_country.csv") #save to csv file
for countryID in meanG_ENS_SIMILARITY_GAPPerCountry:
    meanG_ENS_SIMILARITY_GAPPerCountry[countryID] = float(meanG_ENS_SIMILARITY_GAPPerCountry[countryID]) / ESSItemsPerCountry[countryID] #calc mean G_ENS_SIMILARITY_GAP per country
meanG_ENS_SIMILARITY_GAPDF = pd.DataFrame.from_dict(meanG_ENS_SIMILARITY_GAPPerCountry,orient="index") #convert dictionary to pandas DataFrame
meanG_ENS_SIMILARITY_GAPDF.reset_index(inplace=True)
meanG_ENS_SIMILARITY_GAPDF.columns = ["Country ID","Mean G_ENS_SIMILARITY_GAP"] #add headers
meanG_ENS_SIMILARITY_GAPDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_SIMILARITY_GAP_per_country.csv") #save to csv file
for countryID in meanG_ENS_ELAPSEDPerCountry:
    meanG_ENS_ELAPSEDPerCountry[countryID] = float(meanG_ENS_ELAPSEDPerCountry[countryID]) / ESSItemsPerCountry[countryID] #calc mean G_ENS_ELAPSED per country
meanG_ENS_ELAPSEDDF = pd.DataFrame.from_dict(meanG_ENS_ELAPSEDPerCountry,orient="index") #convert dictionary to pandas DataFrame
meanG_ENS_ELAPSEDDF.reset_index(inplace=True)
meanG_ENS_ELAPSEDDF.columns = ["Country ID","Mean G_ENS_ELAPSED"] #add headers
meanG_ENS_ELAPSEDDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_ELAPSED_per_country.csv") #save to csv file
#task 9 final processing: topic
for topicID in meanESSPerTopic:
    meanESSPerTopic[topicID] = float(meanESSPerTopic[topicID]) / ESSItemsPerTopic[topicID] #calculates mean ESS per topic
meanESSDF = pd.DataFrame.from_dict(meanESSPerTopic,orient="index") #convert dictionary to pandas DataFrame
meanESSDF.reset_index(inplace=True)
meanESSDF.columns = ["Topic ID","Mean ESS"] #add headers
meanESSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ESS_per_topic.csv") #save to csv file
for topicID in meanAESPerTopic:
    meanAESPerTopic[topicID] = float(meanAESPerTopic[topicID]) / itemsPerTopic[topicID] #calculates mean AES per topic
meanAESDF = pd.DataFrame.from_dict(meanAESPerTopic,orient="index") #convert dictionary to pandas DataFrame
meanAESDF.reset_index(inplace=True)
meanAESDF.columns = ["Topic ID","Mean AES"] #add headers
meanAESDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_AES_per_topic.csv") #save to csv file
for topicID in meanRELEVANCEPerTopic:
    meanRELEVANCEPerTopic[topicID] = float(meanRELEVANCEPerTopic[topicID]) / itemsPerTopic[topicID] #calc mean RELEVANCE per topic
meanRELEVANCEDF = pd.DataFrame.from_dict(meanRELEVANCEPerTopic,orient="index") #convert dictionary to pandas DataFrame
meanRELEVANCEDF.reset_index(inplace=True)
meanRELEVANCEDF.columns = ["Topic ID","Mean Relevance"] #add headers
meanRELEVANCEDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_RELEVANCE_per_topic.csv") #save to csv file
for topicID in meanENSPerTopic:
    meanENSPerTopic[topicID] = float(meanENSPerTopic[topicID]) / ESSItemsPerTopic[topicID] #calc mean ENS per topic
meanENSDF = pd.DataFrame.from_dict(meanENSPerTopic,orient="index") #convert dictionary to pandas DataFrame
meanENSDF.reset_index(inplace=True)
meanENSDF.columns = ["Topic ID","Mean ENS"] #add headers
meanENSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_per_topic.csv") #save to csv file
for topicID in meanENS_SIMILARITY_GAPPerTopic:
    meanENS_SIMILARITY_GAPPerTopic[topicID] = float(meanENS_SIMILARITY_GAPPerTopic[topicID]) / ESSItemsPerTopic[topicID] #calc mean ENS_SIMILARITY_GAP per topic
meanENS_SIMILARITY_GAPDF = pd.DataFrame.from_dict(meanENS_SIMILARITY_GAPPerTopic,orient="index") #convert dictionary to pandas DataFrame
meanENS_SIMILARITY_GAPDF.reset_index(inplace=True)
meanENS_SIMILARITY_GAPDF.columns = ["Topic ID","Mean ENS_SIMILARITY_GAP"] #add headers
meanENS_SIMILARITY_GAPDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_SIMILARITY_GAP_per_topic.csv") #save to csv file
for topicID in meanENS_ELAPSEDPerTopic:
    meanENS_ELAPSEDPerTopic[topicID] = float(meanENS_ELAPSEDPerTopic[topicID]) / ESSItemsPerTopic[topicID] #calc mean ENS_ELAPSED per topic
meanENS_ELAPSEDDF = pd.DataFrame.from_dict(meanENS_ELAPSEDPerTopic,orient="index") #convert dictionary to pandas DataFrame
meanENS_ELAPSEDDF.reset_index(inplace=True)
meanENS_ELAPSEDDF.columns = ["Topic ID","Mean ENS_ELAPSED"] #add headers
meanENS_ELAPSEDDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_ELAPSED_per_topic.csv") #save to csv file
for topicID in meanG_ENSPerTopic:
    meanG_ENSPerTopic[topicID] = float(meanG_ENSPerTopic[topicID]) / ESSItemsPerTopic[topicID] #calc mean G_ENS per topic
meanG_ENSDF = pd.DataFrame.from_dict(meanG_ENSPerTopic,orient="index") #convert dictionary to pandas DataFrame
meanG_ENSDF.reset_index(inplace=True)
meanG_ENSDF.columns = ["Topic ID","Mean G_ENS"] #add headers
meanG_ENSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_per_topic.csv") #save to csv file
for topicID in meanG_ENS_SIMILARITY_GAPPerTopic:
    meanG_ENS_SIMILARITY_GAPPerTopic[topicID] = float(meanG_ENS_SIMILARITY_GAPPerTopic[topicID]) / ESSItemsPerTopic[topicID] #calc mean G_ENS_SIMILARITY_GAP per topic
meanG_ENS_SIMILARITY_GAPDF = pd.DataFrame.from_dict(meanG_ENS_SIMILARITY_GAPPerTopic,orient="index") #convert dictionary to pandas DataFrame
meanG_ENS_SIMILARITY_GAPDF.reset_index(inplace=True)
meanG_ENS_SIMILARITY_GAPDF.columns = ["Topic ID","Mean G_ENS_SIMILARITY_GAP"] #add headers
meanG_ENS_SIMILARITY_GAPDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_SIMILARITY_GAP_per_topic.csv") #save to csv file
for topicID in meanG_ENS_ELAPSEDPerTopic:
    meanG_ENS_ELAPSEDPerTopic[topicID] = float(meanG_ENS_ELAPSEDPerTopic[topicID]) / ESSItemsPerTopic[topicID] #calc mean G_ENS_ELAPSED per topic
meanG_ENS_ELAPSEDDF = pd.DataFrame.from_dict(meanG_ENS_ELAPSEDPerTopic,orient="index") #convert dictionary to pandas DataFrame
meanG_ENS_ELAPSEDDF.reset_index(inplace=True)
meanG_ENS_ELAPSEDDF.columns = ["Topic ID","Mean G_ENS_ELAPSED"] #add headers
meanG_ENS_ELAPSEDDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_ELAPSED_per_topic.csv") #save to csv file
#task 9 final processing: group
for groupID in meanESSPerGroup:
    meanESSPerGroup[groupID] = float(meanESSPerGroup[groupID]) / ESSItemsPerGroup[groupID] #calculates mean ESS per group
meanESSDF = pd.DataFrame.from_dict(meanESSPerGroup,orient="index") #convert dictionary to pandas DataFrame
meanESSDF.reset_index(inplace=True)
meanESSDF.columns = ["Group ID","Mean ESS"] #add headers
meanESSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ESS_per_group.csv") #save to csv file
for groupID in meanAESPerGroup:
    meanAESPerGroup[groupID] = float(meanAESPerGroup[groupID]) / itemsPerGroup[groupID] #calculates mean AES per group
meanAESDF = pd.DataFrame.from_dict(meanAESPerGroup,orient="index") #convert dictionary to pandas DataFrame
meanAESDF.reset_index(inplace=True)
meanAESDF.columns = ["Group ID","Mean AES"] #add headers
meanAESDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_AES_per_group.csv") #save to csv file
for groupID in meanRELEVANCEPerGroup:
    meanRELEVANCEPerGroup[groupID] = float(meanRELEVANCEPerGroup[groupID]) / itemsPerGroup[groupID] #calc mean RELEVANCE per group
meanRELEVANCEDF = pd.DataFrame.from_dict(meanRELEVANCEPerGroup,orient="index") #convert dictionary to pandas DataFrame
meanRELEVANCEDF.reset_index(inplace=True)
meanRELEVANCEDF.columns = ["Group ID","Mean Relevance"] #add headers
meanRELEVANCEDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_RELEVANCE_per_group.csv") #save to csv file
for groupID in meanENSPerGroup:
    meanENSPerGroup[groupID] = float(meanENSPerGroup[groupID]) / ESSItemsPerGroup[groupID] #calc mean ENS per group
meanENSDF = pd.DataFrame.from_dict(meanENSPerGroup,orient="index") #convert dictionary to pandas DataFrame
meanENSDF.reset_index(inplace=True)
meanENSDF.columns = ["Group ID","Mean ENS"] #add headers
meanENSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_per_group.csv") #save to csv file
for groupID in meanENS_SIMILARITY_GAPPerGroup:
    meanENS_SIMILARITY_GAPPerGroup[groupID] = float(meanENS_SIMILARITY_GAPPerGroup[groupID]) / ESSItemsPerGroup[groupID] #calc mean ENS_SIMILARITY_GAP per group
meanENS_SIMILARITY_GAPDF = pd.DataFrame.from_dict(meanENS_SIMILARITY_GAPPerGroup,orient="index") #convert dictionary to pandas DataFrame
meanENS_SIMILARITY_GAPDF.reset_index(inplace=True)
meanENS_SIMILARITY_GAPDF.columns = ["Group ID","Mean ENS_SIMILARITY_GAP"] #add headers
meanENS_SIMILARITY_GAPDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_SIMILARITY_GAP_per_group.csv") #save to csv file
for groupID in meanENS_ELAPSEDPerGroup:
    meanENS_ELAPSEDPerGroup[groupID] = float(meanENS_ELAPSEDPerGroup[groupID]) / ESSItemsPerGroup[groupID] #calc mean ENS_ELAPSED per group
meanENS_ELAPSEDDF = pd.DataFrame.from_dict(meanENS_ELAPSEDPerGroup,orient="index") #convert dictionary to pandas DataFrame
meanENS_ELAPSEDDF.reset_index(inplace=True)
meanENS_ELAPSEDDF.columns = ["Group ID","Mean ENS_ELAPSED"] #add headers
meanENS_ELAPSEDDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_ELAPSED_per_group.csv") #save to csv file
for groupID in meanG_ENSPerGroup:
    meanG_ENSPerGroup[groupID] = float(meanG_ENSPerGroup[groupID]) / ESSItemsPerGroup[groupID] #calc mean G_ENS per group
meanG_ENSDF = pd.DataFrame.from_dict(meanG_ENSPerGroup,orient="index") #convert dictionary to pandas DataFrame
meanG_ENSDF.reset_index(inplace=True)
meanG_ENSDF.columns = ["Group ID","Mean G_ENS"] #add headers
meanG_ENSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_per_group.csv") #save to csv file
for groupID in meanG_ENS_SIMILARITY_GAPPerGroup:
    meanG_ENS_SIMILARITY_GAPPerGroup[groupID] = float(meanG_ENS_SIMILARITY_GAPPerGroup[groupID]) / ESSItemsPerGroup[groupID] #calc mean G_ENS_SIMILARITY_GAP per group
meanG_ENS_SIMILARITY_GAPDF = pd.DataFrame.from_dict(meanG_ENS_SIMILARITY_GAPPerGroup,orient="index") #convert dictionary to pandas DataFrame
meanG_ENS_SIMILARITY_GAPDF.reset_index(inplace=True)
meanG_ENS_SIMILARITY_GAPDF.columns = ["Group ID","Mean G_ENS_SIMILARITY_GAP"] #add headers
meanG_ENS_SIMILARITY_GAPDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_SIMILARITY_GAP_per_group.csv") #save to csv file
for groupID in meanG_ENS_ELAPSEDPerGroup:
    meanG_ENS_ELAPSEDPerGroup[groupID] = float(meanG_ENS_ELAPSEDPerGroup[groupID]) / ESSItemsPerGroup[groupID] #calc mean G_ENS_ELAPSED per group
meanG_ENS_ELAPSEDDF = pd.DataFrame.from_dict(meanG_ENS_ELAPSEDPerGroup,orient="index") #convert dictionary to pandas DataFrame
meanG_ENS_ELAPSEDDF.reset_index(inplace=True)
meanG_ENS_ELAPSEDDF.columns = ["Group ID","Mean G_ENS_ELAPSED"] #add headers
meanG_ENS_ELAPSEDDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_ELAPSED_per_group.csv") #save to csv file
#task 9 final processing: type
for typeID in meanESSPerType:
    meanESSPerType[typeID] = float(meanESSPerType[typeID]) / ESSItemsPerType[typeID] #calculates mean ESS per type
meanESSDF = pd.DataFrame.from_dict(meanESSPerType,orient="index") #convert dictionary to pandas DataFrame
meanESSDF.reset_index(inplace=True)
meanESSDF.columns = ["Type ID","Mean ESS"] #add headers
meanESSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ESS_per_type.csv") #save to csv file
for typeID in meanAESPerType:
    meanAESPerType[typeID] = float(meanAESPerType[typeID]) / itemsPerType[typeID] #calculates mean AES per type
meanAESDF = pd.DataFrame.from_dict(meanAESPerType,orient="index") #convert dictionary to pandas DataFrame
meanAESDF.reset_index(inplace=True)
meanAESDF.columns = ["Type ID","Mean AES"] #add headers
meanAESDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_AES_per_type.csv") #save to csv file
for typeID in meanRELEVANCEPerType:
    meanRELEVANCEPerType[typeID] = float(meanRELEVANCEPerType[typeID]) / itemsPerType[typeID] #calc mean RELEVANCE per type
meanRELEVANCEDF = pd.DataFrame.from_dict(meanRELEVANCEPerType,orient="index") #convert dictionary to pandas DataFrame
meanRELEVANCEDF.reset_index(inplace=True)
meanRELEVANCEDF.columns = ["Type ID","Mean Relevance"] #add headers
meanRELEVANCEDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_RELEVANCE_per_type.csv") #save to csv file
for typeID in meanENSPerType:
    meanENSPerType[typeID] = float(meanENSPerType[typeID]) / ESSItemsPerType[typeID] #calc mean ENS per type
meanENSDF = pd.DataFrame.from_dict(meanENSPerType,orient="index") #convert dictionary to pandas DataFrame
meanENSDF.reset_index(inplace=True)
meanENSDF.columns = ["Type ID","Mean ENS"] #add headers
meanENSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_per_type.csv") #save to csv file
for typeID in meanENS_SIMILARITY_GAPPerType:
    meanENS_SIMILARITY_GAPPerType[typeID] = float(meanENS_SIMILARITY_GAPPerType[typeID]) / ESSItemsPerType[typeID] #calc mean ENS_SIMILARITY_GAP per type
meanENS_SIMILARITY_GAPDF = pd.DataFrame.from_dict(meanENS_SIMILARITY_GAPPerType,orient="index") #convert dictionary to pandas DataFrame
meanENS_SIMILARITY_GAPDF.reset_index(inplace=True)
meanENS_SIMILARITY_GAPDF.columns = ["Type ID","Mean ENS_SIMILARITY_GAP"] #add headers
meanENS_SIMILARITY_GAPDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_SIMILARITY_GAP_per_type.csv") #save to csv file
for typeID in meanENS_ELAPSEDPerType:
    meanENS_ELAPSEDPerType[typeID] = float(meanENS_ELAPSEDPerType[typeID]) / ESSItemsPerType[typeID] #calc mean ENS_ELAPSED per type
meanENS_ELAPSEDDF = pd.DataFrame.from_dict(meanENS_ELAPSEDPerType,orient="index") #convert dictionary to pandas DataFrame
meanENS_ELAPSEDDF.reset_index(inplace=True)
meanENS_ELAPSEDDF.columns = ["Type ID","Mean ENS_ELAPSED"] #add headers
meanENS_ELAPSEDDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_ELAPSED_per_type.csv") #save to csv file
for typeID in meanG_ENSPerType:
    meanG_ENSPerType[typeID] = float(meanG_ENSPerType[typeID]) / ESSItemsPerType[typeID] #calc mean G_ENS per type
meanG_ENSDF = pd.DataFrame.from_dict(meanG_ENSPerType,orient="index") #convert dictionary to pandas DataFrame
meanG_ENSDF.reset_index(inplace=True)
meanG_ENSDF.columns = ["Type ID","Mean G_ENS"] #add headers
meanG_ENSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_per_type.csv") #save to csv file
for typeID in meanG_ENS_SIMILARITY_GAPPerType:
    meanG_ENS_SIMILARITY_GAPPerType[typeID] = float(meanG_ENS_SIMILARITY_GAPPerType[typeID]) / ESSItemsPerType[typeID] #calc mean G_ENS_SIMILARITY_GAP per type
meanG_ENS_SIMILARITY_GAPDF = pd.DataFrame.from_dict(meanG_ENS_SIMILARITY_GAPPerType,orient="index") #convert dictionary to pandas DataFrame
meanG_ENS_SIMILARITY_GAPDF.reset_index(inplace=True)
meanG_ENS_SIMILARITY_GAPDF.columns = ["Type ID","Mean G_ENS_SIMILARITY_GAP"] #add headers
meanG_ENS_SIMILARITY_GAPDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_SIMILARITY_GAP_per_type.csv") #save to csv file
for typeID in meanG_ENS_ELAPSEDPerType:
    meanG_ENS_ELAPSEDPerType[typeID] = float(meanG_ENS_ELAPSEDPerType[typeID]) / ESSItemsPerType[typeID] #calc mean G_ENS_ELAPSED per type
meanG_ENS_ELAPSEDDF = pd.DataFrame.from_dict(meanG_ENS_ELAPSEDPerType,orient="index") #convert dictionary to pandas DataFrame
meanG_ENS_ELAPSEDDF.reset_index(inplace=True)
meanG_ENS_ELAPSEDDF.columns = ["Type ID","Mean G_ENS_ELAPSED"] #add headers
meanG_ENS_ELAPSEDDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_ELAPSED_per_type.csv") #save to csv file
#task 9 final processing: category
for categoryID in meanESSPerCategory:
    meanESSPerCategory[categoryID] = float(meanESSPerCategory[categoryID]) / ESSItemsPerCategory[categoryID] #calculates mean ESS per category
meanESSDF = pd.DataFrame.from_dict(meanESSPerCategory,orient="index") #convert dictionary to pandas DataFrame
meanESSDF.reset_index(inplace=True)
meanESSDF.columns = ["Category ID","Mean ESS"] #add headers
meanESSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ESS_per_category.csv") #save to csv file
for categoryID in meanAESPerCategory:
    meanAESPerCategory[categoryID] = float(meanAESPerCategory[categoryID]) / itemsPerCategory[categoryID] #calculates mean AES per category
meanAESDF = pd.DataFrame.from_dict(meanAESPerCategory,orient="index") #convert dictionary to pandas DataFrame
meanAESDF.reset_index(inplace=True)
meanAESDF.columns = ["Category ID","Mean AES"] #add headers
meanAESDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_AES_per_category.csv") #save to csv file
for categoryID in meanRELEVANCEPerCategory:
    meanRELEVANCEPerCategory[categoryID] = float(meanRELEVANCEPerCategory[categoryID]) / itemsPerCategory[categoryID] #calc mean RELEVANCE per category
meanRELEVANCEDF = pd.DataFrame.from_dict(meanRELEVANCEPerCategory,orient="index") #convert dictionary to pandas DataFrame
meanRELEVANCEDF.reset_index(inplace=True)
meanRELEVANCEDF.columns = ["Category ID","Mean Relevance"] #add headers
meanRELEVANCEDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_RELEVANCE_per_category.csv") #save to csv file
for categoryID in meanENSPerCategory:
    meanENSPerCategory[categoryID] = float(meanENSPerCategory[categoryID]) / ESSItemsPerCategory[categoryID] #calc mean ENS per category
meanENSDF = pd.DataFrame.from_dict(meanENSPerCategory,orient="index") #convert dictionary to pandas DataFrame
meanENSDF.reset_index(inplace=True)
meanENSDF.columns = ["Category ID","Mean ENS"] #add headers
meanENSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_per_category.csv") #save to csv file
for categoryID in meanENS_SIMILARITY_GAPPerCategory:
    meanENS_SIMILARITY_GAPPerCategory[categoryID] = float(meanENS_SIMILARITY_GAPPerCategory[categoryID]) / ESSItemsPerCategory[categoryID] #calc mean ENS_SIMILARITY_GAP per category
meanENS_SIMILARITY_GAPDF = pd.DataFrame.from_dict(meanENS_SIMILARITY_GAPPerCategory,orient="index") #convert dictionary to pandas DataFrame
meanENS_SIMILARITY_GAPDF.reset_index(inplace=True)
meanENS_SIMILARITY_GAPDF.columns = ["Category ID","Mean ENS_SIMILARITY_GAP"] #add headers
meanENS_SIMILARITY_GAPDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_SIMILARITY_GAP_per_category.csv") #save to csv file
for categoryID in meanENS_ELAPSEDPerCategory:
    meanENS_ELAPSEDPerCategory[categoryID] = float(meanENS_ELAPSEDPerCategory[categoryID]) / ESSItemsPerCategory[categoryID] #calc mean ENS_ELAPSED per category
meanENS_ELAPSEDDF = pd.DataFrame.from_dict(meanENS_ELAPSEDPerCategory,orient="index") #convert dictionary to pandas DataFrame
meanENS_ELAPSEDDF.reset_index(inplace=True)
meanENS_ELAPSEDDF.columns = ["Category ID","Mean ENS_ELAPSED"] #add headers
meanENS_ELAPSEDDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_ELAPSED_per_category.csv") #save to csv file
for categoryID in meanG_ENSPerCategory:
    meanG_ENSPerCategory[categoryID] = float(meanG_ENSPerCategory[categoryID]) / ESSItemsPerCategory[categoryID] #calc mean G_ENS per category
meanG_ENSDF = pd.DataFrame.from_dict(meanG_ENSPerCategory,orient="index") #convert dictionary to pandas DataFrame
meanG_ENSDF.reset_index(inplace=True)
meanG_ENSDF.columns = ["Category ID","Mean G_ENS"] #add headers
meanG_ENSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_per_category.csv") #save to csv file
for categoryID in meanG_ENS_SIMILARITY_GAPPerCategory:
    meanG_ENS_SIMILARITY_GAPPerCategory[categoryID] = float(meanG_ENS_SIMILARITY_GAPPerCategory[categoryID]) / ESSItemsPerCategory[categoryID] #calc mean G_ENS_SIMILARITY_GAP per category
meanG_ENS_SIMILARITY_GAPDF = pd.DataFrame.from_dict(meanG_ENS_SIMILARITY_GAPPerCategory,orient="index") #convert dictionary to pandas DataFrame
meanG_ENS_SIMILARITY_GAPDF.reset_index(inplace=True)
meanG_ENS_SIMILARITY_GAPDF.columns = ["Category ID","Mean G_ENS_SIMILARITY_GAP"] #add headers
meanG_ENS_SIMILARITY_GAPDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_SIMILARITY_GAP_per_category.csv") #save to csv file
for categoryID in meanG_ENS_ELAPSEDPerCategory:
    meanG_ENS_ELAPSEDPerCategory[categoryID] = float(meanG_ENS_ELAPSEDPerCategory[categoryID]) / ESSItemsPerCategory[categoryID] #calc mean G_ENS_ELAPSED per category
meanG_ENS_ELAPSEDDF = pd.DataFrame.from_dict(meanG_ENS_ELAPSEDPerCategory,orient="index") #convert dictionary to pandas DataFrame
meanG_ENS_ELAPSEDDF.reset_index(inplace=True)
meanG_ENS_ELAPSEDDF.columns = ["Category ID","Mean G_ENS_ELAPSED"] #add headers
meanG_ENS_ELAPSEDDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_ELAPSED_per_category.csv") #save to csv file
#task 9 final processing: news type
for news_typeID in meanESSPerNewsType:
    meanESSPerNewsType[news_typeID] = float(meanESSPerNewsType[news_typeID]) / ESSItemsPerNewsType[news_typeID] #calculates mean ESS per news_type
meanESSDF = pd.DataFrame.from_dict(meanESSPerNewsType,orient="index") #convert dictionary to pandas DataFrame
meanESSDF.reset_index(inplace=True)
meanESSDF.columns = ["NewsType ID","Mean ESS"] #add headers
meanESSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ESS_per_news_type.csv") #save to csv file
for news_typeID in meanAESPerNewsType:
    meanAESPerNewsType[news_typeID] = float(meanAESPerNewsType[news_typeID]) / itemsPerNewsType[news_typeID] #calculates mean AES per news_type
meanAESDF = pd.DataFrame.from_dict(meanAESPerNewsType,orient="index") #convert dictionary to pandas DataFrame
meanAESDF.reset_index(inplace=True)
meanAESDF.columns = ["NewsType ID","Mean AES"] #add headers
meanAESDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_AES_per_news_type.csv") #save to csv file
for news_typeID in meanRELEVANCEPerNewsType:
    meanRELEVANCEPerNewsType[news_typeID] = float(meanRELEVANCEPerNewsType[news_typeID]) / itemsPerNewsType[news_typeID] #calc mean RELEVANCE per news_type
meanRELEVANCEDF = pd.DataFrame.from_dict(meanRELEVANCEPerNewsType,orient="index") #convert dictionary to pandas DataFrame
meanRELEVANCEDF.reset_index(inplace=True)
meanRELEVANCEDF.columns = ["NewsType ID","Mean Relevance"] #add headers
meanRELEVANCEDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_RELEVANCE_per_news_type.csv") #save to csv file
for news_typeID in meanENSPerNewsType:
    meanENSPerNewsType[news_typeID] = float(meanENSPerNewsType[news_typeID]) / ESSItemsPerNewsType[news_typeID] #calc mean ENS per news_type
meanENSDF = pd.DataFrame.from_dict(meanENSPerNewsType,orient="index") #convert dictionary to pandas DataFrame
meanENSDF.reset_index(inplace=True)
meanENSDF.columns = ["NewsType ID","Mean ENS"] #add headers
meanENSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_per_news_type.csv") #save to csv file
for news_typeID in meanENS_SIMILARITY_GAPPerNewsType:
    meanENS_SIMILARITY_GAPPerNewsType[news_typeID] = float(meanENS_SIMILARITY_GAPPerNewsType[news_typeID]) / ESSItemsPerNewsType[news_typeID] #calc mean ENS_SIMILARITY_GAP per news_type
meanENS_SIMILARITY_GAPDF = pd.DataFrame.from_dict(meanENS_SIMILARITY_GAPPerNewsType,orient="index") #convert dictionary to pandas DataFrame
meanENS_SIMILARITY_GAPDF.reset_index(inplace=True)
meanENS_SIMILARITY_GAPDF.columns = ["NewsType ID","Mean ENS_SIMILARITY_GAP"] #add headers
meanENS_SIMILARITY_GAPDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_SIMILARITY_GAP_per_news_type.csv") #save to csv file
for news_typeID in meanENS_ELAPSEDPerNewsType:
    meanENS_ELAPSEDPerNewsType[news_typeID] = float(meanENS_ELAPSEDPerNewsType[news_typeID]) / ESSItemsPerNewsType[news_typeID] #calc mean ENS_ELAPSED per news_type
meanENS_ELAPSEDDF = pd.DataFrame.from_dict(meanENS_ELAPSEDPerNewsType,orient="index") #convert dictionary to pandas DataFrame
meanENS_ELAPSEDDF.reset_index(inplace=True)
meanENS_ELAPSEDDF.columns = ["NewsType ID","Mean ENS_ELAPSED"] #add headers
meanENS_ELAPSEDDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_ENS_ELAPSED_per_news_type.csv") #save to csv file
for news_typeID in meanG_ENSPerNewsType:
    meanG_ENSPerNewsType[news_typeID] = float(meanG_ENSPerNewsType[news_typeID]) / ESSItemsPerNewsType[news_typeID] #calc mean G_ENS per news_type
meanG_ENSDF = pd.DataFrame.from_dict(meanG_ENSPerNewsType,orient="index") #convert dictionary to pandas DataFrame
meanG_ENSDF.reset_index(inplace=True)
meanG_ENSDF.columns = ["NewsType ID","Mean G_ENS"] #add headers
meanG_ENSDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_per_news_type.csv") #save to csv file
for news_typeID in meanG_ENS_SIMILARITY_GAPPerNewsType:
    meanG_ENS_SIMILARITY_GAPPerNewsType[news_typeID] = float(meanG_ENS_SIMILARITY_GAPPerNewsType[news_typeID]) / ESSItemsPerNewsType[news_typeID] #calc mean G_ENS_SIMILARITY_GAP per news_type
meanG_ENS_SIMILARITY_GAPDF = pd.DataFrame.from_dict(meanG_ENS_SIMILARITY_GAPPerNewsType,orient="index") #convert dictionary to pandas DataFrame
meanG_ENS_SIMILARITY_GAPDF.reset_index(inplace=True)
meanG_ENS_SIMILARITY_GAPDF.columns = ["NewsType ID","Mean G_ENS_SIMILARITY_GAP"] #add headers
meanG_ENS_SIMILARITY_GAPDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_SIMILARITY_GAP_per_news_type.csv") #save to csv file
for news_typeID in meanG_ENS_ELAPSEDPerNewsType:
    meanG_ENS_ELAPSEDPerNewsType[news_typeID] = float(meanG_ENS_ELAPSEDPerNewsType[news_typeID]) / ESSItemsPerNewsType[news_typeID] #calc mean G_ENS_ELAPSED per news_type
meanG_ENS_ELAPSEDDF = pd.DataFrame.from_dict(meanG_ENS_ELAPSEDPerNewsType,orient="index") #convert dictionary to pandas DataFrame
meanG_ENS_ELAPSEDDF.reset_index(inplace=True)
meanG_ENS_ELAPSEDDF.columns = ["NewsType ID","Mean G_ENS_ELAPSED"] #add headers
meanG_ENS_ELAPSEDDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/mean_G_ENS_ELAPSED_per_news_type.csv") #save to csv file
#task 11 final processing
posNewsPerStockDF = pd.DataFrame.from_dict(posNewsPerStock,orient="index") #convert dictionary to pandas DataFrame
posNewsPerStockDF.reset_index(inplace=True)
posNewsPerStockDF.columns = ["Stock ID","Positive News Items"] #add headers
posNewsPerStockDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/pos_news_per_stock.csv")
negNewsPerStockDF = pd.DataFrame.from_dict(negNewsPerStock,orient="index") #convert dictionary to pandas DataFrame
negNewsPerStockDF.reset_index(inplace=True)
negNewsPerStockDF.columns = ["Stock ID","Negative News Items"] #add headers
negNewsPerStockDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/neg_news_per_stock.csv")
#task 12 final processing
posNewsPerCountryDF = pd.DataFrame.from_dict(posNewsPerCountry,orient="index") #convert dictionary to pandas DataFrame
posNewsPerCountryDF.reset_index(inplace=True)
posNewsPerCountryDF.columns = ["Country","Positive News Items"] #add headers
posNewsPerCountryDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/pos_news_per_country.csv")
negNewsPerCountryDF = pd.DataFrame.from_dict(negNewsPerCountry,orient="index") #convert dictionary to pandas DataFrame
negNewsPerCountryDF.reset_index(inplace=True)
negNewsPerCountryDF.columns = ["Country","Negative News Items"] #add headers
negNewsPerCountryDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/neg_news_per_country.csv")
#task 13 final processing - Topic
posNewsPerTopicDF = pd.DataFrame.from_dict(posNewsPerTopic,orient="index") #convert dictionary to pandas DataFrame
posNewsPerTopicDF.reset_index(inplace=True)
posNewsPerTopicDF.columns = ["Topic","Positive News Items"] #add headers
posNewsPerTopicDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/pos_news_per_topic.csv")
negNewsPerTopicDF = pd.DataFrame.from_dict(negNewsPerTopic,orient="index") #convert dictionary to pandas DataFrame
negNewsPerTopicDF.reset_index(inplace=True)
negNewsPerTopicDF.columns = ["Topic","Negative News Items"] #add headers
negNewsPerTopicDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/neg_news_per_topic.csv")
#task 13 final processing - Group
posNewsPerGroupDF = pd.DataFrame.from_dict(posNewsPerGroup,orient="index") #convert dictionary to pandas DataFrame
posNewsPerGroupDF.reset_index(inplace=True)
posNewsPerGroupDF.columns = ["Group","Positive News Items"] #add headers
posNewsPerGroupDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/pos_news_per_group.csv")
negNewsPerGroupDF = pd.DataFrame.from_dict(negNewsPerGroup,orient="index") #convert dictionary to pandas DataFrame
negNewsPerGroupDF.reset_index(inplace=True)
negNewsPerGroupDF.columns = ["Group","Negative News Items"] #add headers
negNewsPerGroupDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/neg_news_per_group.csv")
#task 13 final processing - Type
posNewsPerTypeDF = pd.DataFrame.from_dict(posNewsPerType,orient="index") #convert dictionary to pandas DataFrame
posNewsPerTypeDF.reset_index(inplace=True)
posNewsPerTypeDF.columns = ["Type","Positive News Items"] #add headers
posNewsPerTypeDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/pos_news_per_type.csv")
negNewsPerTypeDF = pd.DataFrame.from_dict(negNewsPerType,orient="index") #convert dictionary to pandas DataFrame
negNewsPerTypeDF.reset_index(inplace=True)
negNewsPerTypeDF.columns = ["Type","Negative News Items"] #add headers
negNewsPerTypeDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/neg_news_per_type.csv")
#task 13 final processing - Category
posNewsPerCategoryDF = pd.DataFrame.from_dict(posNewsPerCategory,orient="index") #convert dictionary to pandas DataFrame
posNewsPerCategoryDF.reset_index(inplace=True)
posNewsPerCategoryDF.columns = ["Category","Positive News Items"] #add headers
posNewsPerCategoryDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/pos_news_per_category.csv")
negNewsPerCategoryDF = pd.DataFrame.from_dict(negNewsPerCategory,orient="index") #convert dictionary to pandas DataFrame
negNewsPerCategoryDF.reset_index(inplace=True)
negNewsPerCategoryDF.columns = ["Category","Negative News Items"] #add headers
negNewsPerCategoryDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/neg_news_per_category.csv")
#task 13 final processing - NewsType
posNewsPerNewsTypeDF = pd.DataFrame.from_dict(posNewsPerNewsType,orient="index") #convert dictionary to pandas DataFrame
posNewsPerNewsTypeDF.reset_index(inplace=True)
posNewsPerNewsTypeDF.columns = ["News Type","Positive News Items"] #add headers
posNewsPerNewsTypeDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/pos_news_per_news_type.csv")
negNewsPerNewsTypeDF = pd.DataFrame.from_dict(negNewsPerNewsType,orient="index") #convert dictionary to pandas DataFrame
negNewsPerNewsTypeDF.reset_index(inplace=True)
negNewsPerNewsTypeDF.columns = ["News Type","Negative News Items"] #add headers
negNewsPerNewsTypeDF.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/neg_news_per_news_type.csv")

#Printing Results:

#1. Find the total number of news items.
print("Total number of news items:", totalItems)

#2. Find the overall average of different sentiment, relevance, and novelty measures (consult RavenPack USER GUIDE).
print("Average Event Sentiment:", meanESS)
print("Average Aggregate Event Sentiment:", meanAES)
print("Average Relevance:", meanRELEVANCE)
print("Average Novelty:", meanENS)
print("Average Novelty Similarity Gap:", meanENS_SIMILARITY_GAP)
print("Average Novelty Elapsed Time:", meanENS_ELAPSED)
print("Average Global Novelty:", meanG_ENS)
print("Average Global Novelty Similarity Gap:", meanG_ENS_SIMILARITY_GAP)
print("Average Global Novelty Elapsed Time:", meanG_ENS_ELAPSED)

#3. Find the number of news items per stock (use RP_ENTITY_ID to identify stocks).
print("Number of News Items Per Stock: see items_per_stock.csv")

#4. Find the number of news items per country (use COUNTRY_CODE to identify stocks).
print("Number of News Items Per Country: see items_per_country.csv")

#5. Find the number of news items per TOPIC, GROUP, TYPE, CATEGORY, NEWS TYPE, separately.
print("Number of News Items Per Topic: see items_per_topic.csv")
print("Number of News Items Per Group: see items_per_group.csv")
print("Number of News Items Per Type: see items_per_type.csv")
print("Number of News Items Per Category: see items_per_category.csv")
print("Number of News Items Per News Type: see items_per_news_type.csv")

#6 & 7. Find the average of different sentiment, relevance, and novelty measures per stock (use RP_ENTITY_ID to identify stocks).
print("Average Event Sentiment For Each Stock: see mean_ESS_per_stock.csv")
print("Average Aggregate Event Sentiment For Each Stock: see mean_AES_per_stock.csv")
print("Average Relevance For Each Stock: see mean_RELEVANCE_per_stock.csv")
print("Average Novelty For Each Stock: see mean_ENS_per_stock.csv")
print("Average Novelty Similarity Gap For Each Stock: see mean_ENS_SIMILARITY_GAP_per_stock.csv")
print("Average Novelty Elapsed Time For Each Stock: see mean_ENS_ELAPSED_per_stock.csv")
print("Average Global Novelty For Each Stock: see mean_G_ENS_per_stock.csv")
print("Average Global Novelty Similarity Gap For Each Stock: see mean_G_ENS_SIMILARITY_GAP_per_stock.csv")
print("Average Global Novelty Elapsed Time For Each Stock: see mean_G_ENS_ELAPSED_per_stock.csv")

#8. Find the average of different sentiment, relevance, and novelty measures per country (use COUNTRY_CODE to identify stocks).
print("Average Event Sentiment For Each Country: see mean_ESS_per_country.csv")
print("Average Aggregate Event Sentiment For Each Country: see mean_AES_per_country.csv")
print("Average Relevance For Each Country: see mean_RELEVANCE_per_country.csv")
print("Average Novelty For Each Country: see mean_ENS_per_country.csv")
print("Average Novelty Similarity Gap For Each Country: see mean_ENS_SIMILARITY_GAP_per_country.csv")
print("Average Novelty Elapsed Time For Each Country: see mean_ENS_ELAPSED_per_country.csv")
print("Average Global Novelty For Each Country: see mean_G_ENS_per_country.csv")
print("Average Global Novelty Similarity Gap For Each Country: see mean_G_ENS_SIMILARITY_GAP_per_country.csv")
print("Average Global Novelty Elapsed Time For Each Country: see mean_G_ENS_ELAPSED_per_country.csv")

#9. Find the average of different sentiment, relevance, and novelty measures per TOPIC, GROUP, TYPE, CATEGORY, NEWS TYPE, separately.
print("Average Event Sentiment For Each Topic: see mean_ESS_per_topic.csv")
print("Average Aggregate Event Sentiment For Each Topic: see mean_AES_per_topic.csv")
print("Average Relevance For Each Topic: see mean_RELEVANCE_per_topic.csv")
print("Average Novelty For Each Topic: see mean_ENS_per_topic.csv")
print("Average Novelty Similarity Gap For Each Topic: see mean_ENS_SIMILARITY_GAP_per_topic.csv")
print("Average Novelty Elapsed Time For Each Topic: see mean_ENS_ELAPSED_per_topic.csv")
print("Average Global Novelty For Each Topic: see mean_G_ENS_per_topic.csv")
print("Average Global Novelty Similarity Gap For Each Topic: see mean_G_ENS_SIMILARITY_GAP_per_topic.csv")
print("Average Global Novelty Elapsed Time For Each Topic: see mean_G_ENS_ELAPSED_per_topic.csv")

print("Average Event Sentiment For Each Group: see mean_ESS_per_group.csv")
print("Average Aggregate Event Sentiment For Each Group: see mean_AES_per_group.csv")
print("Average Relevance For Each Group: see mean_RELEVANCE_per_group.csv")
print("Average Novelty For Each Group: see mean_ENS_per_group.csv")
print("Average Novelty Similarity Gap For Each Group: see mean_ENS_SIMILARITY_GAP_per_group.csv")
print("Average Novelty Elapsed Time For Each Group: see mean_ENS_ELAPSED_per_group.csv")
print("Average Global Novelty For Each Group: see mean_G_ENS_per_group.csv")
print("Average Global Novelty Similarity Gap For Each Group: see mean_G_ENS_SIMILARITY_GAP_per_group.csv")
print("Average Global Novelty Elapsed Time For Each Group: see mean_G_ENS_ELAPSED_per_group.csv")

print("Average Event Sentiment For Each Type: see mean_ESS_per_type.csv")
print("Average Aggregate Event Sentiment For Each Type: see mean_AES_per_type.csv")
print("Average Relevance For Each Type: see mean_RELEVANCE_per_type.csv")
print("Average Novelty For Each Type: see mean_ENS_per_type.csv")
print("Average Novelty Similarity Gap For Each Type: see mean_ENS_SIMILARITY_GAP_per_type.csv")
print("Average Novelty Elapsed Time For Each Type: see mean_ENS_ELAPSED_per_type.csv")
print("Average Global Novelty For Each Type: see mean_G_ENS_per_type.csv")
print("Average Global Novelty Similarity Gap For Each Type: see mean_G_ENS_SIMILARITY_GAP_per_type.csv")
print("Average Global Novelty Elapsed Time For Each Type: see mean_G_ENS_ELAPSED_per_type.csv")

print("Average Event Sentiment For Each Category: see mean_ESS_per_category.csv")
print("Average Aggregate Event Sentiment For Each Category: see mean_AES_per_category.csv")
print("Average Relevance For Each Category: see mean_RELEVANCE_per_category.csv")
print("Average Novelty For Each Category: see mean_ENS_per_category.csv")
print("Average Novelty Similarity Gap For Each Category: see mean_ENS_SIMILARITY_GAP_per_category.csv")
print("Average Novelty Elapsed Time For Each Category: see mean_ENS_ELAPSED_per_category.csv")
print("Average Global Novelty For Each Category: see mean_G_ENS_per_category.csv")
print("Average Global Novelty Similarity Gap For Each Category: see mean_G_ENS_SIMILARITY_GAP_per_category.csv")
print("Average Global Novelty Elapsed Time For Each Category: see mean_G_ENS_ELAPSED_per_category.csv")

print("Average Event Sentiment For Each News Type: see mean_ESS_per_news_type.csv")
print("Average Aggregate Event Sentiment For Each News Type: see mean_AES_per_news_type.csv")
print("Average Relevance For Each News Type: see mean_RELEVANCE_per_news_type.csv")
print("Average Novelty For Each News Type: see mean_ENS_per_news_type.csv")
print("Average Novelty Similarity Gap For Each News Type: see mean_ENS_SIMILARITY_GAP_per_news_type.csv")
print("Average Novelty Elapsed Time For Each News Type: see mean_ENS_ELAPSED_per_news_type.csv")
print("Average Global Novelty For Each News Type: see mean_G_ENS_per_news_type.csv")
print("Average Global Novelty Similarity Gap For Each News Type: see mean_G_ENS_SIMILARITY_GAP_per_news_type.csv")
print("Average Global Novelty Elapsed Time For Each News Type: see mean_G_ENS_ELAPSED_per_news_type.csv")

#10. Find the total number of positive, and negative news.
print("Total Positive News Items:", totalPosItems)
print("Total Negative News Items:", totalNegItems)

#11. Find the total number of positive, and negative news per stock.
print("Total Positive News Items Per Stock: see pos_news_per_stock.csv")
print("Total Negative News Items Per Stock: see neg_news_per_stock.csv")

#12. Find the number of positive, and negative news per country (use COUNTRY_CODE to identify stocks).
print("Total Positive News Items Per Country: see pos_news_per_country.csv")
print("Total Negative News Items Per Country: see neg_news_per_country.csv")

#13. Find the number of positive, and negative news per TOPIC, GROUP, TYPE, CATEGORY, NEWS TYPE, separately.
print("Total Positive News Items Per Topic: see pos_news_per_topic.csv")
print("Total Negative News Items Per Topic: see neg_news_per_topic.csv")
print("Total Positive News Items Per Group: see pos_news_per_group.csv")
print("Total Negative News Items Per Group: see neg_news_per_group.csv")
print("Total Positive News Items Per Type: see pos_news_per_type.csv")
print("Total Negative News Items Per Type: see neg_news_per_type.csv")
print("Total Positive News Items Per Category: see pos_news_per_category.csv")
print("Total Negative News Items Per Category: see neg_news_per_category.csv")
print("Total Positive News Items Per News Type: see pos_news_per_news_type.csv")
print("Total Negative News Items Per News Type: see neg_news_per_news_type.csv")
'''
#14. Find the total number of news items by days (Mon-Sun).
print("Total Monday News:", monCount)
print("Total Tuesday News:", tueCount)
print("Total Wednesday News:", wedCount)
print("Total Thursday News:", thuCount)
print("Total Friday News:", friCount)
print("Total Saturday News:", satCount)
print("Total Sunday News:", sunCount)

#15. Find the total number of news items by weekdays vs weekends (only 2 categories).
print("Total Weekday News:", weekdayCount)
print("Total Weekend News:", weekendCount)

#16. Find the total number of news items published before and after 3:30 pm on weekdays.
print("Total Weekday News Before 3:30 pm:", before330Count)
print("Total Weekday News After 3:30 pm:", after330Count)
'''