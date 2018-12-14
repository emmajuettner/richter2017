#Additional tasks
import pandas as pd
from dateutil.parser import parse
import datetime as dt
fileName = "C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/final_dataset.csv"
#fileName = "C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/2000-01-equities.csv"

totalItemsByDay = [0,0,0,0,0,0,0]
totalESSItemsByDay = [0,0,0,0,0,0,0]
meanESSbyDay = [0,0,0,0,0,0,0]
meanAESbyDay = [0,0,0,0,0,0,0]
meanRELEVANCEbyDay = [0,0,0,0,0,0,0]
meanENSbyDay = [0,0,0,0,0,0,0]
meanENS_SIMILARITY_GAPbyDay = [0,0,0,0,0,0,0]
meanENS_ELAPSEDbyDay = [0,0,0,0,0,0,0]
meanG_ENSbyDay = [0,0,0,0,0,0,0]
meanG_ENS_SIMILARITY_GAPbyDay = [0,0,0,0,0,0,0]
meanG_ENS_ELAPSEDbyDay = [0,0,0,0,0,0,0]
totalPosItemsbyDay = [0,0,0,0,0,0,0]
totalNegItemsbyDay = [0,0,0,0,0,0,0]

time1 = dt.time(hour=15,minute=30)
totalItemsByTime = [0,0]
totalESSItemsByTime = [0,0]
meanESSbyTime = [0,0]
meanAESbyTime = [0,0]
meanRELEVANCEbyTime = [0,0]
meanENSbyTime = [0,0]
meanENS_SIMILARITY_GAPbyTime = [0,0]
meanENS_ELAPSEDbyTime = [0,0]
meanG_ENSbyTime = [0,0]
meanG_ENS_SIMILARITY_GAPbyTime = [0,0]
meanG_ENS_ELAPSEDbyTime = [0,0]
totalPosItemsbyTime = [0,0]
totalNegItemsbyTime = [0,0]

totalItemsByMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
totalESSItemsByMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanESSbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanAESbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanRELEVANCEbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanENSbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanENS_SIMILARITY_GAPbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanENS_ELAPSEDbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENSbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENS_SIMILARITY_GAPbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENS_ELAPSEDbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
totalPosItemsbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
totalNegItemsbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]

totalItemsByYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
totalESSItemsByYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanESSbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanAESbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanRELEVANCEbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanENSbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanENS_SIMILARITY_GAPbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanENS_ELAPSEDbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENSbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENS_SIMILARITY_GAPbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENS_ELAPSEDbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
totalPosItemsbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
totalNegItemsbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]

#this for loop deals with tasks 1-4
for chunk in pd.read_csv(fileName,encoding='latin1',comment="TIMESTAMP_UTC",chunksize=10**6,engine="python",header=None,names=["TIMESTAMP_UTC","RP_ENTITY_ID","ENTITY_TYPE","ENTITY_NAME","POSITION_NAME","RP_POSITION_ID","COUNTRY_CODE","RELEVANCE","TOPIC","GROUP","TYPE","SUB_TYPE","PROPERTY","EVALUATION_METHOD","MATURITY","CATEGORY","ESS","AES","AEV","ENS","ENS_SIMILARITY_GAP","ENS_KEY","ENS_ELAPSED","G_ENS","G_ENS_SIMILARITY_GAP","G_ENS_KEY","G_ENS_ELAPSED","EVENT_SIMILARITY_KEY","NEWS_TYPE","SOURCE","RP_STORY_ID","RP_STORY_EVENT_INDEX","RP_STORY_EVENT_COUNT","PRODUCT","COMPANY","ISIN","CSS","NIP","PEQ","BEE","BMQ","BAM","BCA","BER","ANL_CHG","MCQ"]):
    for index, row in chunk.iterrows():
        date = parse(row["TIMESTAMP_UTC"])
        dayOfWeek = date.weekday() #a number 0-6, starting with Monday as 0
        timeOfDay = date.time()
        itemMonth = date.month - 1 #a number 1-12, starting with January as 0
        itemYear = date.year - 2000 #a number 0-16, starting with 2000 as 0
        if dayOfWeek >= 0 and dayOfWeek <= 4: #check whether the time is before or after 3:30 pm (only for weekdays)
            if timeOfDay < time1:
                timeFlag = 0
            elif timeOfDay > time1:
                timeFlag = 1
            totalItemsByTime[timeFlag] = totalItemsByTime[timeFlag] + 1
            meanAESbyTime[timeFlag] = meanAESbyTime[timeFlag] + row["AES"]
            meanRELEVANCEbyTime[timeFlag] = meanRELEVANCEbyTime[timeFlag] + row["RELEVANCE"]
        totalItemsByDay[dayOfWeek] = totalItemsByDay[dayOfWeek] + 1
        meanAESbyDay[dayOfWeek] = meanAESbyDay[dayOfWeek] + row["AES"]
        meanRELEVANCEbyDay[dayOfWeek] = meanRELEVANCEbyDay[dayOfWeek] + row["RELEVANCE"]
        
        totalItemsByMonth[itemMonth] = totalItemsByMonth[itemMonth] + 1
        meanAESbyMonth[itemMonth] = meanAESbyMonth[itemMonth] + row["AES"]
        meanRELEVANCEbyMonth[itemMonth] = meanRELEVANCEbyMonth[itemMonth] + row["RELEVANCE"]
        
        totalItemsByYear[itemYear] = totalItemsByYear[itemYear] + 1
        meanAESbyYear[itemYear] = meanAESbyYear[itemYear] + row["AES"]
        meanRELEVANCEbyYear[itemYear] = meanRELEVANCEbyYear[itemYear] + row["RELEVANCE"]
        
        if row["ESS"] >= 0 or row["ESS"] < 0:
            totalESSItemsByDay[dayOfWeek] = totalESSItemsByDay[dayOfWeek] + 1
            meanESSbyDay[dayOfWeek] = meanESSbyDay[dayOfWeek] + row["ESS"]
            meanENSbyDay[dayOfWeek] = meanENSbyDay[dayOfWeek] + row["ENS"]
            meanENS_SIMILARITY_GAPbyDay[dayOfWeek] = meanENS_SIMILARITY_GAPbyDay[dayOfWeek] + row["ENS_SIMILARITY_GAP"]
            meanENS_ELAPSEDbyDay[dayOfWeek] = meanENS_ELAPSEDbyDay[dayOfWeek] + row["ENS_ELAPSED"]
            meanG_ENSbyDay[dayOfWeek] = meanG_ENSbyDay[dayOfWeek] + row["G_ENS"]
            meanG_ENS_SIMILARITY_GAPbyDay[dayOfWeek] = meanG_ENS_SIMILARITY_GAPbyDay[dayOfWeek] + row["G_ENS_SIMILARITY_GAP"]
            meanG_ENS_ELAPSEDbyDay[dayOfWeek] = meanG_ENS_ELAPSEDbyDay[dayOfWeek] + row["G_ENS_ELAPSED"]
            
            totalESSItemsByTime[timeFlag] = totalESSItemsByTime[timeFlag] + 1
            meanESSbyTime[timeFlag] = meanESSbyTime[timeFlag] + row["ESS"]
            meanENSbyTime[timeFlag] = meanENSbyTime[timeFlag] + row["ENS"]
            meanENS_SIMILARITY_GAPbyTime[timeFlag] = meanENS_SIMILARITY_GAPbyTime[timeFlag] + row["ENS_SIMILARITY_GAP"]
            meanENS_ELAPSEDbyTime[timeFlag] = meanENS_ELAPSEDbyTime[timeFlag] + row["ENS_ELAPSED"]
            meanG_ENSbyTime[timeFlag] = meanG_ENSbyTime[timeFlag] + row["G_ENS"]
            meanG_ENS_SIMILARITY_GAPbyTime[timeFlag] = meanG_ENS_SIMILARITY_GAPbyTime[timeFlag] + row["G_ENS_SIMILARITY_GAP"]
            meanG_ENS_ELAPSEDbyTime[timeFlag] = meanG_ENS_ELAPSEDbyTime[timeFlag] + row["G_ENS_ELAPSED"]
            
            totalESSItemsByMonth[itemMonth] = totalESSItemsByMonth[itemMonth] + 1
            meanESSbyMonth[itemMonth] = meanESSbyMonth[itemMonth] + row["ESS"]
            meanENSbyMonth[itemMonth] = meanENSbyMonth[itemMonth] + row["ENS"]
            meanENS_SIMILARITY_GAPbyMonth[itemMonth] = meanENS_SIMILARITY_GAPbyMonth[itemMonth] + row["ENS_SIMILARITY_GAP"]
            meanENS_ELAPSEDbyMonth[itemMonth] = meanENS_ELAPSEDbyMonth[itemMonth] + row["ENS_ELAPSED"]
            meanG_ENSbyMonth[itemMonth] = meanG_ENSbyMonth[itemMonth] + row["G_ENS"]
            meanG_ENS_SIMILARITY_GAPbyMonth[itemMonth] = meanG_ENS_SIMILARITY_GAPbyMonth[itemMonth] + row["G_ENS_SIMILARITY_GAP"]
            meanG_ENS_ELAPSEDbyMonth[itemMonth] = meanG_ENS_ELAPSEDbyMonth[itemMonth] + row["G_ENS_ELAPSED"]
            
            totalESSItemsByYear[itemYear] = totalESSItemsByYear[itemYear] + 1
            meanESSbyYear[itemYear] = meanESSbyYear[itemYear] + row["ESS"]
            meanENSbyYear[itemYear] = meanENSbyYear[itemYear] + row["ENS"]
            meanENS_SIMILARITY_GAPbyYear[itemYear] = meanENS_SIMILARITY_GAPbyYear[itemYear] + row["ENS_SIMILARITY_GAP"]
            meanENS_ELAPSEDbyYear[itemYear] = meanENS_ELAPSEDbyYear[itemYear] + row["ENS_ELAPSED"]
            meanG_ENSbyYear[itemYear] = meanG_ENSbyYear[itemYear] + row["G_ENS"]
            meanG_ENS_SIMILARITY_GAPbyYear[itemYear] = meanG_ENS_SIMILARITY_GAPbyYear[itemYear] + row["G_ENS_SIMILARITY_GAP"]
            meanG_ENS_ELAPSEDbyYear[itemYear] = meanG_ENS_ELAPSEDbyYear[itemYear] + row["G_ENS_ELAPSED"]
            
            if row["ESS"] > 50:
                totalPosItemsbyDay[dayOfWeek] = totalPosItemsbyDay[dayOfWeek] + 1
                totalPosItemsbyTime[timeFlag] = totalPosItemsbyTime[timeFlag] + 1
                totalPosItemsbyMonth[itemMonth] = totalPosItemsbyMonth[itemMonth] + 1
                totalPosItemsbyYear[itemYear] = totalPosItemsbyYear[itemYear] + 1
            elif row["ESS"] < 50:
                totalNegItemsbyDay[dayOfWeek] = totalNegItemsbyDay[dayOfWeek] + 1
                totalNegItemsbyTime[timeFlag] = totalNegItemsbyTime[timeFlag] + 1
                totalNegItemsbyMonth[itemMonth] = totalNegItemsbyMonth[itemMonth] + 1
                totalNegItemsbyYear[itemYear] = totalNegItemsbyYear[itemYear] + 1

#Final Processing

#task 2 final processing
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

#task 3 final processing
totalItemsByTime = [totalItemsByTime[0],totalItemsByTime[1]]
totalESSItemsByTime = [totalESSItemsByTime[0],totalESSItemsByTime[1]]
meanESSbyTime = [float(meanESSbyTime[0])/totalESSItemsByTime,float(meanESSbyTime[1])/totalESSItemsByTime]
meanAESbyTime = [float(meanAESbyTime[0])/totalItemsByTime,float(meanAESbyTime[1])/totalItemsByTime]
meanRELEVANCEbyDayType = [float(meanRELEVANCEbyTime[0])/totalItemsByTime,float(meanRELEVANCEbyTime[1])/totalItemsByTime]
meanENSbyTime = [float(meanENSbyTime[0])/totalESSItemsByTime,float(meanENSbyTime[1])/totalESSItemsByTime]
meanENS_ELAPSEDbyTime = [float(meanENS_ELAPSEDbyTime[0])/totalESSItemsByTime,float(meanENS_ELAPSEDbyTime[1])/totalESSItemsByTime]
meanENS_SIMILARITY_GAPbyTime = [float(meanENS_SIMILARITY_GAPbyTime[0])/totalESSItemsByTime,float(meanENS_SIMILARITY_GAPbyTime[1])/totalESSItemsByTime]
meanG_ENSbyTime = [float(meanG_ENSbyTime[0])/totalESSItemsByTime,float(meanG_ENSbyTime[1])/totalESSItemsByTime]
meanG_ENS_ELAPSEDbyTime = [float(meanG_ENS_ELAPSEDbyTime[0])/totalESSItemsByTime,float(meanG_ENS_ELAPSEDbyTime[1])/totalESSItemsByTime]
meanG_ENS_SIMILARITY_GAPbyTime = [float(meanG_ENS_SIMILARITY_GAPbyTime[0])/totalESSItemsByTime,float(meanG_ENS_SIMILARITY_GAPbyTime[1])/totalESSItemsByTime]

#task 1 final processing
for i in range(7):
    meanESSbyDay[i] = float(meanESSbyDay[i]) / totalESSItemsByDay[i]
    meanAESbyDay[i] = float(meanAESbyDay[i]) / totalItemsByDay[i]
    meanRELEVANCEbyDay[i] = float(meanRELEVANCEbyDay[i]) / totalItemsByDay[i]
    meanENSbyDay[i] = float(meanENSbyDay[i]) / totalESSItemsByDay[i]
    meanENS_SIMILARITY_GAPbyDay[i] = float(meanENS_SIMILARITY_GAPbyDay[i]) / totalESSItemsByDay[i]
    meanENS_ELAPSEDbyDay[i] = float(meanENS_ELAPSEDbyDay[i]) / totalESSItemsByDay[i]
    meanG_ENSbyDay[i] = float(meanG_ENSbyDay[i]) / totalESSItemsByDay[i]
    meanG_ENS_SIMILARITY_GAPbyDay[i] = float(meanG_ENS_SIMILARITY_GAPbyDay[i]) / totalESSItemsByDay[i]
    meanG_ENS_ELAPSEDbyDay[i] = float(meanG_ENS_ELAPSEDbyDay[i]) / totalESSItemsByDay[i]

#task 4 final processing
for i in range(12):
    meanESSbyMonth[i] = float(meanESSbyMonth[i]) / totalESSItemsByMonth[i]
    meanAESbyMonth[i] = float(meanAESbyMonth[i]) / totalItemsByMonth[i]
    meanRELEVANCEbyMonth[i] = float(meanRELEVANCEbyMonth[i]) / totalItemsByMonth[i]
    meanENSbyMonth[i] = float(meanENSbyMonth[i]) / totalESSItemsByMonth[i]
    meanENS_SIMILARITY_GAPbyMonth[i] = float(meanENS_SIMILARITY_GAPbyMonth[i]) / totalESSItemsByMonth[i]
    meanENS_ELAPSEDbyMonth[i] = float(meanENS_ELAPSEDbyMonth[i]) / totalESSItemsByMonth[i]
    meanG_ENSbyMonth[i] = float(meanG_ENSbyMonth[i]) / totalESSItemsByMonth[i]
    meanG_ENS_SIMILARITY_GAPbyMonth[i] = float(meanG_ENS_SIMILARITY_GAPbyMonth[i]) / totalESSItemsByMonth[i]
    meanG_ENS_ELAPSEDbyMonth[i] = float(meanG_ENS_ELAPSEDbyMonth[i]) / totalESSItemsByMonth[i]

for i in range(17):
    meanESSbyYear[i] = float(meanESSbyYear[i]) / totalESSItemsByYear[i]
    meanAESbyYear[i] = float(meanAESbyYear[i]) / totalItemsByYear[i]
    meanRELEVANCEbyYear[i] = float(meanRELEVANCEbyYear[i]) / totalItemsByYear[i]
    meanENSbyYear[i] = float(meanENSbyYear[i]) / totalESSItemsByYear[i]
    meanENS_SIMILARITY_GAPbyYear[i] = float(meanENS_SIMILARITY_GAPbyYear[i]) / totalESSItemsByYear[i]
    meanENS_ELAPSEDbyYear[i] = float(meanENS_ELAPSEDbyYear[i]) / totalESSItemsByYear[i]
    meanG_ENSbyYear[i] = float(meanG_ENSbyYear[i]) / totalESSItemsByYear[i]
    meanG_ENS_SIMILARITY_GAPbyYear[i] = float(meanG_ENS_SIMILARITY_GAPbyYear[i]) / totalESSItemsByYear[i]
    meanG_ENS_ELAPSEDbyYear[i] = float(meanG_ENS_ELAPSEDbyYear[i]) / totalESSItemsByYear[i]

#printing
#(results are stored in arrays, index values indicate which day of week/month/etc. the result is associated with)
#1. Average of different sentiment, relevance, and novelty measures; total number of positive, and negative news by days (Mon-Sun).
print("Sentiment/Relevance/Novelty Measures by Day of Week (Monday = 0)")
print("Average Event Sentiment:", meanESSbyDay)
print("Average Aggregate Event Sentiment:", meanAESbyDay)
print("Average Relevance:", meanRELEVANCEbyDay)
print("Average Novelty:", meanENSbyDay)
print("Average Novelty Similarity Gap:", meanENS_SIMILARITY_GAPbyDay)
print("Average Novelty Elapsed Time:", meanENS_ELAPSEDbyDay)
print("Average Global Novelty:", meanG_ENSbyDay)
print("Average Global Novelty Similarity Gap:", meanG_ENS_SIMILARITY_GAPbyDay)
print("Average Global Novelty Elapsed Time:", meanG_ENS_ELAPSEDbyDay)
print("Total Positive News Items:", totalPosItemsbyDay)
print("Total Negative News Items:", totalNegItemsbyDay)

#2. Average of different sentiment, relevance, and novelty measures; total number of positive, and negative news by weekdays vs weekends (only 2 categories).
print("Sentiment/Relevance/Novelty Measures by Weekday vs. Weekend (weekday = 0, weekend = 1)")
print("Average Event Sentiment:", meanESSbyDayType)
print("Average Aggregate Event Sentiment:", meanAESbyDayType)
print("Average Relevance:", meanRELEVANCEbyDayType)
print("Average Novelty:", meanENSbyDayType)
print("Average Novelty Similarity Gap:", meanENS_SIMILARITY_GAPbyDayType)
print("Average Novelty Elapsed Time:", meanENS_ELAPSEDbyDayType)
print("Average Global Novelty:", meanG_ENSbyDayType)
print("Average Global Novelty Similarity Gap:", meanG_ENS_SIMILARITY_GAPbyDayType)
print("Average Global Novelty Elapsed Time:", meanG_ENS_ELAPSEDbyDayType)
print("Total Positive News Items:", totalPosItemsbyDayType)
print("Total Negative News Items:", totalNegItemsbyDayType)

#3. Average of different sentiment, relevance, and novelty measures; total number of positive, and negative news of news items published before and after 3:30 pm on weekdays.
print("Sentiment/Relevance/Novelty Measures Before/After 3:30 pm (weekdays only) (before 3:30pm = 0, after 3:30pm = 1)")
print("Average Event Sentiment:", meanESSbyTime)
print("Average Aggregate Event Sentiment:", meanAESbyTime)
print("Average Relevance:", meanRELEVANCEbyTime)
print("Average Novelty:", meanENSbyTime)
print("Average Novelty Similarity Gap:", meanENS_SIMILARITY_GAPbyTime)
print("Average Novelty Elapsed Time:", meanENS_ELAPSEDbyTime)
print("Average Global Novelty:", meanG_ENSbyTime)
print("Average Global Novelty Similarity Gap:", meanG_ENS_SIMILARITY_GAPbyTime)
print("Average Global Novelty Elapsed Time:", meanG_ENS_ELAPSEDbyTime)
print("Total Positive News Items:", totalPosItemsbyTime)
print("Total Negative News Items:", totalNegItemsbyTime)

#4. Total number of news; average of different sentiment, relevance, and novelty measures; total number of positive, and negative news; per year, and per month (2000-2016, Jan-Dec).
print("Sentiment/Relevance/Novelty Measures by Month (January = 0)")
print("Average Event Sentiment:", meanESSbyMonth)
print("Average Aggregate Event Sentiment:", meanAESbyMonth)
print("Average Relevance:", meanRELEVANCEbyMonth)
print("Average Novelty:", meanENSbyMonth)
print("Average Novelty Similarity Gap:", meanENS_SIMILARITY_GAPbyMonth)
print("Average Novelty Elapsed Time:", meanENS_ELAPSEDbyMonth)
print("Average Global Novelty:", meanG_ENSbyMonth)
print("Average Global Novelty Similarity Gap:", meanG_ENS_SIMILARITY_GAPbyMonth)
print("Average Global Novelty Elapsed Time:", meanG_ENS_ELAPSEDbyMonth)
print("Total Positive News Items:", totalPosItemsbyMonth)
print("Total Negative News Items:", totalNegItemsbyMonth)

print("Sentiment/Relevance/Novelty Measures by Year (2000 = 0)")
print("Average Event Sentiment:", meanESSbyYear)
print("Average Aggregate Event Sentiment:", meanAESbyYear)
print("Average Relevance:", meanRELEVANCEbyYear)
print("Average Novelty:", meanENSbyYear)
print("Average Novelty Similarity Gap:", meanENS_SIMILARITY_GAPbyYear)
print("Average Novelty Elapsed Time:", meanENS_ELAPSEDbyYear)
print("Average Global Novelty:", meanG_ENSbyYear)
print("Average Global Novelty Similarity Gap:", meanG_ENS_SIMILARITY_GAPbyYear)
print("Average Global Novelty Elapsed Time:", meanG_ENS_ELAPSEDbyYear)
print("Total Positive News Items:", totalPosItemsbyYear)
print("Total Negative News Items:", totalNegItemsbyYear)

#5. ONLY NOVEL NEWS: Total number of news; average of different sentiment, relevance measures; total number of positive, and negative news; per year, and per month (2000-2016, Jan-Dec).
noveltyLevel = 100 #indicates lowest level of novelty that will be considered

totalItemsByMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
totalESSItemsByMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanESSbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanAESbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanRELEVANCEbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanENSbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanENS_SIMILARITY_GAPbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanENS_ELAPSEDbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENSbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENS_SIMILARITY_GAPbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENS_ELAPSEDbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
totalPosItemsbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
totalNegItemsbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]

totalItemsByYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
totalESSItemsByYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanESSbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanAESbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanRELEVANCEbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanENSbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanENS_SIMILARITY_GAPbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanENS_ELAPSEDbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENSbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENS_SIMILARITY_GAPbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENS_ELAPSEDbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
totalPosItemsbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
totalNegItemsbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]

for chunk in pd.read_csv(fileName,encoding='latin1',comment="TIMESTAMP_UTC",chunksize=10**6,engine="python",header=None,names=["TIMESTAMP_UTC","RP_ENTITY_ID","ENTITY_TYPE","ENTITY_NAME","POSITION_NAME","RP_POSITION_ID","COUNTRY_CODE","RELEVANCE","TOPIC","GROUP","TYPE","SUB_TYPE","PROPERTY","EVALUATION_METHOD","MATURITY","CATEGORY","ESS","AES","AEV","ENS","ENS_SIMILARITY_GAP","ENS_KEY","ENS_ELAPSED","G_ENS","G_ENS_SIMILARITY_GAP","G_ENS_KEY","G_ENS_ELAPSED","EVENT_SIMILARITY_KEY","NEWS_TYPE","SOURCE","RP_STORY_ID","RP_STORY_EVENT_INDEX","RP_STORY_EVENT_COUNT","PRODUCT","COMPANY","ISIN","CSS","NIP","PEQ","BEE","BMQ","BAM","BCA","BER","ANL_CHG","MCQ"]):
    for index, row in chunk.iterrows():
        if row["ENS"] >= noveltyLevel:
            date = parse(row["TIMESTAMP_UTC"])
            itemMonth = date.month - 1 #a number 1-12, starting with January as 0
            itemYear = date.year - 2000 #a number 0-16, starting with 2000 as 0
            totalItemsByMonth[itemMonth] = totalItemsByMonth[itemMonth] + 1
            meanAESbyMonth[itemMonth] = meanAESbyMonth[itemMonth] + row["AES"]
            meanRELEVANCEbyMonth[itemMonth] = meanRELEVANCEbyMonth[itemMonth] + row["RELEVANCE"]
        
            totalItemsByYear[itemYear] = totalItemsByYear[itemYear] + 1
            meanAESbyYear[itemYear] = meanAESbyYear[itemYear] + row["AES"]
            meanRELEVANCEbyYear[itemYear] = meanRELEVANCEbyYear[itemYear] + row["RELEVANCE"]
        
            if row["ESS"] >= 0 or row["ESS"] < 0:
                totalESSItemsByMonth[itemMonth] = totalESSItemsByMonth[itemMonth] + 1
                meanESSbyMonth[itemMonth] = meanESSbyMonth[itemMonth] + row["ESS"]
                meanENSbyMonth[itemMonth] = meanENSbyMonth[itemMonth] + row["ENS"]
                meanENS_SIMILARITY_GAPbyMonth[itemMonth] = meanENS_SIMILARITY_GAPbyMonth[itemMonth] + row["ENS_SIMILARITY_GAP"]
                meanENS_ELAPSEDbyMonth[itemMonth] = meanENS_ELAPSEDbyMonth[itemMonth] + row["ENS_ELAPSED"]
                meanG_ENSbyMonth[itemMonth] = meanG_ENSbyMonth[itemMonth] + row["G_ENS"]
                meanG_ENS_SIMILARITY_GAPbyMonth[itemMonth] = meanG_ENS_SIMILARITY_GAPbyMonth[itemMonth] + row["G_ENS_SIMILARITY_GAP"]
                meanG_ENS_ELAPSEDbyMonth[itemMonth] = meanG_ENS_ELAPSEDbyMonth[itemMonth] + row["G_ENS_ELAPSED"]
            
                totalESSItemsByYear[itemYear] = totalESSItemsByYear[itemYear] + 1
                meanESSbyYear[itemYear] = meanESSbyYear[itemYear] + row["ESS"]
                meanENSbyYear[itemYear] = meanENSbyYear[itemYear] + row["ENS"]
                meanENS_SIMILARITY_GAPbyYear[itemYear] = meanENS_SIMILARITY_GAPbyYear[itemYear] + row["ENS_SIMILARITY_GAP"]
                meanENS_ELAPSEDbyYear[itemYear] = meanENS_ELAPSEDbyYear[itemYear] + row["ENS_ELAPSED"]
                meanG_ENSbyYear[itemYear] = meanG_ENSbyYear[itemYear] + row["G_ENS"]
                meanG_ENS_SIMILARITY_GAPbyYear[itemYear] = meanG_ENS_SIMILARITY_GAPbyYear[itemYear] + row["G_ENS_SIMILARITY_GAP"]
                meanG_ENS_ELAPSEDbyYear[itemYear] = meanG_ENS_ELAPSEDbyYear[itemYear] + row["G_ENS_ELAPSED"]
            
                if row["ESS"] > 50:
                    totalPosItemsbyMonth[itemMonth] = totalPosItemsbyMonth[itemMonth] + 1
                    totalPosItemsbyYear[itemYear] = totalPosItemsbyYear[itemYear] + 1
                elif row["ESS"] < 50:
                    totalNegItemsbyMonth[itemMonth] = totalNegItemsbyMonth[itemMonth] + 1
                    totalNegItemsbyYear[itemYear] = totalNegItemsbyYear[itemYear] + 1

for i in range(12):
    meanESSbyMonth[i] = float(meanESSbyMonth[i]) / totalESSItemsByMonth[i]
    meanAESbyMonth[i] = float(meanAESbyMonth[i]) / totalItemsByMonth[i]
    meanRELEVANCEbyMonth[i] = float(meanRELEVANCEbyMonth[i]) / totalItemsByMonth[i]
    meanENSbyMonth[i] = float(meanENSbyMonth[i]) / totalESSItemsByMonth[i]
    meanENS_SIMILARITY_GAPbyMonth[i] = float(meanENS_SIMILARITY_GAPbyMonth[i]) / totalESSItemsByMonth[i]
    meanENS_ELAPSEDbyMonth[i] = float(meanENS_ELAPSEDbyMonth[i]) / totalESSItemsByMonth[i]
    meanG_ENSbyMonth[i] = float(meanG_ENSbyMonth[i]) / totalESSItemsByMonth[i]
    meanG_ENS_SIMILARITY_GAPbyMonth[i] = float(meanG_ENS_SIMILARITY_GAPbyMonth[i]) / totalESSItemsByMonth[i]
    meanG_ENS_ELAPSEDbyMonth[i] = float(meanG_ENS_ELAPSEDbyMonth[i]) / totalESSItemsByMonth[i]

for i in range(17):
    meanESSbyYear[i] = float(meanESSbyYear[i]) / totalESSItemsByYear[i]
    meanAESbyYear[i] = float(meanAESbyYear[i]) / totalItemsByYear[i]
    meanRELEVANCEbyYear[i] = float(meanRELEVANCEbyYear[i]) / totalItemsByYear[i]
    meanENSbyYear[i] = float(meanENSbyYear[i]) / totalESSItemsByYear[i]
    meanENS_SIMILARITY_GAPbyYear[i] = float(meanENS_SIMILARITY_GAPbyYear[i]) / totalESSItemsByYear[i]
    meanENS_ELAPSEDbyYear[i] = float(meanENS_ELAPSEDbyYear[i]) / totalESSItemsByYear[i]
    meanG_ENSbyYear[i] = float(meanG_ENSbyYear[i]) / totalESSItemsByYear[i]
    meanG_ENS_SIMILARITY_GAPbyYear[i] = float(meanG_ENS_SIMILARITY_GAPbyYear[i]) / totalESSItemsByYear[i]
    meanG_ENS_ELAPSEDbyYear[i] = float(meanG_ENS_ELAPSEDbyYear[i]) / totalESSItemsByYear[i]

print("Novel News Only: Sentiment/Relevance/Novelty Measures by Month (January = 0)")
print("Average Event Sentiment:", meanESSbyMonth)
print("Average Aggregate Event Sentiment:", meanAESbyMonth)
print("Average Relevance:", meanRELEVANCEbyMonth)
print("Average Novelty:", meanENSbyMonth)
print("Average Novelty Similarity Gap:", meanENS_SIMILARITY_GAPbyMonth)
print("Average Novelty Elapsed Time:", meanENS_ELAPSEDbyMonth)
print("Average Global Novelty:", meanG_ENSbyMonth)
print("Average Global Novelty Similarity Gap:", meanG_ENS_SIMILARITY_GAPbyMonth)
print("Average Global Novelty Elapsed Time:", meanG_ENS_ELAPSEDbyMonth)
print("Total Positive News Items:", totalPosItemsbyMonth)
print("Total Negative News Items:", totalNegItemsbyMonth)

print("Novel News Only: Sentiment/Relevance/Novelty Measures by Year (2000 = 0)")
print("Average Event Sentiment:", meanESSbyYear)
print("Average Aggregate Event Sentiment:", meanAESbyYear)
print("Average Relevance:", meanRELEVANCEbyYear)
print("Average Novelty:", meanENSbyYear)
print("Average Novelty Similarity Gap:", meanENS_SIMILARITY_GAPbyYear)
print("Average Novelty Elapsed Time:", meanENS_ELAPSEDbyYear)
print("Average Global Novelty:", meanG_ENSbyYear)
print("Average Global Novelty Similarity Gap:", meanG_ENS_SIMILARITY_GAPbyYear)
print("Average Global Novelty Elapsed Time:", meanG_ENS_ELAPSEDbyYear)
print("Total Positive News Items:", totalPosItemsbyYear)
print("Total Negative News Items:", totalNegItemsbyYear)

#6. ONLY MOST RELEVANT NEWS: Total number of news; average of different sentiment, and novelty measures; total number of positive, and negative news; per year, and per month (2000-2016, Jan-Dec).
relevanceLevel = 90 #indicates lowest level of relevance that will be considered

totalItemsByMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
totalESSItemsByMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanESSbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanAESbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanRELEVANCEbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanENSbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanENS_SIMILARITY_GAPbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanENS_ELAPSEDbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENSbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENS_SIMILARITY_GAPbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENS_ELAPSEDbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
totalPosItemsbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]
totalNegItemsbyMonth = [0,0,0,0,0,0,0,0,0,0,0,0]

totalItemsByYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
totalESSItemsByYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanESSbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanAESbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanRELEVANCEbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanENSbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanENS_SIMILARITY_GAPbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanENS_ELAPSEDbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENSbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENS_SIMILARITY_GAPbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
meanG_ENS_ELAPSEDbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
totalPosItemsbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
totalNegItemsbyYear = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]

for chunk in pd.read_csv(fileName,encoding='latin1',comment="TIMESTAMP_UTC",chunksize=10**6,engine="python",header=None,names=["TIMESTAMP_UTC","RP_ENTITY_ID","ENTITY_TYPE","ENTITY_NAME","POSITION_NAME","RP_POSITION_ID","COUNTRY_CODE","RELEVANCE","TOPIC","GROUP","TYPE","SUB_TYPE","PROPERTY","EVALUATION_METHOD","MATURITY","CATEGORY","ESS","AES","AEV","ENS","ENS_SIMILARITY_GAP","ENS_KEY","ENS_ELAPSED","G_ENS","G_ENS_SIMILARITY_GAP","G_ENS_KEY","G_ENS_ELAPSED","EVENT_SIMILARITY_KEY","NEWS_TYPE","SOURCE","RP_STORY_ID","RP_STORY_EVENT_INDEX","RP_STORY_EVENT_COUNT","PRODUCT","COMPANY","ISIN","CSS","NIP","PEQ","BEE","BMQ","BAM","BCA","BER","ANL_CHG","MCQ"]):
    for index, row in chunk.iterrows():
        if row["RELEVANCE"] >= relevanceLevel:
            date = parse(row["TIMESTAMP_UTC"])
            itemMonth = date.month - 1 #a number 1-12, starting with January as 0
            itemYear = date.year - 2000 #a number 0-16, starting with 2000 as 0
            totalItemsByMonth[itemMonth] = totalItemsByMonth[itemMonth] + 1
            meanAESbyMonth[itemMonth] = meanAESbyMonth[itemMonth] + row["AES"]
            meanRELEVANCEbyMonth[itemMonth] = meanRELEVANCEbyMonth[itemMonth] + row["RELEVANCE"]
        
            totalItemsByYear[itemYear] = totalItemsByYear[itemYear] + 1
            meanAESbyYear[itemYear] = meanAESbyYear[itemYear] + row["AES"]
            meanRELEVANCEbyYear[itemYear] = meanRELEVANCEbyYear[itemYear] + row["RELEVANCE"]
        
            if row["ESS"] >= 0 or row["ESS"] < 0:
                totalESSItemsByMonth[itemMonth] = totalESSItemsByMonth[itemMonth] + 1
                meanESSbyMonth[itemMonth] = meanESSbyMonth[itemMonth] + row["ESS"]
                meanENSbyMonth[itemMonth] = meanENSbyMonth[itemMonth] + row["ENS"]
                meanENS_SIMILARITY_GAPbyMonth[itemMonth] = meanENS_SIMILARITY_GAPbyMonth[itemMonth] + row["ENS_SIMILARITY_GAP"]
                meanENS_ELAPSEDbyMonth[itemMonth] = meanENS_ELAPSEDbyMonth[itemMonth] + row["ENS_ELAPSED"]
                meanG_ENSbyMonth[itemMonth] = meanG_ENSbyMonth[itemMonth] + row["G_ENS"]
                meanG_ENS_SIMILARITY_GAPbyMonth[itemMonth] = meanG_ENS_SIMILARITY_GAPbyMonth[itemMonth] + row["G_ENS_SIMILARITY_GAP"]
                meanG_ENS_ELAPSEDbyMonth[itemMonth] = meanG_ENS_ELAPSEDbyMonth[itemMonth] + row["G_ENS_ELAPSED"]
            
                totalESSItemsByYear[itemYear] = totalESSItemsByYear[itemYear] + 1
                meanESSbyYear[itemYear] = meanESSbyYear[itemYear] + row["ESS"]
                meanENSbyYear[itemYear] = meanENSbyYear[itemYear] + row["ENS"]
                meanENS_SIMILARITY_GAPbyYear[itemYear] = meanENS_SIMILARITY_GAPbyYear[itemYear] + row["ENS_SIMILARITY_GAP"]
                meanENS_ELAPSEDbyYear[itemYear] = meanENS_ELAPSEDbyYear[itemYear] + row["ENS_ELAPSED"]
                meanG_ENSbyYear[itemYear] = meanG_ENSbyYear[itemYear] + row["G_ENS"]
                meanG_ENS_SIMILARITY_GAPbyYear[itemYear] = meanG_ENS_SIMILARITY_GAPbyYear[itemYear] + row["G_ENS_SIMILARITY_GAP"]
                meanG_ENS_ELAPSEDbyYear[itemYear] = meanG_ENS_ELAPSEDbyYear[itemYear] + row["G_ENS_ELAPSED"]
            
                if row["ESS"] > 50:
                    totalPosItemsbyMonth[itemMonth] = totalPosItemsbyMonth[itemMonth] + 1
                    totalPosItemsbyYear[itemYear] = totalPosItemsbyYear[itemYear] + 1
                elif row["ESS"] < 50:
                    totalNegItemsbyMonth[itemMonth] = totalNegItemsbyMonth[itemMonth] + 1
                    totalNegItemsbyYear[itemYear] = totalNegItemsbyYear[itemYear] + 1

for i in range(12):
    meanESSbyMonth[i] = float(meanESSbyMonth[i]) / totalESSItemsByMonth[i]
    meanAESbyMonth[i] = float(meanAESbyMonth[i]) / totalItemsByMonth[i]
    meanRELEVANCEbyMonth[i] = float(meanRELEVANCEbyMonth[i]) / totalItemsByMonth[i]
    meanENSbyMonth[i] = float(meanENSbyMonth[i]) / totalESSItemsByMonth[i]
    meanENS_SIMILARITY_GAPbyMonth[i] = float(meanENS_SIMILARITY_GAPbyMonth[i]) / totalESSItemsByMonth[i]
    meanENS_ELAPSEDbyMonth[i] = float(meanENS_ELAPSEDbyMonth[i]) / totalESSItemsByMonth[i]
    meanG_ENSbyMonth[i] = float(meanG_ENSbyMonth[i]) / totalESSItemsByMonth[i]
    meanG_ENS_SIMILARITY_GAPbyMonth[i] = float(meanG_ENS_SIMILARITY_GAPbyMonth[i]) / totalESSItemsByMonth[i]
    meanG_ENS_ELAPSEDbyMonth[i] = float(meanG_ENS_ELAPSEDbyMonth[i]) / totalESSItemsByMonth[i]

for i in range(17):
    meanESSbyYear[i] = float(meanESSbyYear[i]) / totalESSItemsByYear[i]
    meanAESbyYear[i] = float(meanAESbyYear[i]) / totalItemsByYear[i]
    meanRELEVANCEbyYear[i] = float(meanRELEVANCEbyYear[i]) / totalItemsByYear[i]
    meanENSbyYear[i] = float(meanENSbyYear[i]) / totalESSItemsByYear[i]
    meanENS_SIMILARITY_GAPbyYear[i] = float(meanENS_SIMILARITY_GAPbyYear[i]) / totalESSItemsByYear[i]
    meanENS_ELAPSEDbyYear[i] = float(meanENS_ELAPSEDbyYear[i]) / totalESSItemsByYear[i]
    meanG_ENSbyYear[i] = float(meanG_ENSbyYear[i]) / totalESSItemsByYear[i]
    meanG_ENS_SIMILARITY_GAPbyYear[i] = float(meanG_ENS_SIMILARITY_GAPbyYear[i]) / totalESSItemsByYear[i]
    meanG_ENS_ELAPSEDbyYear[i] = float(meanG_ENS_ELAPSEDbyYear[i]) / totalESSItemsByYear[i]

print("Relevant News Only: Sentiment/Relevance/Novelty Measures by Month (January = 0)")
print("Average Event Sentiment:", meanESSbyMonth)
print("Average Aggregate Event Sentiment:", meanAESbyMonth)
print("Average Relevance:", meanRELEVANCEbyMonth)
print("Average Novelty:", meanENSbyMonth)
print("Average Novelty Similarity Gap:", meanENS_SIMILARITY_GAPbyMonth)
print("Average Novelty Elapsed Time:", meanENS_ELAPSEDbyMonth)
print("Average Global Novelty:", meanG_ENSbyMonth)
print("Average Global Novelty Similarity Gap:", meanG_ENS_SIMILARITY_GAPbyMonth)
print("Average Global Novelty Elapsed Time:", meanG_ENS_ELAPSEDbyMonth)
print("Total Positive News Items:", totalPosItemsbyMonth)
print("Total Negative News Items:", totalNegItemsbyMonth)

print("Relevant News Only: Sentiment/Relevance/Novelty Measures by Year (2000 = 0)")
print("Average Event Sentiment:", meanESSbyYear)
print("Average Aggregate Event Sentiment:", meanAESbyYear)
print("Average Relevance:", meanRELEVANCEbyYear)
print("Average Novelty:", meanENSbyYear)
print("Average Novelty Similarity Gap:", meanENS_SIMILARITY_GAPbyYear)
print("Average Novelty Elapsed Time:", meanENS_ELAPSEDbyYear)
print("Average Global Novelty:", meanG_ENSbyYear)
print("Average Global Novelty Similarity Gap:", meanG_ENS_SIMILARITY_GAPbyYear)
print("Average Global Novelty Elapsed Time:", meanG_ENS_ELAPSEDbyYear)
print("Total Positive News Items:", totalPosItemsbyYear)
print("Total Negative News Items:", totalNegItemsbyYear)