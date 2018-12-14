import datetime as dt
from dateutil.parser import parse
import pandas as pd

fileName = "C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/five years data/five_years_data.csv"
chunkCount = -1
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
print("done initializing variables")
for chunk in pd.read_csv(fileName,encoding='latin1',comment="TIMESTAMP_UTC",chunksize=10**6,engine="python",header=None,names=["TIMESTAMP_UTC","RP_ENTITY_ID","ENTITY_TYPE","ENTITY_NAME","POSITION_NAME","RP_POSITION_ID","COUNTRY_CODE","RELEVANCE","TOPIC","GROUP","TYPE","SUB_TYPE","PROPERTY","EVALUATION_METHOD","MATURITY","CATEGORY","ESS","AES","AEV","ENS","ENS_SIMILARITY_GAP","ENS_KEY","ENS_ELAPSED","G_ENS","G_ENS_SIMILARITY_GAP","G_ENS_KEY","G_ENS_ELAPSED","EVENT_SIMILARITY_KEY","NEWS_TYPE","SOURCE","RP_STORY_ID","RP_STORY_EVENT_INDEX","RP_STORY_EVENT_COUNT","PRODUCT","COMPANY","ISIN","CSS","NIP","PEQ","BEE","BMQ","BAM","BCA","BER","ANL_CHG","MCQ"]):
    chunkCount = chunkCount + 1
    print("starting chunk",chunkCount)
    for index, row in chunk.iterrows():
        #14: keep count of total news items for each day of the week
        try:
            date = parse(row["TIMESTAMP_UTC"])
            dayOfWeek = date.weekday()
            timeOfDay = date.time()
            if dayOfWeek==0:
                monCount = monCount + 1
                weekdayCount = weekdayCount + 1
                if timeOfDay < time1:
                    before330Count = before330Count + 1
                elif timeOfDay > time1:
                    after330Count = after330Count + 1
            elif dayOfWeek==1:
                tueCount = tueCount + 1
                weekdayCount = weekdayCount + 1
                if timeOfDay < time1:
                    before330Count = before330Count + 1
                elif timeOfDay > time1:
                    after330Count = after330Count + 1
            elif dayOfWeek==2:
                wedCount = wedCount + 1
                weekdayCount = weekdayCount + 1
                if timeOfDay < time1:
                    before330Count = before330Count + 1
                elif timeOfDay > time1:
                    after330Count = after330Count + 1
            elif dayOfWeek==3:
                thuCount = thuCount + 1
                weekdayCount = weekdayCount + 1
                if timeOfDay < time1:
                    before330Count = before330Count + 1
                elif timeOfDay > time1:
                    after330Count = after330Count + 1
            elif dayOfWeek==4:
                friCount = friCount + 1
                weekdayCount = weekdayCount + 1
                if timeOfDay < time1:
                    before330Count = before330Count + 1
                elif timeOfDay > time1:
                    after330Count = after330Count + 1
            elif dayOfWeek==5:
                satCount = satCount + 1
                weekendCount = weekendCount + 1
            elif dayOfWeek==6:
                sunCount = sunCount + 1
                weekendCount = weekendCount + 1
        except:
            print("exception thrown for row", row)
    print("finishing chunk", chunkCount)
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
