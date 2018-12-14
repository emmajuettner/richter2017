#Initial Tasks, using January 2000 data
import pandas as pd
#from _datetime import date
#fileName = "C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/RPNA_DJEdition_2000_4.0-Equities/RPNA_DJEdition_2000_4.0-Equities/2000-01-equities.csv"
fileName = "D:/Richter Project/2000-01-equities.csv"
#data = pd.read_csv(fileName,encoding='latin1',low_memory=False)
myList = []
data = pd.read_csv(fileName,comment="TIMESTAMP_UTC",engine="python",encoding='latin1',header=None,names=["TIMESTAMP_UTC","RP_ENTITY_ID","ENTITY_TYPE","ENTITY_NAME","POSITION_NAME","RP_POSITION_ID","COUNTRY_CODE","RELEVANCE","TOPIC","GROUP","TYPE","SUB_TYPE","PROPERTY","EVALUATION_METHOD","MATURITY","CATEGORY","ESS","AES","AEV","ENS","ENS_SIMILARITY_GAP","ENS_KEY","ENS_ELAPSED","G_ENS","G_ENS_SIMILARITY_GAP","G_ENS_KEY","G_ENS_ELAPSED","EVENT_SIMILARITY_KEY","NEWS_TYPE","SOURCE","RP_STORY_ID","RP_STORY_EVENT_INDEX","RP_STORY_EVENT_COUNT","PRODUCT","COMPANY","ISIN","CSS","NIP","PEQ","BEE","BMQ","BAM","BCA","BER","ANL_CHG","MCQ"])
#memory_map=True,

#1. Find the total number of news items.
print("Total number of news items:", data["TIMESTAMP_UTC"].count())

#2. Find the overall average of different sentiment, relevance, and novelty measures (consult RavenPack USER GUIDE).
print("Average Event Sentiment:", data["ESS"].mean())
print("Average Aggregate Event Sentiment:", data["AES"].mean())
print("Average Relevance:", data["RELEVANCE"].mean())
print("Average Novelty:", data["ENS"].mean())
print("Average Novelty Similarity Gap:", data["ENS_SIMILARITY_GAP"].mean())
print("Average Novelty Elapsed Time:", data["ENS_ELAPSED"].mean())
print("Average Global Novelty:", data["G_ENS"].mean())
print("Average Global Novelty Similarity Gap:", data["G_ENS_SIMILARITY_GAP"].mean())
print("Average Global Novelty Elapsed Time:", data["G_ENS_ELAPSED"].mean())
'''
#3. Find the number of news items per stock (use RP_ENTITY_ID to identify stocks).
stockGrouped = data.groupby("RP_ENTITY_ID").count()["TIMESTAMP_UTC"]
print(stockGrouped)

#4. Find the number of news items per country (use COUNTRY_CODE to identify stocks).
countryGrouped = data.groupby("COUNTRY_CODE").count()["TIMESTAMP_UTC"]
print(countryGrouped)

#5. Find the number of news items per TOPIC, GROUP, TYPE, CATEGORY, NEWS TYPE, separately.
topicGrouped = data.groupby("TOPIC").count()["TIMESTAMP_UTC"]
print(topicGrouped)

groupGrouped = data.groupby("GROUP").count()["TIMESTAMP_UTC"]
print(groupGrouped)

typeGrouped = data.groupby("TYPE").count()["TIMESTAMP_UTC"]
print(typeGrouped)

categoryGrouped = data.groupby("CATEGORY").count()["TIMESTAMP_UTC"]
print(categoryGrouped)

newsTypeGrouped = data.groupby("NEWS_TYPE").count()["TIMESTAMP_UTC"]
print(newsTypeGrouped)

#6. Find the average of different sentiment, relevance, and novelty measures for each stock.


#7. Find the average of different sentiment, relevance, and novelty measures per stock (use RP_ENTITY_ID to identify stocks).
print("Average Event Sentiment For Each Stock:\n",data.groupby("RP_ENTITY_ID")["ESS"].mean(),"\n")
print("Average Aggregate Event Sentiment For Each Stock:\n",data.groupby("RP_ENTITY_ID")["AES"].mean(),"\n")
print("Average Relevance For Each Stock:\n",data.groupby("RP_ENTITY_ID")["RELEVANCE"].mean(),"\n")
print("Average Novelty For Each Stock:\n",data.groupby("RP_ENTITY_ID")["ENS"].mean(),"\n")
print("Average Novelty Similarity Gap For Each Stock:\n",data.groupby("RP_ENTITY_ID")["ENS_SIMILARITY_GAP"].mean(),"\n")
print("Average Novelty Elapsed Time For Each Stock:\n",data.groupby("RP_ENTITY_ID")["ENS_ELAPSED"].mean(),"\n")
print("Average Global Novelty For Each Stock:\n",data.groupby("RP_ENTITY_ID")["G_ENS"].mean(),"\n")
print("Average Global Novelty Similarity Gap For Each Stock:\n",data.groupby("RP_ENTITY_ID")["G_ENS_SIMILARITY_GAP"].mean(),"\n")
print("Average Global Novelty Elapsed Time For Each Stock:\n",data.groupby("RP_ENTITY_ID")["G_ENS_ELAPSED"].mean(),"\n")

#8. Find the average of different sentiment, relevance, and novelty measures per country (use COUNTRY_CODE to identify stocks).
print("Average Event Sentiment For Each Country:\n",data.groupby("COUNTRY_CODE")["ESS"].mean(),"\n")
print("Average Aggregate Event Sentiment For Each Country:\n",data.groupby("COUNTRY_CODE")["AES"].mean(),"\n")
print("Average Relevance For Each Country:\n",data.groupby("COUNTRY_CODE")["RELEVANCE"].mean(),"\n")
print("Average Novelty For Each Country:\n",data.groupby("COUNTRY_CODE")["ENS"].mean(),"\n")
print("Average Novelty Similarity Gap For Each Country:\n",data.groupby("COUNTRY_CODE")["ENS_SIMILARITY_GAP"].mean(),"\n")
print("Average Novelty Elapsed Time For Each Country:\n",data.groupby("COUNTRY_CODE")["ENS_ELAPSED"].mean(),"\n")
print("Average Global Novelty For Each Country:\n",data.groupby("COUNTRY_CODE")["G_ENS"].mean(),"\n")
print("Average Global Novelty Similarity Gap For Each Country:\n",data.groupby("COUNTRY_CODE")["G_ENS_SIMILARITY_GAP"].mean(),"\n")
print("Average Global Novelty Elapsed Time For Each Country:\n",data.groupby("COUNTRY_CODE")["G_ENS_ELAPSED"].mean(),"\n")

#9. Find the average of different sentiment, relevance, and novelty measures per TOPIC, GROUP, TYPE, CATEGORY, NEWS TYPE, separately.
print("Average Event Sentiment For Each Topic:\n",data.groupby("TOPIC")["ESS"].mean(),"\n")
print("Average Aggregate Event Sentiment For Each Topic:\n",data.groupby("TOPIC")["AES"].mean(),"\n")
print("Average Relevance For Each Topic:\n",data.groupby("TOPIC")["RELEVANCE"].mean(),"\n")
print("Average Novelty For Each Topic:\n",data.groupby("TOPIC")["ENS"].mean(),"\n")
print("Average Novelty Similarity Gap For Each Topic:\n",data.groupby("TOPIC")["ENS_SIMILARITY_GAP"].mean(),"\n")
print("Average Novelty Elapsed Time For Each Topic:\n",data.groupby("TOPIC")["ENS_ELAPSED"].mean(),"\n")
print("Average Global Novelty For Each Topic:\n",data.groupby("TOPIC")["G_ENS"].mean(),"\n")
print("Average Global Novelty Similarity Gap For Each Topic:\n",data.groupby("TOPIC")["G_ENS_SIMILARITY_GAP"].mean(),"\n")
print("Average Global Novelty Elapsed Time For Each Topic:\n",data.groupby("TOPIC")["G_ENS_ELAPSED"].mean(),"\n")

print("Average Event Sentiment For Each Group:\n",data.groupby("GROUP")["ESS"].mean(),"\n")
print("Average Aggregate Event Sentiment For Each Group:\n",data.groupby("GROUP")["AES"].mean(),"\n")
print("Average Relevance For Each Group:\n",data.groupby("GROUP")["RELEVANCE"].mean(),"\n")
print("Average Novelty For Each Group:\n",data.groupby("GROUP")["ENS"].mean(),"\n")
print("Average Novelty Similarity Gap For Each Group:\n",data.groupby("GROUP")["ENS_SIMILARITY_GAP"].mean(),"\n")
print("Average Novelty Elapsed Time For Each Group:\n",data.groupby("GROUP")["ENS_ELAPSED"].mean(),"\n")
print("Average Global Novelty For Each Group:\n",data.groupby("GROUP")["G_ENS"].mean(),"\n")
print("Average Global Novelty Similarity Gap For Each Group:\n",data.groupby("GROUP")["G_ENS_SIMILARITY_GAP"].mean(),"\n")
print("Average Global Novelty Elapsed Time For Each Group:\n",data.groupby("GROUP")["G_ENS_ELAPSED"].mean(),"\n")

print("Average Event Sentiment For Each Type:\n",data.groupby("TYPE")["ESS"].mean(),"\n")
print("Average Aggregate Event Sentiment For Each Type:\n",data.groupby("TYPE")["AES"].mean(),"\n")
print("Average Relevance For Each Type:\n",data.groupby("TYPE")["RELEVANCE"].mean(),"\n")
print("Average Novelty For Each Type:\n",data.groupby("TYPE")["ENS"].mean(),"\n")
print("Average Novelty Similarity Gap For Each Type:\n",data.groupby("TYPE")["ENS_SIMILARITY_GAP"].mean(),"\n")
print("Average Novelty Elapsed Time For Each Type:\n",data.groupby("TYPE")["ENS_ELAPSED"].mean(),"\n")
print("Average Global Novelty For Each Type:\n",data.groupby("TYPE")["G_ENS"].mean(),"\n")
print("Average Global Novelty Similarity Gap For Each Type:\n",data.groupby("TYPE")["G_ENS_SIMILARITY_GAP"].mean(),"\n")
print("Average Global Novelty Elapsed Time For Each Type:\n",data.groupby("TYPE")["G_ENS_ELAPSED"].mean(),"\n")

print("Average Event Sentiment For Each Category:\n",data.groupby("CATEGORY")["ESS"].mean(),"\n")
print("Average Aggregate Event Sentiment For Each Category:\n",data.groupby("CATEGORY")["AES"].mean(),"\n")
print("Average Relevance For Each Category:\n",data.groupby("CATEGORY")["RELEVANCE"].mean(),"\n")
print("Average Novelty For Each Category:\n",data.groupby("CATEGORY")["ENS"].mean(),"\n")
print("Average Novelty Similarity Gap For Each Category:\n",data.groupby("CATEGORY")["ENS_SIMILARITY_GAP"].mean(),"\n")
print("Average Novelty Elapsed Time For Each Category:\n",data.groupby("CATEGORY")["ENS_ELAPSED"].mean(),"\n")
print("Average Global Novelty For Each Category:\n",data.groupby("CATEGORY")["G_ENS"].mean(),"\n")
print("Average Global Novelty Similarity Gap For Each Category:\n",data.groupby("CATEGORY")["G_ENS_SIMILARITY_GAP"].mean(),"\n")
print("Average Global Novelty Elapsed Time For Each Category:\n",data.groupby("CATEGORY")["G_ENS_ELAPSED"].mean(),"\n")

print("Average Event Sentiment For Each News Type:\n",data.groupby("NEWS_TYPE")["ESS"].mean(),"\n")
print("Average Aggregate Event Sentiment For Each News Type:\n",data.groupby("NEWS_TYPE")["AES"].mean(),"\n")
print("Average Relevance For Each News Type:\n",data.groupby("NEWS_TYPE")["RELEVANCE"].mean(),"\n")
print("Average Novelty For Each News Type:\n",data.groupby("NEWS_TYPE")["ENS"].mean(),"\n")
print("Average Novelty Similarity Gap For Each News Type:\n",data.groupby("NEWS_TYPE")["ENS_SIMILARITY_GAP"].mean(),"\n")
print("Average Novelty Elapsed Time For Each News Type:\n",data.groupby("NEWS_TYPE")["ENS_ELAPSED"].mean(),"\n")
print("Average Global Novelty For Each News Type:\n",data.groupby("NEWS_TYPE")["G_ENS"].mean(),"\n")
print("Average Global Novelty Similarity Gap For Each News Type:\n",data.groupby("NEWS_TYPE")["G_ENS_SIMILARITY_GAP"].mean(),"\n")
print("Average Global Novelty Elapsed Time For Each News Type:\n",data.groupby("NEWS_TYPE")["G_ENS_ELAPSED"].mean(),"\n")

#10. Find the total number of positive, and negative news.
totalPosItems = 0
totalNegItems = 0
for num in data["ESS"]:
    if num>50:
        totalPosItems = totalPosItems + 1
    elif num<50:
        totalNegItems = totalNegItems + 1
print("Total Positive News Items:", totalPosItems)
print("Total Negative News Items:", totalNegItems)

#11. Find the total number of positive, and negative news per stock.
posNewsPerStock = {}
negNewsPerStock = {}
for num in range(data.ix[:,["RP_ENTITY_ID"]].__len__()):
    numID = data.at[num,"RP_ENTITY_ID"]
    numESS = data.at[num,"ESS"]
    if numESS <= 100 and numESS > 50:
        if numID in posNewsPerStock:
            posNewsPerStock[numID] = posNewsPerStock[numID] + 1
        else:
            posNewsPerStock[numID] = 1
    elif numESS < 50 and numESS >= 0:
        if numID in negNewsPerStock:
            negNewsPerStock[numID] = negNewsPerStock[numID] + 1
        else:
            negNewsPerStock[numID] = 1
posNewsPerStockDF = pd.DataFrame.from_dict(posNewsPerStock,orient="index") #convert dictionary to pandas DataFrame
posNewsPerStockDF.reset_index(inplace=True)
posNewsPerStockDF.columns = ["Stock ID","Positive News Items"] #add headers
print(posNewsPerStockDF)
negNewsPerStockDF = pd.DataFrame.from_dict(negNewsPerStock,orient="index") #convert dictionary to pandas DataFrame
negNewsPerStockDF.reset_index(inplace=True)
negNewsPerStockDF.columns = ["Stock ID","Negative News Items"] #add headers
print(negNewsPerStockDF)

#12. Find the number of positive, and negative news per country (use COUNTRY_CODE to identify stocks).
posNewsPerCountry = {}
negNewsPerCountry = {}
for num in range(data.ix[:,["COUNTRY_CODE"]].__len__()):
    numID = data.at[num,"COUNTRY_CODE"]
    numESS = data.at[num,"ESS"]
    if numESS <= 100 and numESS > 50:
        if numID in posNewsPerCountry:
            posNewsPerCountry[numID] = posNewsPerCountry[numID] + 1
        else:
            posNewsPerCountry[numID] = 1
    elif numESS < 50 and numESS >= 0:
        if numID in negNewsPerCountry:
            negNewsPerCountry[numID] = negNewsPerCountry[numID] + 1
        else:
            negNewsPerCountry[numID] = 1
posNewsPerCountryDF = pd.DataFrame.from_dict(posNewsPerCountry,orient="index") #convert dictionary to pandas DataFrame
posNewsPerCountryDF.reset_index(inplace=True)
posNewsPerCountryDF.columns = ["Country ID","Positive News Items"] #add headers
print(posNewsPerCountryDF)
negNewsPerCountryDF = pd.DataFrame.from_dict(negNewsPerCountry,orient="index") #convert dictionary to pandas DataFrame
negNewsPerCountryDF.reset_index(inplace=True)
negNewsPerCountryDF.columns = ["Country ID","Negative News Items"] #add headers
print(negNewsPerCountryDF)

#13. Find the number of positive, and negative news per TOPIC, GROUP, TYPE, CATEGORY, NEWS TYPE, separately.
posNewsPerTopic = {}
negNewsPerTopic = {}
for num in range(data.ix[:,["TOPIC"]].__len__()):
    numID = data.at[num,"TOPIC"]
    numESS = data.at[num,"ESS"]
    if numESS <= 100 and numESS > 50:
        if numID in posNewsPerTopic:
            posNewsPerTopic[numID] = posNewsPerTopic[numID] + 1
        else:
            posNewsPerTopic[numID] = 1
    elif numESS < 50 and numESS >= 0:
        if numID in negNewsPerTopic:
            negNewsPerTopic[numID] = negNewsPerTopic[numID] + 1
        else:
            negNewsPerTopic[numID] = 1
posNewsPerTopicDF = pd.DataFrame.from_dict(posNewsPerTopic,orient="index") #convert dictionary to pandas DataFrame
posNewsPerTopicDF.reset_index(inplace=True)
posNewsPerTopicDF.columns = ["Topic","Positive News Items"] #add headers
print(posNewsPerTopicDF)
negNewsPerTopicDF = pd.DataFrame.from_dict(negNewsPerTopic,orient="index") #convert dictionary to pandas DataFrame
negNewsPerTopicDF.reset_index(inplace=True)
negNewsPerTopicDF.columns = ["Topic","Negative News Items"] #add headers
print(negNewsPerTopicDF)

posNewsPerGroup = {}
negNewsPerGroup = {}
for num in range(data.ix[:,["GROUP"]].__len__()):
    numID = data.at[num,"GROUP"]
    numESS = data.at[num,"ESS"]
    if numESS <= 100 and numESS > 50:
        if numID in posNewsPerGroup:
            posNewsPerGroup[numID] = posNewsPerGroup[numID] + 1
        else:
            posNewsPerGroup[numID] = 1
    elif numESS < 50 and numESS >= 0:
        if numID in negNewsPerGroup:
            negNewsPerGroup[numID] = negNewsPerGroup[numID] + 1
        else:
            negNewsPerGroup[numID] = 1
posNewsPerGroupDF = pd.DataFrame.from_dict(posNewsPerGroup,orient="index") #convert dictionary to pandas DataFrame
posNewsPerGroupDF.reset_index(inplace=True)
posNewsPerGroupDF.columns = ["Group","Positive News Items"] #add headers
print(posNewsPerGroupDF)
negNewsPerGroupDF = pd.DataFrame.from_dict(negNewsPerGroup,orient="index") #convert dictionary to pandas DataFrame
negNewsPerGroupDF.reset_index(inplace=True)
negNewsPerGroupDF.columns = ["Group","Negative News Items"] #add headers
print(negNewsPerGroupDF)

posNewsPerType = {}
negNewsPerType = {}
for num in range(data.ix[:,["TYPE"]].__len__()):
    numID = data.at[num,"TYPE"]
    numESS = data.at[num,"ESS"]
    if numESS <= 100 and numESS > 50:
        if numID in posNewsPerType:
            posNewsPerType[numID] = posNewsPerType[numID] + 1
        else:
            posNewsPerType[numID] = 1
    elif numESS < 50 and numESS >= 0:
        if numID in negNewsPerType:
            negNewsPerType[numID] = negNewsPerType[numID] + 1
        else:
            negNewsPerType[numID] = 1
posNewsPerTypeDF = pd.DataFrame.from_dict(posNewsPerType,orient="index") #convert dictionary to pandas DataFrame
posNewsPerTypeDF.reset_index(inplace=True)
posNewsPerTypeDF.columns = ["Type","Positive News Items"] #add headers
print(posNewsPerTypeDF)
negNewsPerTypeDF = pd.DataFrame.from_dict(negNewsPerType,orient="index") #convert dictionary to pandas DataFrame
negNewsPerTypeDF.reset_index(inplace=True)
negNewsPerTypeDF.columns = ["Type","Negative News Items"] #add headers
print(negNewsPerTypeDF)

posNewsPerCategory = {}
negNewsPerCategory = {}
for num in range(data.ix[:,["CATEGORY"]].__len__()):
    numID = data.at[num,"CATEGORY"]
    numESS = data.at[num,"ESS"]
    if numESS <= 100 and numESS > 50:
        if numID in posNewsPerCategory:
            posNewsPerCategory[numID] = posNewsPerCategory[numID] + 1
        else:
            posNewsPerCategory[numID] = 1
    elif numESS < 50 and numESS >= 0:
        if numID in negNewsPerCategory:
            negNewsPerCategory[numID] = negNewsPerCategory[numID] + 1
        else:
            negNewsPerCategory[numID] = 1
posNewsPerCategoryDF = pd.DataFrame.from_dict(posNewsPerCategory,orient="index") #convert dictionary to pandas DataFrame
posNewsPerCategoryDF.reset_index(inplace=True)
posNewsPerCategoryDF.columns = ["Category","Positive News Items"] #add headers
print(posNewsPerCategoryDF)
negNewsPerCategoryDF = pd.DataFrame.from_dict(negNewsPerCategory,orient="index") #convert dictionary to pandas DataFrame
negNewsPerCategoryDF.reset_index(inplace=True)
negNewsPerCategoryDF.columns = ["Category","Negative News Items"] #add headers
print(negNewsPerCategoryDF)

posNewsPerNewsType = {}
negNewsPerNewsType = {}
for num in range(data.ix[:,["NEWS_TYPE"]].__len__()):
    numID = data.at[num,"NEWS_TYPE"]
    numESS = data.at[num,"ESS"]
    if numESS <= 100 and numESS > 50:
        if numID in posNewsPerNewsType:
            posNewsPerNewsType[numID] = posNewsPerNewsType[numID] + 1
        else:
            posNewsPerNewsType[numID] = 1
    elif numESS < 50 and numESS >= 0:
        if numID in negNewsPerNewsType:
            negNewsPerNewsType[numID] = negNewsPerNewsType[numID] + 1
        else:
            negNewsPerNewsType[numID] = 1
posNewsPerNewsTypeDF = pd.DataFrame.from_dict(posNewsPerNewsType,orient="index") #convert dictionary to pandas DataFrame
posNewsPerNewsTypeDF.reset_index(inplace=True)
posNewsPerNewsTypeDF.columns = ["News Type","Positive News Items"] #add headers
print(posNewsPerNewsTypeDF)
negNewsPerNewsTypeDF = pd.DataFrame.from_dict(negNewsPerNewsType,orient="index") #convert dictionary to pandas DataFrame
negNewsPerNewsTypeDF.reset_index(inplace=True)
negNewsPerNewsTypeDF.columns = ["News Type","Negative News Items"] #add headers
print(negNewsPerNewsTypeDF)

#Later (Using date and time functions)
from dateutil.parser import parse

#14. Find the total number of news items by days (Mon-Sun).
monCount = 0
tueCount = 0
wedCount = 0
thuCount = 0
friCount = 0
satCount = 0
sunCount = 0
for timeStamp in data["TIMESTAMP_UTC"]:
    date = parse(timeStamp)
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
print("Total Monday News:", monCount)
print("Total Tuesday News:", tueCount)
print("Total Wednesday News:", wedCount)
print("Total Thursday News:", thuCount)
print("Total Friday News:", friCount)
print("Total Saturday News:", satCount)
print("Total Sunday News:", sunCount)

#15. Find the total number of news items by weekdays vs weekends (only 2 categories).
weekdayCount = 0
weekendCount = 0
for timeStamp in data["TIMESTAMP_UTC"]:
    date = parse(timeStamp)
    dayOfWeek = date.weekday()
    if dayOfWeek >= 0 and dayOfWeek <= 4:
        weekdayCount = weekdayCount + 1
    elif dayOfWeek > 4 and dayOfWeek <= 6:
        weekendCount = weekendCount + 1
print("Total Weekday News:", weekdayCount)
print("Total Weekend News:", weekendCount)

#16. Find the total number of news items published before and after 3:30 pm on weekdays.
import datetime
before330Count = 0
after330Count = 0
time1 = datetime.time(hour=15,minute=30)
for timeStamp in data["TIMESTAMP_UTC"]:
    date = parse(timeStamp)
    timeOfDay = date.time()
    dayOfWeek = date.weekday()
    if dayOfWeek >= 0 and dayOfWeek <= 4:
        if timeOfDay < time1:
            before330Count = before330Count + 1
        elif timeOfDay > time1:
            after330Count = after330Count + 1
print("Total Weekday News Before 3:30 pm:", before330Count)
print("Total Weekday News After 3:30 pm:", after330Count)
'''