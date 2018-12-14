import pandas as pd
fileName = "D:/Richter Project/2000-01-equities.csv"

target = pd.DataFrame(columns=["COMP","ORGA","CURR","CMDT","PLCE","ORGT","POSI"])
compNum = 0
orgaNum = 0
currNum = 0
cmdtNum = 0
plceNum = 0
orgtNum = 0
posiNum = 0

for chunk in pd.read_csv(fileName,encoding='latin1',comment="TIMESTAMP_UTC",chunksize=10**6,engine="python",header=None,
                         names=["TIMESTAMP_UTC","RP_ENTITY_ID","ENTITY_TYPE","ENTITY_NAME","POSITION_NAME",
                                "RP_POSITION_ID","COUNTRY_CODE","RELEVANCE","TOPIC","GROUP","TYPE","SUB_TYPE",
                                "PROPERTY","EVALUATION_METHOD","MATURITY","CATEGORY","ESS","AES","AEV","ENS",
                                "ENS_SIMILARITY_GAP","ENS_KEY","ENS_ELAPSED","G_ENS","G_ENS_SIMILARITY_GAP",
                                "G_ENS_KEY","G_ENS_ELAPSED","EVENT_SIMILARITY_KEY","NEWS_TYPE","SOURCE",
                                "RP_STORY_ID","RP_STORY_EVENT_INDEX","RP_STORY_EVENT_COUNT","PRODUCT","COMPANY",
                                "ISIN","CSS","NIP","PEQ","BEE","BMQ","BAM","BCA","BER","ANL_CHG","MCQ"]):
    for index, row in chunk.iterrows():
        if row["ENTITY_TYPE"] == "COMP": #if it's a COMP
            if row["ENTITY_NAME"] not in target["COMP"]: #and if it's not in the target already
                target.at[compNum,"COMP"] = row["ENTITY_NAME"]
                #target.iloc(compNum)["COMP"] = row["ENTITY_NAME"] #put it in the COMP column
                compNum = compNum + 1 #and increase the number of COMPs
        if row["ENTITY_TYPE"] == "ORGA": 
            if row["ENTITY_NAME"] not in target["ORGA"]: 
                target.iloc(orgaNum)["ORGA"] = row["ENTITY_NAME"] 
                orgaNum = orgaNum + 1 
        if row["ENTITY_TYPE"] == "CURR": 
            if row["ENTITY_NAME"] not in target["CURR"]: 
                target.iloc(currNum)["CURR"] = row["ENTITY_NAME"] 
                currNum = currNum + 1 
        if row["ENTITY_TYPE"] == "CMDT": 
            if row["ENTITY_NAME"] not in target["CMDT"]: 
                target.iloc(cmdtNum)["CMDT"] = row["ENTITY_NAME"] 
                cmdtNum = cmdtNum + 1 
        if row["ENTITY_TYPE"] == "PLCE": 
            if row["ENTITY_NAME"] not in target["PLCE"]: 
                target.iloc(plceNum)["PLCE"] = row["ENTITY_NAME"] 
                plceNum = plceNum + 1 
        if row["ENTITY_TYPE"] == "ORGT": 
            if row["ENTITY_NAME"] not in target["ORGT"]: 
                target.iloc(orgtNum)["ORGT"] = row["ENTITY_NAME"] 
                orgtNum = orgtNum + 1 
        if row["ENTITY_TYPE"] == "POSI": 
            if row["ENTITY_NAME"] not in target["POSI"]: 
                target.iloc(posiNum)["POSI"] = row["ENTITY_NAME"] 
                posiNum = posiNum + 1 
    print("# of COMPs: " + compNum)
    print("# of ORGAs: " + orgaNum)
    print("# of CURRs: " + currNum)
    print("# of CMDTs: " + cmdtNum)
    print("# of PLCEs: " + plceNum)
    print("# of ORGTs: " + orgtNum)
    print("# of POSIs: " + posiNum)
target.to_csv("D:/Richter Project/list_of_entities.csv")