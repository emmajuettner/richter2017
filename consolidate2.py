import os
import glob
import pandas as pd
os.chdir("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/decompressed-dataset/all_data/chunk1/chunk2")
files = glob.iglob("*.csv")
target = pd.DataFrame()
print("testing2")
count = 0
for filename in files:
    nextFile = pd.read_csv(filename,encoding='latin1')
    target = target.append(nextFile)
    print(count)
    count = count + 1
print("testing3")
target.to_csv("C:/Users/emmaj/OneDrive/School/Lake Forest/Freshman Year/Richter Project/full_data_set.csv",sep=",")
print("testing4")
'''
dtype={"RELEVANCE":int,"AES":int,"AEV":int,"RP_STORY_EVENT_INDEX":int,"RP_STORY_EVENT_COUNT":int,"CSS":int,"NIP":int,"PEQ":int,"BEE":int,"BMQ":int,"BAM":int,"BCA":int,"BER":int,"ANL_CHG":int,"MCQ":int,"RP_STORY_EVENT_INDEX":int,"RP_STORY_EVENT_COUNT":int,"CSS":int,"NIP":int,"PEQ":int,"BEE":int,"BMQ":int,"BAM":int,"BCA":int,"BER":int,"ANL_CHG":int,"MCQ":int}
'''