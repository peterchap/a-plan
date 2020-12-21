import pandas as pd
import glob, os

directory1 = "E:/A-plan October/"
directory2 = "E:/A-plan October/A-Plan October Final Data/"
file1 = "aplan260919_delivery.csv"
file2 = "decisionproduction.csv"
df1 = pd.read_csv(directory1 + file1)
print(df1.shape)

df1.sort_values('m_LogDate', ascending=True,inplace=True)
df1.drop_duplicates(subset='m_LogDate', keep='last',inplace=True)
print(df1.shape)

df2 = pd.read_csv(directory1 + file2)

df3 = pd.merge(df1,df2[['m_To','Status']],on= 'm_To', how='left')
df3.sort_values('m_LogDate', ascending=True,inplace=True)
df3.drop_duplicates(subset='m_LogDate', keep='last',inplace=True)
print(df3.shape)
print(list(df3.columns.values))

stats = pd.DataFrame()

os.chdir(directory2)
for file in glob.glob("*production*.csv"):
    df4 = pd.read_csv(file,encoding ="ISO-8859-1")
    print(list(df4.columns.values))
    df5 = pd.merge(df4[['Email']],df3[['m_CampaignId','m_To','m_Status','Status']], left_on = 'Email', right_on = 'm_To', how='left')
    df5.Status.fillna(df5.m_Status, inplace=True)
    b = (list(df5.groupby(['m_Status','Status']).size()))
    b.append(file)
    filestat = pd.DataFrame([b])
    #print(filestat)
    stats = stats.append(filestat)
columns=['Hard', 'Soft', 'Defered', 'Delivered', 'Expired', 'Filtered', 'File']    
#stats.to_csv(directory1 + "stats_production.csv", index=False)
print("In Production Completed")

