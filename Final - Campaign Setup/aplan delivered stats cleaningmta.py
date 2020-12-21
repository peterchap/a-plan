import pandas as pd
import glob, os

directory1 = "E:/A-Plan/A-Plan January 2021/"
directory2 = "E:/A-Plan/A-Plan January 2021/A-Plan January Final Data/"

file1 = "Aplan_main_MTA_Delivery_201220.csv"
file2 = "decisionproductionMTA_jan21.csv"

df1 = pd.read_csv(directory1 + file1,encoding ='ISO-8859-1')
df1.sort_values(by=["m_CampaignId","m_LogDate"], inplace = True) 
df1.drop_duplicates(subset=["m_CampaignId","m_LogDate","m_To"], keep='last',inplace=True) 

df2 = pd.read_csv(directory1 + file2,encoding ='ISO-8859-1')
print(list(df2.columns.values))
#df2.columns =['email','ip','send domain','log1','log2','Bounce']
df2.columns =['status_code','log','email','Bounce']
print(list(df2.columns.values))

df3 = pd.merge(df1[['m_CampaignId','m_To','m_Status']],df2[['email','Bounce']], left_on = 'm_To', right_on = 'email', how='left')
df3.Bounce.fillna(df3.m_Status, inplace=True)
print(list(df3.columns.values))
df3 = df3.drop(['m_Status','email'],axis=1)
print(list(df3.columns.values))
print(df3.head(5))
#a = DataFrame(df3.groupby(['campaign_id', 'Bounce']).count().reset_index())
a = pd.DataFrame({'count' : df3.groupby( [ 'm_CampaignId', 'Bounce'] ).size()}).reset_index()
b = pd.pivot_table(a, values='count', index='m_CampaignId',columns='Bounce')
print(b.shape)
b.to_csv(directory1 + "oempro_production_jan21_Delivery_stats.csv", index=True)