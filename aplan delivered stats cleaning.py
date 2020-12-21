import pandas as pd
import glob, os

directory1 = "E:/A-Plan December/"


file1 = "tableall.csv"
file2 = "decision_cleaning.csv"

df1 = pd.read_csv(directory1 + file1,encoding ='ISO-8859-1')
df2 = pd.read_csv(directory1 + file2,encoding ='ISO-8859-1')
df2.columns =['email','IP','sending_domain','log1','log2','Bounce']

df3 = pd.merge(df1[['campaign_id','email','status','last_attempt','sending_domain']],df2[['email','sending_domain','Bounce']], left_on = 'email', right_on = 'email', how='left')
df3.Bounce.fillna(df3.status, inplace=True)
df3 = df3.drop(['status', 'last_attempt', 'sending_domain_x', 'sending_domain_y'],axis=1)
print(list(df3.columns.values))
print(df3.head(5))
#a = DataFrame(df3.groupby(['campaign_id', 'Bounce']).count().reset_index())
a = pd.DataFrame({'count' : df3.groupby( [ 'campaign_id', 'Bounce'] ).size()}).reset_index()
b = pd.pivot_table(a, values='count', index='campaign_id',columns='Bounce')
print(b.shape)
b.to_csv(directory1 + "oempro_cleaning_Delivery_stats.csv", index=True)