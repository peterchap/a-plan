import pandas as pd

directory = 'E:/Cleaning-todo/'
onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"
file = 'Aplan data_Jan20jan20Stage1Complete.csv'

df1 = pd.read_csv(directory + file,sep=',',encoding = "ISO-8859-1",low_memory=False)
print(df1.shape)
print(list(df1.columns.values))

df1.drop_duplicates(subset='email', keep='first',inplace=True)
print(df1.shape)

df2 = pd.read_csv(onedrive+"domain_status_UK.csv",encoding = "ISO-8859-1",low_memory=False)
print(df2.shape)
print(list(df2.columns.values))



df = pd.merge(df1, df2, on=['domain'], how='left')

print(df.shape)

df['rtotal'] = df['is_blacklisted'] + df['is_banned_word'] + df['is_banned_domain'] + df['is_complaint'] + df['is_hardbounce']


print(df['rtotal'].value_counts())
print(list(df.columns.values))


df.loc[df.rtotal > 0, 'data flag'] ='Remove'

print(df['data flag'].value_counts())

print(df['user_status'].value_counts())

df.loc[(df['data flag'] != 'Remove') & (df['user_status'] != 'Mailable'), 'data flag'] = 'In Cleaning'



df.loc[(df['data flag'].isnull()) , 'data flag'] = 'In Production'
print(df['data flag'].value_counts())

bad =['FOREIGN', 'UNKNOWN', 'NO MX', 'EXCLUDED', 'BAD', 'BLACKLISTED','SPAM TRAP', 'EXPIRED', 'Not Set', 'TEMP', 'INVALID']
df.loc[df.status.isin(bad), 'data flag'] = 'Remove'

print(df['data flag'].value_counts())

cols = ['domain','data flag','status']
a = df[cols][df['status'].isnull() & (df['data flag'] != 'Remove')]
df.loc[(df['status'].isnull() & (df['data flag'] != 'Remove')),'data flag'] = 'In Cleaning'


print(df['data flag'].value_counts())
print('Domain Unknown',a.count(), a)

print(df['status'].value_counts())

df.to_csv(directory + "Aplan data_November19_matched_data_flag.csv", index=False)
a.to_csv(directory +  "Aplan data_November19_matched_unknowns.csv", index=False)