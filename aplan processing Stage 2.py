import pandas as pd

directory = 'E:/Cleaning-todo/'
file = 'A-Plan_SQLDone_Dec19_Read.csv'
onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"

df1 = pd.read_csv(directory + file,sep=',',encoding = "ISO-8859-1",low_memory=False,error_bad_lines=False)
print(df1.shape)
print(list(df1.columns.values))

df1.drop_duplicates(subset='email', keep='first',inplace=True)
print("SQL File", df1.shape)

df2 = pd.read_csv(onedrive + "domain_status_UK.csv",encoding = "ISO-8859-1",low_memory=False)
print(" Domain Status File",df2.shape)
print(list(df2.columns.values))
#df3 = pd.read_csv("C:/Users/Peter/Downloads/domain_status_Update.csv",encoding = "ISO-8859-1",low_memory=False)
#df2 = pd.concat([df2, df3]).drop_duplicates(['domain'], keep='last').sort_values('domain')
#df2.to_csv("C:/Users/Peter/Downloads/domain_status_UK.csv", index=False)
#print(df2.shape)
#stats = pd.read_csv(directory + "Aplan_Sep19_stats.csv")

df = pd.merge(df1, df2, on=['domain'], how='left')

print("Merged File",df.shape)

stats = pd.DataFrame()

stats = stats.append(pd.Series(['All', 'Blacklisted', df[df['is_blacklisted'] == 1].shape[0]]), ignore_index=True)
stats = stats.append(pd.Series(['All', 'Banned words', df[df['is_banned_word'] == 1].shape[0]]), ignore_index=True)
stats = stats.append(pd.Series(['All', 'Banned Domains', df[df['is_banned_domain'] == 1].shape[0]]), ignore_index=True)
stats = stats.append(pd.Series(['All', 'Complainers', df[df['is_complaint'] == 1].shape[0]]), ignore_index=True)
stats = stats.append(pd.Series(['All', 'Hard Bounces', df[df['is_hardbounce'] == 1].shape[0]]), ignore_index=True)

print(stats)


df['rtotal'] = df['is_blacklisted'] + df['is_banned_word'] + df['is_banned_domain'] + df['is_complaint'] + df['is_hardbounce']


print(df['rtotal'].value_counts())


bad =['FOREIGN', 'UNKNOWN', 'NO MX', 'EXCLUDED', 'BAD', 'BLACKLISTED','SPAM TRAP', 'EXPIRED', 'Not Set', 'TEMP', 'INVALID']
df.loc[df.status.isin(bad), 'data flag'] = 'Remove'

df.loc[df.rtotal > 0, 'data flag'] ='Remove'

userstatus_remove = ['Rejected','Cleaning - Dead', 'Cleaning - Quarantined', 'Quarantine']
df.loc[df['user_status'].isin(userstatus_remove), 'data flag'] ='Remove'
quarantine = df[df['user_status'].isin(userstatus_remove)]
print(quarantine.shape)
quarantine.to_csv(directory + "Quarantine_Dec19.csv", index=False)

production_filters = ['New','Active','Probational-Gmail','Reactive 30-90 days','Active Probational 181 days plus',\
'Active-Hotmail','Active Probational 91-180 days','Probational-Hotmail',\
'Active-Gmail']

repossess_filters = ['Repossess-Gmail','Repossess 30','Repossess-Hotmail']

df.loc[(df['master_filter'].isin(production_filters)) , 'data flag'] = 'Active'
df.loc[(df['master_filter'].isin(repossess_filters)& (df['data flag'] != 'Remove')) , 'data flag'] = 'Repossess'

df.loc[(df['data flag'].isnull()),'data flag'] = 'Cleaning'

cols = ['domain','data flag','status']
a = df[cols][df['status'].isnull() & (df['data flag'] != 'Remove')]


print(df['data flag'].value_counts())
#print('Domain Unknown',a.count(), a)

print(df['status'].value_counts())

nulls = df[df['data flag'].isnull()]
print(nulls.shape)
print(df.shape)

ispgroup = pd.read_csv(onedrive+'ISP Group domains.csv')
b = pd.merge(df, ispgroup, left_on='domain', right_on='Domain', how='left')
b['Group'].fillna("Other", inplace = True)
print(b['Group'].value_counts())

stats.to_csv(directory + "A-Plan_Dec19_Stats_read.csv", index=False)
df.to_csv(directory +"A-Plan_Dec19_all_data_flag_Read.csv", index=False)
a.to_csv(directory + "A-Plan_Dec19_unknowns_Read.csv", index=False)