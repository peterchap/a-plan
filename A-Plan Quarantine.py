import pandas as pd 

directory = 'E:/A-Plan June 2020/A-Plan June Renewal Data/'

month = 'Jun20'

statusfile = "A_Plan_Jun20_newstatus_Stage1Complete_v2.csv"



#Start processing
df = pd.read_csv(directory + statusfile, )

print('Gross', df.shape[0])
df1 = df[df['data flag'] =='remove']
df2 = df[df['user_status'].isin(['Rejected', 'Quarantine', 'Cleaning-Dead'])]
df3 = pd.concat([df1,df2], ignore_index=True)
df3.drop_duplicates(subset='email', keep='first', inplace=True)

print(df3.shape)

df3.to_csv(directory + 'Quarantined_v2_' + month + '.csv', index=False)
#to_drop=[]
#df2.drop(to_drop, axis=1, inplace=True)
#df2.columns = map(str.lower, df2.columns)

#print("All Remove Flag", df2.shape)
#print(df2.columns)
