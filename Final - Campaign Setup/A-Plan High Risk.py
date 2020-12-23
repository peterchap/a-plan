import pandas as pd 

directory = 'E:/A-Plan/A-Plan February 2021/A-Plan February Renewal Data/'

month = 'Feb21'

statusfile = "A_Plan_Feb21_newstatus_Stage1Complete_v2.csv"



#Start processing
df = pd.read_csv(directory + statusfile )

print('Gross', df.shape[0])
selection = ['remove', 'cleaning']
df1 = df['email'][df['data flag'].isin(selection)]
df1.drop_duplicates(keep='first', inplace=True)

print(df1.shape)

df1.to_csv(directory + 'High Risk_remail' + month + '.csv', index=False)