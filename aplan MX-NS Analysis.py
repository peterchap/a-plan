import pandas as pd

directory = 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/Data Cleaning Project/'
directory2="E:/A-Plan February 2020/A-Plan February Renewal Data ESB/"

file1 = 'domainok_bad.csv'
file2 = 'A_Plan_Feb20_Stage1Complete.csv'


df1 = pd.read_csv(directory+file1,encoding = "ISO-8859-1",low_memory=False)

df2 = pd.read_csv(directory2+file2,encoding = "ISO-8859-1",low_memory=False,usecols=['domain', 'status'])

match = pd.merge(df1, df2, left_on=['Domain'], right_on=['domain'], how='left')

match.to_csv(directory + "aplan domain_traps_matched.csv", index=False)