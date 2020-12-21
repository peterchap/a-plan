import pandas as pd

directory = 'E:/A-Plan November/A-Plan November Renewal Data/'
onedrive= 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/'
file = 'A-Plan_SQL_Completed_Nov19.csv'
df1 = pd.read_csv(directory + file,sep=',',encoding = "ISO-8859-1",low_memory=False,error_bad_lines=False)
print(df1.shape)
print(list(df1.columns.values))

df1.drop_duplicates(subset='email', keep='first',inplace=True)
print("SQL File", df1.shape)

df1 = df1[df1['master_filter'].notnull()]
print("master filter", df1.shape)


#stats.to_csv(directory + "A-Plan_Nov19_Stats.csv", index=False)
df1.to_csv(directory +"Aplan_masterfilter _Nov19.csv", index=False)
#a.to_csv(directory + "MXO_EMAIL_OVERSIGHT_AFY_30000_test_unknowns.csv", index=False)