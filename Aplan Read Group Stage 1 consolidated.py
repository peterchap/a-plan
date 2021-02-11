import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
import pyodbc
from validate_email import validate_email
from disposable_email_domains import blocklist
import time

def flag_temp_domains(data):
    domains = data['domain']
    #domains.drop_duplicates(inplace=True)
    m =[]
    for domain in domains:
        m.append(( domain, domain in blocklist))
    n = pd.DataFrame(m, columns=('domain', 'temp'))
    data['temp'] = n['temp']
    return data

directory = 'E:/Cleaning-todo/'
onedrive= 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/'

filename = 'Aplan data_Feb21.csv'
month = 'mar21'

df = pd.read_csv(directory + filename,encoding ='utf-8')

print('Start Time: ', time.ctime(time.time()))

print(df.columns)

df.drop_duplicates(subset=['Email'], inplace=True)
print(df.shape)

#df['is_valid_email'] = df['email'].apply(lambda x:validate_email(x))
#df = df[df['is_valid_email']]
#print("Removed invalid email formats", df.shape)
print(df[~df.Email.str.contains("@",na=False)])
df = df[df.Email.str.contains("@",na=False)]
char = '\+|\*|\'| |\%|,|\"|\/'
df = df[~df['Email'].str.contains(char,regex=True)]

print("Removed invalid email addresses", df.shape)

new = df["Email"].str.split(pat="@", expand=True)
df['Domain'] = new[1]

df.rename(columns={"BranchName": "Source_URL", "Forename": "First_Name", \
    "Surname" : "Last_Name", "SupplierCode" : "List_ID"}, inplace=True)

df.drop_duplicates(subset='Email', keep='first',inplace=True)
df = df.reset_index(drop=True)
#df.drop('is_valid_email',axis =1,inplace=True)
print("SQL Input File", df.shape)
print(df.columns)

df.astype({ 'Email' : str, 'Domain' : str})



server = '78.129.204.215'
database = 'ListRepository'

engine = create_engine("mssql+pyodbc://perf_webuser:n3tw0rk!5t@t5@" + server + "/" + database + "?driver=ODBC+Driver+17+for+SQL+Server",fast_executemany=True)


cnxn = engine.connect()
rs = cnxn.execute('DELETE FROM dbo.temp_tia')
cnxn.close()
print(rs)

df.to_sql('temp_tia', con = engine, schema = 'dbo', if_exists = 'append', index=False, chunksize = 1000)


cursor = engine.raw_connection().cursor()
cursor.execute("dbo.Temp_Tia_UpdateMetadata")
cursor.commit()

query = "SELECT * FROM dbo.temp_tia"
df1 = pd.read_sql_query(query,engine)

print(df1.shape)
print("Temp_tia processing completed successfully")



df2 = pd.read_csv(onedrive + "Data Cleaning Project/domain_status.csv",\
    encoding = "utf-8",low_memory=False, usecols=['name', 'status'])
cols = {'name' : 'domain'}
df2.rename(columns=cols, inplace=True)
print(" Domain Status File",df2.shape)
print(list(df2.columns.values))

df = pd.merge(df1, df2, left_on=['domain'], right_on=['domain'],how='left')
#df.to_csv(directory + filename[:-4] +  month + "status_test_v2.csv", index=False)
print("Merged File",df.shape)



mailable = ['OK','Not Checked', 'Recheck']
df.loc[(df['status'].isin(mailable)), 'data flag'] = 'cleaning'
df.loc[(~df['status'].isin(mailable)), 'data flag'] = 'remove'

df = flag_temp_domains(df)

#df = pd.merge(df, temps, on=['domain'], how='left')
df.loc[df['temp'] == 1, 'status'] ='Temp domain'
print("temps File",df.shape)

df.loc[df['is_blacklisted'] == 1, 'status'] ='Blacklisted'
df.loc[df['is_banned_word'] == 1, 'status'] ='Banned words'
#df.loc[df['is_banned_domain'] == 1, 'status'] ='Banned domains'
df.loc[df['is_complaint'] == 1, 'status'] ='Complainers'
df.loc[df['is_hardbounce'] == 1, 'status'] ='Hard Bounces'
#df.loc[df['user_status'].isin(['Rejected','Cleaning - Dead', 'Cleaning - Quarantined', 'Quarantine']),\
#'status'] = 'Cleaning - Rejected'
#df.loc[df['user_status'] == 'Rejected', 'status'] = 'Cleaning - Rejected'
df.loc[df['domain'].str.contains('.gov'), 'status'] = 'Gov Domain'


'''
df3 = pd.read_csv(onedrive + "TLDGeneric lookup.csv",encoding = "ISO-8859-1",low_memory=False)

tld = df["domain"].str.rsplit(pat=".",n=1, expand=True)

df['tld'] = tld[1]
df = pd.merge(df, df3, on=['tld'], how='left')
print("tld File",df.shape)

df.loc[~df['location'].isin(['generic','Generic', 'United Kingdom (UK)','UK']), 'status'] = 'Non-UK'
'''


cols = ['domain','status']
unknowns = df[cols][df['status'].isnull()]
unknowns.to_csv(directory + filename[:-4] + "_unknowns" + month + ".csv", index=False)
print("Unknowns", unknowns.shape)
df.loc[(df['status'].isnull()), 'status'] = 'unknown'


production_filters = ['New','Active','Probational-Gmail','Reactive 30-90 days','Active Probational 181 days plus',\
'Active-Hotmail','Active Probational 91-180 days','Probational-Hotmail',\
'Active-Gmail']

repossess_filters = ['Repossess-Gmail','Repossess 30','Repossess-Hotmail']

df.loc[(df['master_filter'].isin(production_filters)) , 'data flag'] = 'active'
df.loc[(df['master_filter'].isin(repossess_filters)& (df['data flag'] != 'remove')) , 'data flag'] = 'repossess'


print("Final File",df.shape)
print(df['data flag'].value_counts())
print(df['status'].value_counts())
df.to_csv(directory + filename[:-4] +  month + "Stage1Complete.csv", index=False)
print('Finish Time: ', time.ctime(time.time()))