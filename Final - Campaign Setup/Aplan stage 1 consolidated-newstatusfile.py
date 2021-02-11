import pandas as pd
from validate_email import validate_email
import sqlalchemy as sa
from sqlalchemy import create_engine
import pyodbc
from disposable_email_domains import blocklist

def remove_temp_domains(data):
    domains = data['domain']
    #domains.drop_duplicates(inplace=True)
    m =[]
    for domain in domains:
        m.append(( domain, domain in blocklist))
    n = pd.DataFrame(m, columns=('domain', 'temp'))
    data['temp'] = n['temp']
    print(data.head(10))
    return data

directory = 'E:/A-Plan/A-Plan March 2021/A-Plan March Renewal Data/'
onedrive = 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/Data Cleaning Project/'
onedrive2 = 'C:/Users/Peter/OneDrive - Email Switchboard Ltd/'
month = 'Mar21' 

listname = 'Aplan_Mar2021'
email= ['EMAIL']

files = ['4140_A_Plan_Mar_Car_insurance_Branch_ESB_R.csv',\
    '4140_A_Plan_Mar_Car_insurance_National_ESB_R.csv',\
    '4140_A_Plan_Mar_Home_insurance_ESB_R.csv',\
    '4140_A_Plan_Mar_Home_insurance_ESB_DPH.csv']

#excludedomains = 'aplan domain_traps_matched.csv'

stats = pd.DataFrame(columns=['File',  'Count'])
df = pd.DataFrame()
filecounts = pd.DataFrame(columns=['File','List','Count'])

for file in files:
    a = pd.read_csv(directory + file,encoding = "ISO-8859-1",low_memory=False, usecols=email)
    print(a.shape)
    stats = stats.append(pd.Series([file,  a.shape[0]],index=stats.columns), ignore_index=True)
    df = pd.concat([df,a])
print(stats)
stats.to_csv(directory + month + "rawfilecounts.csv", index=False)

df.rename(columns={'EMAIL' : 'email'},inplace=True)
print(df.shape)
df.columns = map(str.lower, df.columns)
df['email'] = df['email'].str.lower()
df.drop_duplicates(subset=['email'], inplace=True)
print(df.shape)

df['is_valid_email'] = df['email'].apply(lambda x:validate_email(x))
df = df[df['is_valid_email']]
print("Removed invalid email formats", df.shape)
print(df[~df.email.str.contains("@",na=False)])
df = df[df.email.str.contains("@",na=False)]
char = '\+|\*|\'| |\%|,|\"|\/'
df = df[~df['email'].str.contains(char,regex=True)]

print("Removed invalid email addresses", df.shape)

new = df["email"].str.split(pat="@", expand=True)
df['domain'] = new[1]
df['list_id'] = listname

df.drop_duplicates(subset='email', keep='first',inplace=True)
df = df.reset_index(drop=True)
df.drop('is_valid_email',axis =1,inplace=True)
print("SQL File", df.shape)

df.astype({ 'email' : str, 'domain' : str})


server = '78.129.204.215'
database = 'ListRepository'

engine = create_engine("mssql+pyodbc://perf_webuser:n3tw0rk!5t@t5@" + server + "/" + database + "?driver=ODBC+Driver+17+for+SQL+Server",fast_executemany=True)

#engine = create_engine("mssql+pyodbc://perf_webuser:n3tw0rk!5t@t5" + server + "/" + database + "?driver=ODBC+Driver+17+for+SQL+Server",fast_executemany=True)


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



df2 = pd.read_csv(onedrive + "domain_status.csv",\
    encoding = "utf-8",low_memory=False, usecols=['name', 'status'])
cols = {'name' : 'domain'}
df2.rename(columns=cols, inplace=True)
print(" Domain Status File",df2.shape)
print(list(df2.columns.values))

df = pd.merge(df1, df2, on=['domain'], how='left')

print("Merged File",df.shape)

df = remove_temp_domains(df)

#df = pd.merge(df, temps, on=['domain'], how='left')
df.loc[df['temp'] == 1, 'status'] ='Temp domain'
print("temps File",df.shape)

df.loc[((df['is_blacklisted'] == 1) & (df['status'] == 'OK')), 'status'] ='Blacklisted'
df.loc[((df['is_banned_word'] == 1) & (df['status'] == 'OK')), 'status'] ='Banned words'
#df.loc[df['is_banned_domain'] == 1, 'status'] ='Banned domains'
df.loc[((df['is_complaint'] == 1) & (df['status'] == 'OK')), 'status'] ='Complainers'
df.loc[((df['is_hardbounce'] == 1) & (df['status'] == 'OK')), 'status'] ='Hard Bounces'
#df.loc[((df['user_status'].isin(['Rejected','Cleaning - Dead', 'Cleaning - Quarantined', 'Quarantine']) & df['status'] == 'OK',\
#'status'] = 'Cleaning - Rejected'
#df.loc[df['user_status'] == 'Rejected', 'status'] = 'Cleaning - Rejected'
df.loc[df['domain'].str.contains('.gov'), 'status'] = 'EXCLUDED'



df3 = pd.read_csv(onedrive2 + "TLDGeneric lookup.csv",encoding = "ISO-8859-1",low_memory=False)

tld = df["domain"].str.rsplit(pat=".",n=1, expand=True)

df['tld'] = tld[1]
df = pd.merge(df, df3, on=['tld'], how='left')

print("tld File",df.shape)
df.loc[~df['location'].isin(['generic','Generic', 'United Kingdom (UK)']), 'status'] = 'Non-UK'

dormant = ['fsnet', 'freeserve', 'wanadoo', 'tesco', 'orangehome']
df.loc[df['domain'].str.contains('|'.join(dormant)), 'status'] ='Dormant domain'

print(df['status'].value_counts())

mailable = ['OK', 'Not Checked']
df.loc[(df['status'].isin(mailable)), 'data flag'] = 'cleaning'
df.loc[(~df['status'].isin(mailable)), 'data flag'] = 'remove'

#exclude = pd.read_csv(directory + excludedomains ,encoding = "ISO-8859-1",low_memory=False,usecols = ['Domain', 'Description'])
#df = pd.merge(df, exclude, left_on=['domain'], right_on=['Domain'], how='left')

#df.loc[df['domain'].isin(exclude.Domain), 'status'] = 'Spam Traps'

#userstatus_remove = ['Rejected', 'Cleaning - Quarantined', 'Quarantine']
#df.loc[df['user_status'].isin(userstatus_remove), 'data flag'] ='remove'
#quarantine = df[df['user_status'].isin(userstatus_remove)]
#print("Quarantined", quarantine.shape)
#quarantine.to_csv(directory + "Quarantine" + month + "_newstatus.csv", index=False)

cols = ['domain','data flag','status']
df.loc[(df['status'].isnull()), 'status'] = 'unknown'
unknowns = df[cols][df['status'].isin(['unknown'])]
unknowns.to_csv(directory + "A-Plan_unknowns-v4" + month + ".csv", index=False)
print("Unknowns", unknowns.shape)


production_filters = ['New','Active','Probational-Gmail','Reactive 30-90 days','Active Probational 181 days plus',\
'Active-Hotmail','Active Probational 91-180 days','Probational-Hotmail',\
'Active-Gmail']

repossess_filters = ['Repossess-Gmail','Repossess 30','Repossess-Hotmail']

df.loc[(df['master_filter'].isin(production_filters)) , 'data flag'] = 'active'
df.loc[(df['master_filter'].isin(repossess_filters)& (df['data flag'] != 'remove')) , 'data flag'] = 'repossess'

print("Final File",df.shape)
print(df['data flag'].value_counts())
print(df['status'].value_counts())
print(df.columns)
df.to_csv(directory + "A_Plan_" +  month + "_newstatus_Stage1Complete_v2.csv", index=False)
