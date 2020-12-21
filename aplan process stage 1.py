import pandas as pd
import numpy as np

from validate_email import validate_email

def charreplace(textstring):
    valstoreplace = ['&' ,'< Select >','0','.','null' ]
    for c in valstoreplace:
        if isinstance(textstring, float):
            textstring = 'null'
        else:
            textstring = textstring.replace(c, '')
    return textstring
    
directory = 'E:/A-plan December/A-Plan December Renewal Data/'
month = 'Dec19' 
files = ['00005A_ORG23790_A_Plan_December_Car_insurance_Branch.csv',\
'00005B_ORG23790_A_Plan_December_Car_insurance_National.csv',\
'00006_ORG23790_A_Plan_December_Home_insurance.csv']

stats = pd.DataFrame(columns=['File',  'Count'])
df = pd.DataFrame()
filecounts = pd.DataFrame(columns=['File','List','Count'])

for file in files:
    a = pd.read_csv(directory + file,encoding = "ISO-8859-1",low_memory=False)
    print(a.shape)
    stats = stats.append(pd.Series([file,  a.shape[0]],index=stats.columns), ignore_index=True)
    df = pd.concat([df,a])
    a['CODEDT'] = a['CODE'] + a['DT_EmailSource'].map(str)
    a['CODEDT'] = a['CODEDT'].str.replace('.0', '')
    if "Branch" in file:
        product = "LocalCar"
    elif "National" in file:
        product = "NationalCar"
    else:
        product = "Home"     
    for i, x in a.groupby(['CODEDT']):
        if  'DT19' in i:
            i = 'MFO'
        elif 'DT1' in i:
            i =  'MPO'
        elif 'DT25' in i:
            i = 'GBO'
        elif i == 'TPLnan':
            i = 'TPL'
        elif i == 'IPTnan':
            i = 'IPT'
        elif i == 'LMnan':
            i = 'UKBO'
        else:
            i = i
        counts = pd.DataFrame([[product,i,str(x.shape[0])]],columns=['File','List','Count'])
        filecounts = filecounts.append(counts)          

    
    

stats = stats.append(pd.Series(['Merged File', df.shape[0]],index=stats.columns), ignore_index=True)
print(df.shape)
print(stats)
print(filecounts.dtypes)
filecounts['Count'] = filecounts['Count'].astype('int')
pivot = pd.pivot_table(filecounts, index='List', columns='File', values='Count', aggfunc=np.sum, margins=True, margins_name = 'Total')
print(pivot)
#df.drop_duplicates(subset='Email', keep='first',inplace=True)
#print("Drop DUplicates", df.shape)
#Remove invalid emails

df['is_valid_email'] = df['Email'].apply(lambda x:validate_email(x))
df = df[df['is_valid_email']]
print("Removed invalid email formats", df.shape)
print(df[~df.Email.str.contains("@",na=False)])
df = df[df.Email.str.contains("@",na=False)]

char = '\+|\*|\'| |\%|,|\"|\/'
df = df[~df['Email'].str.contains(char,regex=True)]
df.to_csv(directory + "A-Plan_combined_" + month + ".csv", index=False)
print("Removed invalid email addresses", df.shape)


to_dropcols = [ 'TITLE', 'FIRSTNAME', 'LASTNAME' ,\
'Brand', 'CODE','A-PLAN_MOTOR', 'A-PLAN_HOME', 'A-PLAN_VAN', 'A-PLAN_BRANCH_TELE',\
'A-PLAN_ADDRESS', 'A-PLAN_ADDRESS2', 'A-PLAN_BRANCH_LINK', 'A-PLAN_BRANCH_MANAGE',\
'A-PLAN_CTA1LINK', 'DT_EmailSource', 'is_valid_email']

df.drop(to_dropcols, axis=1, inplace=True)


new = df["Email"].str.split(pat="@", expand=True)
df['domain'] = new[1]

df.drop_duplicates(subset='Email', keep='first',inplace=True)
df = df.reset_index(drop=True)
print("SQL File", df.shape)

#df['TITLE'] = df['TITLE'].map(lambda x: charreplace(x))
#print(df['TITLE'].value_counts())
#drop_title = ['Prince','Princess','Duke','Consuelo','Mr156227','Cardinal']
#df = df[~df['TITLE'].isin(drop_title)]

df.rename(columns={ 'Email' : 'email'}, inplace=True)

df1 = pd.DataFrame(columns = ['list_id', 'source_url', 'title', 'first_name', 'last_name', 'id', 'email',\
'optin_date','is_duplicate','is_ok','is_blacklisted','is_banned_word','is_banned_domain','is_complaint',\
'is_hardbounce','domain','user_status', 'last_open','last_click','system_created','master_filter',\
'import_filter','email_id','primary_membership_id','primary_membership'])

df1['list_id'] = 'a-plan'
df1['email'] = df['email']
df1['domain'] = df['domain']
df1['id'] = np.arange(len(df1))
df1['is_duplicate'] = 0
df1['is_ok'] = 0
df1['is_blacklisted'] = 0
df1['is_banned_word'] = 0
df1['is_banned_domain'] = 0
df1['is_complaint'] = 0
df1['is_hardbounce'] = 0
df1['last_open'] = 0
df1['last_click'] = 0
df1['system_created'] = 0
df1['email_id'] = 0
df1['primary_membership_id'] 

print(df1.head(5))
df1.to_csv(directory + "A-Plan_readyforSQL_" + month + ".csv", index=False)
stats.to_csv(directory + "A-plan_stats_" + month + ".csv", index=False)
filecounts.to_csv(directory + "A-Plan_filecounts_" + month + ".csv", index=False)