import pandas as pd
import numpy as np
from disposable_email_domains import blocklist
from validate_email import validate_email

#Functions

#Remove invalid email formats

def remove_invalid_emails(data):
    data.loc[~data['email'].apply(lambda x:validate_email(x)), 'data flag'] = 'remove'
    data.loc[~data['email'].apply(lambda x:validate_email(x)), 'status'] = 'Invalid Email'
    data.loc[~data.email.str.contains("@",na=False), 'data flag'] ='remove'
    data.loc[~data.email.str.contains("@",na=False), 'status'] ='Invalid EMail'
    char = '\+|\*|\'| |\%|,|\"|\/'
    data.loc[df.email.str.contains(char,regex=True,na=False), 'data flag'] = 'remove'
    data.loc[df.email.str.contains(char,regex=True,na=False), 'status'] = 'Invalid Email'
    return data



# End of Functions

directory = 'E:/a-Plan/A-Plan October 2020/A-Plan October Renewal Data/'
directory_2 = 'E:/A-plan/A-Plan October 2020/A-Plan October Final Data/'
onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/Data Cleaning Project/"
month = 'Sep20' 

files = ['4034_A_Plan_Oct_Car_insurance_Branch_ESB_R.csv',\
'4034_A_Plan_Oct_Car_insurance_National_ESB_R.csv',\
'4034_A_Plan_Oct_Home_insurance_ESB_R.csv',\
'4034_A_Plan_Oct_Home_insurance_ESB_DPH.csv']

statusfile = "A_Plan_Oct20_newstatus_Stage1Complete_v2.csv"

valuecounts = pd.DataFrame()

#Start processing
df2 = pd.read_csv(directory + statusfile)

print('Gross', df2.shape[0])
df2 = df2[df2['data flag'] =='remove']
df2 = df2.drop_duplicates(subset='email', keep='first')

print(df2.columns)
to_drop=['list_id', 'source_url', 'title', 'first_name', 'last_name', 'id',\
'optin_date', 'is_duplicate', 'is_ok', 'is_blacklisted',\
'is_banned_word', 'is_banned_domain', 'is_complaint', 'is_hardbounce',\
'domain', 'user_status', 'last_open', 'last_click', 'system_created',\
'master_filter', 'import_filter', 'email_id', 'primary_membership_id',\
'primary_membership', 'temp', 'tld', 'type', 'location',\
'tld manager']
df2.drop(to_drop, axis=1, inplace=True)
df2.columns = map(str.lower, df2.columns)

print("All Remove Flag", df2.shape)
print(df2.columns)



filecounts = pd.DataFrame(columns=['Product','List','Type','Count'])

for file in files:
    df1 = pd.read_csv(directory + file,encoding = "ISO-8859-1",low_memory=False)
    df1.columns = map(str.lower, df1.columns)
    print(file,df1.shape[0])
    df = pd.merge(df1, df2, left_on='email', right_on='email', how='inner') 
    

    # get domain analysis stats
    
       
        
    nulls = df[df['data flag'].isnull()]
    print( "Nulls",nulls.shape[0])
    nulls.to_csv(directory + "nulls_" + file[:10] + month + ".csv", index=False)
    a = df['data flag'].value_counts(dropna='False')

    df = remove_invalid_emails(df)
    b = df['data flag'].value_counts() 
    
     
    #generate remove reason file
    removed = df[df['data flag'] == 'remove']
    removed['List'] = removed['code'] + removed['dt_emailsource'].map(str)
    removed['List'] = removed['List'].str.replace('.0', '')
    
    removed['List'] = removed['List'].replace('DT36', 'DTNEW')
    removed['List'] = removed['List'].replace('DT1', 'MPO')
    removed['List'] = removed['List'].replace('DT25', 'GBO')    
    removed['List'] = removed['List'].replace('TPLnan', 'TPL')
    removed['List'] = removed['List'].replace('IPTnan', 'IPT')
    removed['List'] = removed['List'].replace('LMnan', 'UKBO')
    removed['List'] = removed['List'].replace('CFnan', 'SSU')

    print(removed.columns)

    to_dropremove = ['title','firstname', 'lastname',  'brand', 'a-plan_motor',\
    'a-plan_home', 'a-plan_van', 'a-plan_branch_tele', 'a-plan_address',\
    'a-plan_address2', 'a-plan_branch_link', 'a-plan_branch_manage',\
    'a-plan_cta1link','code', 'dt_emailsource','data flag']
    removed.drop(to_dropremove, axis=1, inplace=True)
    #removed.rename(columns={'is_blacklisted': 'Blacklisted', 'is_banned_word' : 'Banned Word',\
    #'is_banned_domain': 'Banned Domain','is_complaint' : 'Complainer',\
    #'is_hardbounce' : 'Hard Bounce', 'name' : 'Domain Check', 'Temp' : 'Temp Domain'}, inplace=True)
    print('Removed', removed.shape)
    print(removed.columns)
    print(file)
    print(removed.head(5))
    value_counts = removed['status'].value_counts()
    values = pd.DataFrame(value_counts.rename_axis('Category').reset_index(name='Unique Emails'))
    valuecounts = pd.concat([valuecounts,values])
    removed.to_csv(directory_2 + "Removals" + file[-18:-4] + month + ".csv", index=False)

valuecounts.to_csv(directory_2 + "Removals_value_counts" + month + ".csv", index=True)