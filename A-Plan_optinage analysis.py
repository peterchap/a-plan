import pandas as pd
import numpy as np
from disposable_email_domains import blocklist
from validate_email import validate_email

#Functions

#Remove invalid email formats

def remove_invalid_emails(data):
    data.loc[~data['Email'].apply(lambda x:validate_email(x)), 'data flag'] = 'Remove'
    data.loc[~data.Email.str.contains("@",na=False), 'data flag'] ='Remove'
    char = '\+|\*|\'| |\%|,|\"|\/'
    data.loc[df.Email.str.contains(char,regex=True,na=False), 'data flag'] = 'Remove'
    return data


def report_ISP_groups(data, ispgroup):
        
    new = data['Email'].str.split(pat="@", expand=True)
    data.loc[:,'Left']= new.iloc[:,0]
    data.loc[:,'Domain'] = new.iloc[:,1]

    ispdata = pd.merge(data, ispgroup, on='Domain', how='left')
    ispdata.loc[:,'Group'].fillna("Other", inplace = True)
    stat = pd.DataFrame(ispdata['Group'].value_counts()).reset_index()
    stat.rename(columns={'index' : 'ISP', 'Group' : 'count'}, inplace=True)
    return stat

# End of Functions

directory = 'E:/A-Plan January/A-Plan January Renewal Data ESB/'
directory_2 = 'E:/A-Plan January/'
onedrive = "C:/Users/Peter/OneDrive - Email Switchboard Ltd/"



month = 'Jan20' 

files = ['00005A_ORG23881_A_Plan_January_Car_insurance_Branch.csv',\
'00005B_ORG23881_A_Plan_January_Car_insurance_National.csv',\
'00006_ORG23881_A_Plan_January_Home_insurance.csv']



#Start processing
df2 = pd.read_csv(directory + "A_Plan_Jan19_Stage1Complete.csv")
ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")

print('Gross', df2.shape[0])
df2 = df2.drop_duplicates(subset='email', keep='first')


print("All Data Flag", df2.shape[0])

colstouse=['Email', 'EmailOptinMonths', 'CODE', 'DT_EmailSource']
df1 = pd.DataFrame()

for file in files:
    a = pd.read_csv(directory + file,encoding = "ISO-8859-1",low_memory=False, usecols=colstouse)
    print(a.shape)
    df1 = pd.concat([df1,a])

#dtype={'is_blacklisted': np.bool, 'is_banned_word': np.bool, 'is_banned_domain': np.bool, 'is_complaint': np.bool, 'is_hardbounce': np.bool})
    
df = pd.merge(df1, df2, left_on='Email', right_on='email', how='left') 
    
df['CODEDT'] = df['CODE'] + df['DT_EmailSource'].map(str)
df['CODEDT'] = df['CODEDT'].str.replace('.0', '')

df['List'] = ""

df.loc[df['CODEDT'] == 'DT19', ['List']] = 'MFO'
df.loc[df['CODEDT'] == 'DT1', ['List']] = 'MPO'
df.loc[df['CODEDT'] == 'DT25', ['List']] = 'GBO'
df.loc[df['CODEDT'] == 'TPLnan', ['List']] = 'TPL'
df.loc[df['CODEDT'] == 'IPTnan', ['List']] = 'IPT'
df.loc[df['CODEDT'] == 'LMnan', ['List']] = 'UKBO'
df.loc[df['CODEDT'] == 'CFnan', ['List']] = 'SSU'


to_dropcols = ['list_id', 'source_url', 'title', 'first_name', 'last_name', 'id', 'email', 'optin_date', 'is_duplicate', 'is_ok', \
'user_status', 'last_open',\
'last_click', 'system_created',  'import_filter', 'email_id', ]
df.drop(to_dropcols, axis=1, inplace=True)

df['Bucket'] = pd.qcut(df['EmailOptinMonths'],q=6)   
pivot1 = pd.pivot_table(df,index=['List'], columns=['Bucket'],values=['is_hardbounce'],aggfunc=np.sum)

print(pivot1)
pivot1.to_csv(directory_2 + "optin hardbounce pivot"+ month + ".csv", index=True)   
 
pivot2 = pd.pivot_table(df,index=['List'], columns=['Bucket'], values=['Email'],aggfunc=pd.Series.nunique)
pivot2.to_csv(directory_2 + "optin by list email count pivot"+ month + ".csv", index=True)  

newdata = df[df['primary_membership'].isna()]
pivot3 = pd.pivot_table(newdata,index=['EmailOptinMonths'], columns=['List'], values=['Email'],aggfunc=pd.Series.nunique)
pivot3.to_csv(directory_2 + "optin unknown email pivot"+ month + ".csv", index=True)  

production_filters = ['New','Active','Probational-Gmail','Reactive 30-90 days','Active Probational 181 days plus',\
'Active-Hotmail','Active Probational 91-180 days','Probational-Hotmail',\
'Active-Gmail']
activedata = df[df['master_filter'].isin(production_filters)]
pivot4 = pd.pivot_table(activedata,index=['EmailOptinMonths'], columns=['List'], values=['Email'],aggfunc=pd.Series.nunique)
pivot4.to_csv(directory_2 + "optin active pivot"+ month + ".csv", index=True) 
