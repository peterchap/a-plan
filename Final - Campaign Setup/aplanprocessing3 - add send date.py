import pandas as pd
import numpy as np
from disposable_email_domains import blocklist
from validate_email import validate_email

#Functions

#Remove invalid email formats

def remove_invalid_emails(data):
    data.loc[~data['Email'].apply(lambda x:validate_email(x)), 'data flag'] = 'remove'
    data.loc[~data.Email.str.contains("@",na=False), 'data flag'] ='remove'
    char = '\+|\*|\'| |\%|,|\"|\/'
    data.loc[df.Email.str.contains(char,regex=True,na=False), 'data flag'] = 'remove'
    return data

# Remove Bad statuss

def remove_bad_statuss(data):
    patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|enquir|fake|feedback|finance|free|garbage|generic|hello|info|invalid|\
    junk|loan|office|market|penis|person|phruit|police|postmaster|random|recep|register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
    data.loc[data['Email'].str.contains(patternDel, na=False), 'data flag'] = 'remove'
    data.loc[data['FIRSTNAME'].str.contains(patternDel, na=False), 'data flag'] = 'remove'
    data.loc[data['LASTNAME'].str.contains(patternDel, na=False), 'data flag'] = 'remove'
    return data

#Remove Temp Domains
    
def remove_temp_domains(data):
    domains = data['domain']
    domains.drop_duplicates(inplace=True)
    m =[]
    
    for domain in domains:
        m.append(( domain, domain in blocklist))
    
    n = pd.DataFrame(m, columns=('domain', 'Temp'))
    data.domain = data.domain.astype(str)
    n.domain = n.domain.astype(str)
    
    data2 = data.merge(n, on='domain', how='left')

    data2.loc[data2.Temp == 1, 'data flag'] = 'remove'
    return data2

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

directory = 'E:/A-Plan August 2020/A-Plan August Renewal Data/'
directory_2 = 'E:/A-Plan August 2020/A-Plan August Final Data/'
onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"

#directory = 'C:/Users/Peter Chaplin\OneDrive - ESB Connect/A-plan October/A-Plan October Renewal Data/'
#directory_2 = 'C:/Users/Peter Chaplin/OneDrive - ESB Connect/A-plan October/A-Plan October Final Data/'
#onedrive="C:/Users/Peter Chaplin/OneDrive - ESB Connect/"

month = 'August20' 

files = ['3989_A_Plan_Aug_Car_insurance_Branch_ESB_C.csv',\
'3989_A_Plan_Aug_Car_insurance_National_ESB_C.csv',\
'3989_A_Plan_Aug_Home_insurance_ESB_C.csv']


#Start processing
cols_touse = ['email','domain','data flag','status']
df2 = pd.read_csv(directory + "A_Plan_Aug20_newstatus_Stage1Complete_v2.csv",encoding = "utf-8",usecols= cols_touse)
df2 = df2[~df2['data flag'].isin(['remove','Remove'])]
ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")

df3 = pd.read_csv(directory_2 + "Send Date Aug20.csv",encoding = "utf-8")
print(df3.head(5))
#df3['List'] = df3['List'].str.encode(encoding = "utf-8")

print('Status file', df2.shape[0])
#df2 = df2.drop_duplicates(subset='email', keep='first')

inputcheck = 0
outputcheck = 0

car = pd.DataFrame()
home = pd.DataFrame()

for file in files:
    df1 = pd.read_csv(directory + file,encoding = "ISO-8859-1",low_memory=False)
    print(file,df1.shape[0])
    inputcheck = inputcheck + df1.shape[0]
    df = pd.merge(df1, df2, left_on='EMAIL', right_on='email', how='inner') 
    new = df['EMAIL'].str.split(pat="@", expand=True)
    df.loc[:,'Left']= new.iloc[:,0]
    df.loc[:,'Domain'] = new.iloc[:,1]

    df = pd.merge(df, ispgroups, on='Domain', how='left')
    print(file,'ISP', df.shape)
    df.loc[:,'Group'].fillna("Other", inplace = True)
    df.loc[:,'ISP'].fillna("Other", inplace = True)
    print(df['ISP'].value_counts())

    if "Home" in file:
        df['Product'] = "Home"
    else:
        df['Product'] = "Car"   
    
    
    print(file,'Gross', df.shape)
    print(df['Product'].value_counts())
    print(df['data flag'].value_counts())

    df['CODEDT'] = df['CODE'] + df['DT_EmailSource'].map(str)
    df['CODEDT'] = df['CODEDT'].str.replace('.0', '')
   

    df['List'] = df['CODEDT'].map({'DT19' : 'MFO', 'DT1' : 'MPO', 'DT25' : 'GBO',\
        'TPLnan' : 'TPL', 'IPTnan' : 'IPT', 'LMnan' : 'UKBO', 'CFnan' : 'SSU' })
    df['List'] = df['List'].map(str)
    print(df['List'].value_counts())
   

    to_dropcols = ['TITLE',  'Brand', 'A-PLAN_MOTOR', 'A-PLAN_HOME', 'A-PLAN_VAN', 'A-PLAN_BRANCH_TELE',\
    'A-PLAN_ADDRESS', 'A-PLAN_ADDRESS2', 'A-PLAN_BRANCH_LINK', 'A-PLAN_BRANCH_MANAGE', 'A-PLAN_CTA1LINK',\
    'CODE', 'DT_EmailSource', 'EmailOptinMonths',  'CODEDT','FIRSTNAME', 'LASTNAME']
    df.drop(to_dropcols, axis=1, inplace=True)
    
    
    
    df = df[df['status'] =='OK']
   
    print(df.head(5))
  
    
    todropfinal = ['Left', 'Domain',  'Product','Group', 'Vol','email','domain']
    print(df.shape)
    print(df3.columns)

    m = pd.merge(df[df['Product'] == 'Car'], df3, left_on=['Product','List', 'data flag', 'ISP'],right_on=['Product','List', 'data flag', 'ISP'], how='left')
    print(m.head(5))        

    m.drop(todropfinal, axis=1, inplace=True)
    car = car.append(m)
    
    h = pd.merge(df[df['Product'] == 'Home'], df3, left_on=['Product','List', 'data flag', 'ISP'],right_on=['Product','List', 'data flag', 'ISP'], how='left')
    
    h.drop(todropfinal, axis=1, inplace=True)
    home = home.append(h)

print(home.shape)
print(car.shape)

home.to_csv(directory_2 + "Aplan_Home_with_Sendate" + month + ".csv", index=False)
car.to_csv(directory_2 + "Aplan_Car_with_Sendate" + month + ".csv", index=False)
