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

# Remove Bad Names

def remove_bad_names(data):
    patternDel = "abuse|account|admin|backup|cancel|career|comp|contact|crap|email|enquir|fake|feedback|finance|free|garbage|generic|hello|info|invalid|\
    junk|loan|office|market|penis|person|phruit|postmaster|random|recep|register|sales|shit|shop|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
    data.loc[data['Email'].str.contains(patternDel, na=False), 'data flag'] = 'Remove'
    data.loc[data['FIRSTNAME'].str.contains(patternDel, na=False), 'data flag'] = 'Remove'
    data.loc[data['LASTNAME'].str.contains(patternDel, na=False), 'data flag'] = 'Remove'
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

    data2.loc[data2.Temp == 1, 'data flag'] = 'Remove'
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

directory = 'E:/A-plan December/A-Plan December Renewal Data/'
directory_2 = 'E:/A-plan December/A-Plan December Final Data/'
onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"

#directory = 'C:/Users/Peter Chaplin\OneDrive - ESB Connect/A-plan October/A-Plan October Renewal Data/'
#directory_2 = 'C:/Users/Peter Chaplin/OneDrive - ESB Connect/A-plan October/A-Plan October Final Data/'
#onedrive="C:/Users/Peter Chaplin/OneDrive - ESB Connect/"

month = 'Dec19' 

files = ['00005A_ORG23790_A_Plan_December_Car_insurance_Branch.csv',\
'00005B_ORG23790_A_Plan_December_Car_insurance_National.csv',\
'00006_ORG23790_A_Plan_December_Home_insurance.csv']

#home_Ins_1 = '00006_ORG23434_A_Plan_September_Home_insurance.csv'
#home_Ins_2 = '00006_ORG23434_A_Plan_September_Home_insurance_TopUp.csv'

#ins_1 = pd.read_csv(directory + home_Ins_1,encoding = "ISO-8859-1",low_memory=False)
#print(ins_1.shape[0])
#ins_2 = pd.read_csv(directory + home_Ins_2,encoding = "ISO-8859-1",low_memory=False)
#print(ins_2.shape[0])
#home_ins = ins_1.append(ins_2)
#home_ins.to_csv(directory + "home_ins__Sept19.csv", index=False)



#Start processing
df2 = pd.read_csv(directory + "A-Plan_Dec19_all_data_flag.csv")
ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")

print('Gross', df2.shape[0])
df2 = df2.drop_duplicates(subset='email', keep='first')


print("All Data Flag", df2.shape[0])

inputcheck = 0
outputcheck = 0
ispstats = pd.DataFrame(['Product','List','Type','Count'])
filecounts = pd.DataFrame(columns=['Product','List','Type','Count'])

for file in files:
    df1 = pd.read_csv(directory + file,encoding = "ISO-8859-1",low_memory=False)
    print(file,df1.shape[0])
    inputcheck = inputcheck + df1.shape[0]
    df3 = pd.merge(df1, df2, left_on='Email', right_on='email', how='left')
    new = df3['Email'].str.split(pat="@", expand=True)
    df3.loc[:,'Left']= new.iloc[:,0]
    df3.loc[:,'Domain'] = new.iloc[:,1]
    df =  pd.merge(df3, ispgroups, left_on='Domain', right_on='Domain', how='left')
    df = df[df['Group'].isin( ['Microsoft']) == False]
    
    if "Branch" in file:
        product = "LocalCar"
    elif "National" in file:    
        product = "NationalCar"
    else:
        product = "Home"
    
    # get domain analysis stats
    
    e= pd.DataFrame(df['status'].value_counts()).reset_index()
    e.rename(columns={'index' : 'item', 'status' : 'count'}, inplace=True)
    e.insert(0,'product', product)    
        
    nulls = df[df['data flag'].isnull()]
    print(product, "Nulls",nulls.shape[0])
    a = df['data flag'].value_counts(dropna='False')

    df = remove_invalid_emails(df)
    b = df['data flag'].value_counts() 
    stats = pd.DataFrame.from_dict([[product,'Invalid Emails', str(b[2] - a[2])]])
     
    df = remove_bad_names(df)
    c = df['data flag'].value_counts()
    stats = stats.append(pd.DataFrame.from_dict([[product,'Bad Names', str(c[2] - b[2])]]))
    
    df = remove_temp_domains(df)
    d = df['data flag'].value_counts()
    stats = stats.append(pd.DataFrame.from_dict([[product,'Temp Domains', str(d[2] - c[2])]]))
    #flag = df.loc[:,['Email','is_blacklisted', 'is_banned_word', 'is_banned_domain', 'is_complaint', 'is_hardbounce']]
    
    #generate remove reason file
    removed = df[df['data flag'] == 'Remove']
    to_dropremove = ['TITLE', 'FIRSTNAME', 'LASTNAME',  'Brand', 'A-PLAN_MOTOR',\
       'A-PLAN_HOME', 'A-PLAN_VAN', 'A-PLAN_BRANCH_TELE', 'A-PLAN_ADDRESS',\
       'A-PLAN_ADDRESS2', 'A-PLAN_BRANCH_LINK', 'A-PLAN_BRANCH_MANAGE',\
       'A-PLAN_CTA1LINK', 'CODE', 'DT_EmailSource', 'list_id', 'source_url',\
       'title', 'first_name', 'last_name', 'id', 'email', 'optin_date',\
       'is_duplicate', 'is_ok',  'domain','user_status', 'last_open', 'last_click', 'system_created',\
       'master_filter', 'import_filter', 'email_id',  'rtotal','data flag', ]
    removed.drop(to_dropremove, axis=1, inplace=True)
    print('Removed', removed.shape)
    print(removed.columns)
    
    #for item in ['is_blacklisted', 'is_banned_word', 'is_banned_domain', 'is_complaint', 'is_hardbounce']:
    #    f = df[item].value_counts()[1]
    #    stats = stats.append(pd.DataFrame.from_dict([[product,item[3:], str(f)]]))
    
    #stats.columns = ['product', 'item', 'count']
    #stats = stats.append(e)
    #print(stats)           
    #print([df[col].values.sum(axis=0) for col in ('is_blacklisted', 'is_banned_word', 'is_banned_domain', 'is_complaint', 'is_hardbounce')]) 
    
    df['CODEDT'] = df['CODE'] + df['DT_EmailSource'].map(str)
    df['CODEDT'] = df['CODEDT'].str.replace('.0', '')
    to_dropcols = ['list_id', 'source_url', 'title', 'first_name', 'last_name', 'id', 'email', 'optin_date', 'is_duplicate', 'is_ok', \
    'is_blacklisted', 'is_banned_word', 'is_banned_domain', 'is_complaint', 'is_hardbounce', 'domain', 'user_status', 'last_open',\
    'last_click', 'system_created', 'master_filter', 'import_filter', 'email_id', 'status', 'rtotal', 'Temp']
    df.drop(to_dropcols, axis=1, inplace=True)
    
    df['A-PLAN_ADDRESS'].replace(",", " ", inplace=True)
    df['A-PLAN_ADDRESS2'].replace(",", " ", inplace=True)
    
    print('Data flags', df['data flag'].value_counts())
    
    for i, x in df.groupby(['CODEDT']):
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
        elif i == 'CFnan':
            i = 'SSU'
        else:
            i = i
        for j, m in x.groupby('data flag'):
            print(i,j, m.shape[0])
            counts = pd.DataFrame([[product,i,j,str(m.shape[0])]],columns=['Product','List','Type','Count'])
            filecounts = filecounts.append(counts)
            outputcheck = outputcheck + m.shape[0]
            #ispsplit = pd.DataFrame(report_ISP_groups(m[['Email']],ispgroups))
            #ispsplit.loc['Product'] = product
            #ispsplit.loc['List'] = i
            #ispsplit.loc['Segment'] = j
            #ispstats = ispstats.append(ispsplit)
            to_mdropcols = ['data flag', 'CODEDT', 'Group', 'Left','Domain']
            m.drop(to_mdropcols, axis=1, inplace=True)
            m.to_csv(directory_2 + product + "_" + i + "_"+ j + month + "_non_microsoft.csv", index=False)
            

delta = inputcheck - outputcheck
print("Input Check: ", inputcheck, "Output Check: ", outputcheck, "Delta: ", delta)
print(ispstats)
print(filecounts)
ispstats.to_csv(directory_2 +  "ipstats_non_microsoft_"+ month + ".csv", index=True)
filecounts.to_csv(directory_2 + "filecounts_non_microsoft_" + month + ".csv", index=False)