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
    data['Left']= new[0]
    data['Domain'] = new[1]

    ispdata = pd.merge(data, ispgroup, on='Domain', how='left')
    ispdata['Group'].fillna("Other", inplace = True)
    stat = ispdata['Group'].value_counts()
    return stat

# End of Functions

directory = 'E:/A-plan October/A-Plan October Renewal Data/'

onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"

#directory = 'C:/Users/Peter Chaplin\OneDrive - ESB Connect/A-plan October/A-Plan October Renewal Data/'
#directory_2 = 'C:/Users/Peter Chaplin/OneDrive - ESB Connect/A-plan October/A-Plan October Final Data/'
#onedrive="C:/Users/Peter Chaplin/OneDrive - ESB Connect/"

month = 'Oct19'


file = '00005A_ORG23531_A_Plan_October_Car_insurance_Branch.csv'




#Start processing
df2 = pd.read_csv(directory + "aplan_Oct.all_data_flag.csv")
ispgroups = pd.read_csv(onedrive+'ISP Group domains.csv',encoding = "ISO-8859-1")

print('Gross', df2.shape[0])
df2 = df2.drop_duplicates(subset='email', keep='first')


print("All Data Flag", df2.shape[0])

inputcheck = 0
outputcheck = 0
ispstats = pd.DataFrame()
filecounts = pd.DataFrame(columns=['File','List','Type','Count'])

df1 = pd.read_csv(directory + file,encoding = "ISO-8859-1",low_memory=False)
print(file,df1.shape[0])

    
df = pd.merge(df1, df2, left_on='Email', right_on='email', how='left')

print(df['is_blacklisted'].value.counts())
test = df[df['is_blacklisted'] =='True']
print('test', test.shape)
test1 = test[test['name'] == 'BLACKLISTED']
print('test1', test1.shape)

if "Branch" in file:
    product = "LocalCar"
elif "National" in file:
    product = "NationalCar"
else:
    product = "Home"
    
e= pd.DataFrame(df['name'].value_counts()).reset_index()
e.rename(columns={'index' : 'item', 'name' : 'count'}, inplace=True)
e.insert(0,'product', product)
print(e)



    
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
 


    
    #print([df[col].values.sum(axis=0) for col in ('is_blacklisted', 'is_banned_word', 'is_banned_domain', 'is_complaint', 'is_hardbounce')]) 
    

    
#flag = df.loc[:,['Email','is_blacklisted', 'is_banned_word', 'is_banned_domain', 'is_complaint', 'is_hardbounce']]
#b = df['is_hardbounce'].value_counts().reset_index()

#b.columns = ['flag', 'count']
for item in ['is_blacklisted', 'is_banned_word', 'is_banned_domain', 'is_complaint', 'is_hardbounce']:
        f = df[item].value_counts()[1]
        stats = stats.append(pd.DataFrame.from_dict([[product,item[3:], str(f)]]))         

stats.columns = ['product', 'item', 'count']
stats = stats.append(e)
print(stats)   
            
#print(ispstats.shape)
#print(ispstats.columns)
#ispstats.to_csv(directory_2 +  "ipstats_"+ month + ".csv", index=True)
#filecounts.to_csv(directory_2 + "filecounts_" + month + ".csv", index=False)