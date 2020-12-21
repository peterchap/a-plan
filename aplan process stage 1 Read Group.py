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
    
directory = 'C:/Users/Peter Chaplin/Downloads/'
onedrive="C:/Users/Peter Chaplin/OneDrive - ESB Connect/"
month = 'Oct19'
file = 'Aplan data_Oct19'

stats = pd.DataFrame(columns=['File',  'Count'])
df = pd.DataFrame()
filecounts = pd.DataFrame(columns=['File','List','Count'])


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
        elif i == 'Latchnan':
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

print("Removed invalid email addresses", df.shape)


to_dropcols = [ 'A-PLAN_MOTOR', 'A-PLAN_HOME', 'A-PLAN_VAN', 'A-PLAN_BRANCH_TELE',\
'A-PLAN_ADDRESS', 'A-PLAN_ADDRESS2', 'A-PLAN_BRANCH_LINK', 'A-PLAN_BRANCH_MANAGE',\
'A-PLAN_CTA1LINK', 'DT_EmailSource', 'is_valid_email']

df.drop(to_dropcols, axis=1, inplace=True)
df.rename(columns={'Brand': 'Source_URL', 'CODE': 'List_ID'}, inplace=True)

new = df["Email"].str.split(pat="@", expand=True)
df['Domain'] = new[1]

df['TITLE'] = df['TITLE'].map(lambda x: charreplace(x))
print(df['TITLE'].value_counts())
#drop_title = ['Prince','Princess','Duke','Consuelo','Mr156227','Cardinal']
#df = df[~df['TITLE'].isin(drop_title)]

#print(df.head(5))
#df.to_csv(directory + month + "all.csv", index=False)
#stats.to_csv(directory + "Aplan_" + month + "_stats.csv", index=False)
#filecounts.to_csv(directory_2 + "filecounts_" + month + ".csv", index=False)