import pandas as pd

from validate_email import validate_email

def charreplace(textstring):
    valstoreplace = ['&' ,'< Select >','0','.','null' ]
    for c in valstoreplace:
        if isinstance(textstring, float):
            textstring = 'null'
        else:
            textstring = textstring.replace(c, '')
    return textstring
directory = 'E:/A-plan October/A-Plan October Renewal Data/'
files = ['00005A_ORG23531_A_Plan_October_Car_insurance_Branch.csv',\
'00005B_ORG23531_A_Plan_October_Car_insurance_National.csv',\
'00006_ORG23531_A_Plan_October_Home_insurance.csv']

stats = pd.DataFrame(columns=['File', 'Description', 'Count'])
df = pd.DataFrame()

for file in files:
    a = pd.read_csv(directory + file,encoding = "ISO-8859-1",low_memory=False)
    print(a.shape)
    stats = stats.append(pd.Series([file, 'Gross Volume', a.shape[0]],index=stats.columns), ignore_index=True)
    df = pd.concat([df,a])

stats = stats.append(pd.Series(['Merged File', 'Gross Volume', df.shape[0]],index=stats.columns), ignore_index=True)
print(df.shape)
print(stats)

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

print(df.head(5))
df.to_csv(directory + "sepall.csv", index=False)
stats.to_csv(directory + "Aplan_Sep19_stats.csv", index=False)
