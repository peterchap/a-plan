import pandas as pd 
from sqlalchemy import create_engine

directory = 'E:/A-Plan/A-plan November/A-Plan November Renewal Data/'
directory2 = 'E:/A-Plan/A-Plan November/'
postcodedir = 'E:/Post Codes/'
outputdirectory = 'E:/A-Plan/'

statusfile = 'A_Plan_Nov20_newstatus_Stage1Complete_v2.csv'
openers = 'A-Plan_Nov19_openers_20191028.csv'
hbproduction = 'decisionproduction.csv'
hbcleaning = 'decision_cleaning.csv'
hbnov2019 = 'A-Plan Campiagn Hard Bounces Nov19.csv'
postcodes = 'postcodes.csv'

files = ['00005B_ORG23692_A_Plan_November_Car_insurance_National.csv',\
    '00005A_ORG23692_A_Plan_November_Car_insurance_Branch.csv',\
    '00006_ORG23692_A_Plan_November_Home_insurance.csv']

month ='November'
year = '2019'

table1 = 'aplanmatch'
db = 'insurance.db'
sqlite_engine = create_engine('sqlite:///' + outputdirectory + db)

df2 = pd.read_csv(directory + statusfile,usecols=['email','user_status','email_id','status'])
df2 = df2[(df2['user_status'] == 'Mailable') & (df2['status'].isin(['OK', 'Not Checked']))]

#df3 = pd.read_csv(directory2 + openers, encoding = "ISO-8859-1",names=['email','campaign','opened','Time'], usecols=['email','Date'])
#df3 = pd.read_csv(directory2 + openers, encoding = "ISO-8859-1", names=['email','opened'])
#df3.rename(columns={'Date' : 'opened'}, inplace=True)
#df3.drop_duplicates(subset=['email'],keep='last',inplace=True)
'''
df4 = pd.read_csv(directory2 + hbproduction, usecols=['m_To', 'Status'])
df4.rename(columns={'m_To' : 'email'}, inplace=True)
df4.loc[df4['Status'] == 'HARD']


df5 = pd.read_csv(directory2 + hbcleaning, names=['email','IP','sender','code','code2', 'Status'], usecols=['email','Status'])
#df5 = pd.read_csv(directory2 + hbcleaning, usecols=['m_To', 'Status'])
#df5.rename(columns={'m_To' : 'email'}, inplace=True)
df5.loc[df5['Status'] == 'HARD']

hardbounces = pd.concat([df4,df5],ignore_index=True)
hardbounces.drop_duplicates(subset=['email'],keep='last',inplace=True)
'''
hardbounces = pd.read_csv(directory2 + hbnov2019, usecols=['email'])
hardbounces.drop_duplicates(subset=['email'],keep='last',inplace=True)

#df2 = df2.merge(df3, on = 'email', how='left')
fullstatus = (df2.merge(hardbounces, on='email', how='left', indicator=True)
     .query('_merge == "left_only"')
     .drop('_merge', 1))

regions = pd.read_csv(postcodedir + postcodes,low_memory=False, usecols=['Postcode', 'County','Country'])
regions.columns = map(str.lower, regions.columns)

cols = ['TITLE', 'FIRSTNAME', 'LASTNAME', 'Email', 'A-PLAN_ADDRESS2']
for file in files:
    df1 = pd.read_csv(directory + file,encoding = "ISO-8859-1",low_memory=False,usecols=cols)
    df1['Postcode'] = df1['A-PLAN_ADDRESS2'].str[-7:]
    df1.columns = map(str.lower, df1.columns)
   
    print(file,df1.shape[0])
    df = pd.merge(df1, fullstatus, left_on='email', right_on='email', how='inner')
    df = pd.merge(df, regions, on='postcode', how='left')
    df.drop(columns=['a-plan_address2','user_status', 'status','Status'], inplace=True)
    df['renewmonth'] = month
    df['year'] = year
    if "Branch" in file:
        df['product'] = "Car"
    elif "National" in file:    
        df['product'] = "Car"
    else:
        df['product'] = "Home"
    #pre-2020
    df['opened'] = ''
    print(df.columns)

    finaldf = df[['product','year', 'renewmonth', 'postcode','county','country', 'title', 'firstname',\
         'lastname', 'email_id', 'email','opened']]
    print(finaldf.shape)
    print(finaldf.head(5))
    finaldf.to_sql(table1, sqlite_engine, if_exists='append',chunksize=500, index=False)


