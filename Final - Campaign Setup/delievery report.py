import pandas as pd
import os 

directory = 'E:/A-Plan/A-Plan February 2021/'
finaldir = 'A-Plan February Final Data/'
oemprofile = 'oempro_data_export_20210124.csv'
cleanstats = 'oempro_cleaning_feb21_Delivery_stats.csv'
prodstats = 'oempro_production_feb21_Delivery_stats.csv'
openstats = 'oempro_data_export_openers_20210124.csv'
month = 'feb21'

df1 = pd.read_csv(directory + oemprofile,encoding ='ISO-8859-1')
df2 = pd.read_csv(directory + cleanstats,encoding ='ISO-8859-1', usecols = ['m_CampaignId', 'HARD', 'SOFT'])
df3 = pd.read_csv(directory + prodstats,encoding ='ISO-8859-1',usecols = ['m_CampaignId', 'HARD', 'SOFT'])

df1['Group'] = ''
df1['Product'] = ''
df1['List'] = ''
df1['Filter'] = ''
df1['MSP'] = ''


df1 = df1[~df1['CampaignName'].str.contains('Seed|ReAd',case=True, na=False)]
df1 = df1[df1['CampaignName'].str.contains('AUTOCAMPAIGN:',case=True, na=False)]

df1.loc[df1['CampaignName'].str.contains('LIST: DPH_',case=True), ['Group']] = 'DPH'
df1.loc[df1['CampaignName'].str.contains('LIST: R_',case=True), ['Group']] = 'R'

df1.loc[df1['CampaignName'].str.contains('_Home_',case=True), ['Product']] = 'Home'
df1.loc[df1['CampaignName'].str.contains('_Car_Local_',case=True), ['Product']] = 'LocalCar'
df1.loc[df1['CampaignName'].str.contains('_Car_National_',case=True), ['Product']] = 'NationalCar'

df1.loc[df1['CampaignName'].str.contains('_GBO_',case=True), ['List']] = 'GBO'
df1.loc[df1['CampaignName'].str.contains('_MPO1_',case=True), ['List']] = 'MPO1'
df1.loc[df1['CampaignName'].str.contains('_MPO2_',case=True), ['List']] = 'MPO2'
df1.loc[df1['CampaignName'].str.contains('_SSU_',case=True), ['List']] = 'SSU'
df1.loc[df1['CampaignName'].str.contains('_TPL_',case=True), ['List']] = 'TPL'
df1.loc[df1['CampaignName'].str.contains('_UKBO_',case=True), ['List']] = 'UKBO'

df1.loc[df1['CampaignName'].str.contains('_active_',case=True), ['Filter']] = 'active'
df1.loc[df1['CampaignName'].str.contains('_repossess_',case=True), ['Filter']] = 'repossess'
df1.loc[df1['CampaignName'].str.contains('_cleaning_',case=True), ['Filter']] = 'cleaning'

df1.loc[df1['CampaignName'].str.contains('_Non_Microsoft',case=True), ['MSP']] = 'Other'
df1.loc[~df1['CampaignName'].str.contains('_Non_Microsoft',case=True), ['MSP']] = 'Microsoft'

df1['ID'] = 'OemPro-' + df1['CampaignID'].astype(str)

dropcols = ['CampaignID','CampaignName','CampaignShortName', 'Sender', 'MTA']
df1.drop(columns=dropcols, inplace=True)

# Create add send date and openers files

sd1 = df1[['Group', 'Product', 'List', 'Filter', 'MSP', 'SendDate', 'Total']].copy()
sd1.drop_duplicates(subset=['Group', 'Product', 'List', 'Filter', 'MSP'],\
    keep='first', inplace=True, ignore_index=True)

#Read campaign files
basepath = directory + finaldir
home = pd.DataFrame()
car = pd.DataFrame()
with os.scandir(basepath) as files:
    for f in files:
        df0 = pd.read_csv(basepath + f.name,encoding ='utf-8', usecols=['URN','Email'])
        file = f.name
        x = file.split('_')[:5]
        df0['Group'] = x[0]
        df0['Product'] = x[1]
        df0['List'] = x[2]
        df0['Filter'] = x[3]
        if x[4] == 'Microsoft':
            df0['MSP'] = x[4]
        else:
            df0['MSP'] = 'Other'
        df0 = df0.merge(sd1, left_on=['Group', 'Product', 'List', 'Filter', 'MSP'],\
        right_on=['Group', 'Product', 'List', 'Filter', 'MSP'], how='left')
        df0.drop(columns=[], inplace=True)
        if x[1]  == 'Home':
            home = home.append(df0)
        else:
            car = car.append(df0)

# Merge opener file

openers = pd.read_csv(directory + openstats, encoding ='utf-8', sep='\t',\
    names=['Email', 'Campign_ID','Campaign', 'Opened'], usecols=['Email','Opened'])

home = home.merge(openers, on='Email', how='left')
car = car.merge(openers, on='Email', how='left')
home.drop(columns=['Product', 'Filter', 'MSP', 'Total'], inplace=True)
car.drop(columns=['Product', 'Filter', 'MSP', 'Total'], inplace=True)


home.to_csv(directory + 'A-Plan_Home_send_date_'+ month +'.csv', index=False)
car.to_csv(directory + 'A-Plan_Car_send_date_'+ month +'.csv', index=False)


# Create Delivery report

df4 = pd.concat([df2,df3])
df5 = df1.merge(df4, left_on='ID', right_on='m_CampaignId', how='left')
df5.fillna(0, inplace=True)
df5['delta'] = df5['Bounced'] - (df5['HARD'] + df5['SOFT'])
df5['Delivered'] = df5['Delivered'] + df5['delta']
df5['Deferred'] = df5['Expired'] + df5['Queued']
print(df5)
df6 =df5.groupby(['Group','Product','List','Filter', 'MSP']).sum().reset_index()

dropcols = ['Bounced', 'Queued', 'Expired', 'delta']
df6.drop(columns=dropcols, inplace=True)
rename = {'Total' : 'Sent' }
df6.rename(columns=rename, inplace=True)
df6 = df6.append(df6.sum(numeric_only=True), ignore_index=True)
df6['pc_delivered'] = df6['Delivered'] / df6['Sent']
df6['pc_Hard Bounce'] = df6['HARD'] / df6['Delivered']
df6['pc_Open'] = df6['UniqueOpen'] / df6['Delivered']
df6['CTR'] = df6['UniqueClick'] / df6['UniqueOpen']
print(df6.columns)
df7 = df6[['Group', 'Product', 'List', 'Filter', 'MSP', 'Sent', 'Delivered',\
    'pc_delivered', 'HARD', 'pc_Hard Bounce', 'SOFT','Filtered', 'Deferred',\
    'TotalOpen', 'UniqueOpen', 'pc_Open', 'TotalClick', 'UniqueClick', 'CTR']]

df7.to_csv(directory + 'delivery_report.csv', index=False)
