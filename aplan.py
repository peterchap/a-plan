import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date

from disposable_email_domains import blocklist
from validate_email import validate_email

directory = "C:/Users/Peter/downloads/"
file = "Aplan data_June19.csv"
outputdir = "C:/Users/Peter/downloads/"
df = pd.read_csv(directory+file,usecols=range(0,6),error_bad_lines=False,low_memory=False)

print(list(df.columns.values))
print("Gross file", df.shape)

df.dropna(subset=['Email'],inplace=True)

print("Removed null emails", df.shape)

df['is_valid_email'] = df['Email'].apply(lambda x:validate_email(x))
df = df[df['is_valid_email']]
print("Removed invalid email formats", df.shape)


df = df[df.Email.str.contains("@",na=False)]
char = '\+|\*|\'| |\%|,|\"'
df = df[~df['Email'].str.contains(char,regex=True)]


print("Removed invalid email addresses", df.shape)


new = df["Email"].str.split(pat="@", expand=True)
df['Left']= new[0]
df['Domain'] = new[1]

patternDel = "abuse|backup|cancel|career|comp|crap|email|fake|feedback|free|garbage|generic|hello|invalid|\
junk|penis|person|phruit|postmaster|random|register|shit|signup|spam|stuff|support|survey|test|trash|webmaster|xx"
df = df[~df["Left"].str.contains(patternDel, na=False)]
df = df[~df["Forename"].str.contains(patternDel, na=False)]
df = df[~df["Surname"].str.contains(patternDel, na=False)]
print("Removed bad names", df.shape)

#df = df.sort_values('Email', ascending=False)
#df = df.drop_duplicates(subset='Email', keep='first')

#print("Removed duplicates", df.shape)

onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"

ispgroup = pd.read_csv(onedrive+'ISP Group domains.csv')
df = pd.merge(df, ispgroup, on='Domain', how='left')
df['Group'].fillna("Other", inplace = True)

# remove email temp domains
domains = df[df['Group']=="Other"]['Domain']
domains.drop_duplicates(inplace=True)
d =[]
for domain in domains:
    d.append(( domain, domain in blocklist))

e = pd.DataFrame(d, columns=('Domain', 'Temp'))
df = pd.merge(df, e, on='Domain', how='left')
df = df[df.Temp != 1]
print("Removed Temp Domains", df.shape)


print(list(df.columns.values))
print(df['Group'].value_counts())



df['URL'] =""
df['Joindate'] = ""


#Split data into ISP groups 
for isp, df_Group in df.groupby('Group'):
    af=df.loc[df['Group'] == isp]
    af=af[['Email', 'Title', 'Forename', 'Surname',  'SupplierCode',  'BranchName']]
       print(isp, af.shape)

print("Completed Successfully")