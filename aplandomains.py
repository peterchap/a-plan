import pandas as pd
from disposable_email_domains import blocklist

df = pd.read_csv("C:/Users/Peter/Downloads/A-Plan July Renewal Data matched.csv")
df.drop_duplicates(subset='email', keep='first',inplace=True)

onedrive="C:/Users/Peter/OneDrive - Email Switchboard Ltd/"

ispgroup = pd.read_csv(onedrive +'ISP Group domains.csv')
df = pd.merge(df, ispgroup,left_on='domain', right_on='Domain', how='left')
df['Group'].fillna("Other", inplace = True)

print(list(df.columns.values))
print(df['Group'].value_counts())

cleaning = df[df['Data Flag']=="In Cleaning"]
print(cleaning['Group'].value_counts())

production = df[df['Data Flag']=="In Production"]
print(production['Group'].value_counts())

# remove email temp domains
domains = df[df['Group']=="Other"]['Domain']
domains.drop_duplicates(inplace=True)
d =[]
for domain in domains:
    d.append(( domain, domain in blocklist))

e = pd.DataFrame(d, columns=('domain', 'Temp'))
df = pd.merge(df, e, on='domain', how='left')
df = df[df.Temp != 1]

print("Removed Temp Domains", df.shape)
