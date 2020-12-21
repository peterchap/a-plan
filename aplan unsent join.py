import pandas as pd

file = 'C:/Users/Peter/Downloads/file_listing.csv'
directory = 'C:/Users/Peter/Downloads/A-Plan August NotSent Data/'

df = pd.read_csv(file,encoding = "ISO-8859-1",low_memory=False)

for a,b in zip(df['Cleaning'], df['OEMPRO']):
    left = pd.read_csv(a,encoding = "ISO-8859-1",low_memory=False)
    right = pd.read_csv(b,encoding = "ISO-8859-1",low_memory=False)
    c = (left.merge(right, left_on='Email', right_on='Email Address', how='left', indicator=True)
             .query('_merge == "left_only"')
             .drop('_merge', 1))
    print(b,c.shape[0])
    
    d = len(b) - b.find('\\')
    e = (b[-(d-1):])
    c.to_csv(directory+e, index=False)