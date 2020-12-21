import pandas as pd 
import os
import glob

directory = 'E:/A-Plan/A-Plan October 2020/A-Plan October Removes/'
directory2 = 'E:/A-Plan/A-Plan October 2020/'

month = 'Oct20'

os.chdir(directory)

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
print('files', len(all_filenames))
count = 0

for file in all_filenames:
    data = pd.read_csv(file, encoding = "ISO-8859-1", low_memory=False)
    count = count + data.shape[0]

print('Total', count)
#combine all files in the list
df = pd.concat([pd.read_csv(f, encoding = "ISO-8859-1", low_memory=False) for f in all_filenames ])

print(df.shape)

value_counts = df['status'].value_counts(dropna=False)
counts = pd.DataFrame(value_counts.rename_axis('Category').reset_index(name='Unique Emails'))
print(counts)

counts.to_csv(directory2 + "Remove_reason_counts" + month + ".csv", index=True)

'''
dph = pd.DataFrame()

for file in all_filenames:
    if "DPH" in file:
        dph


#export to csv

print("Completed Successfully")
'''