import pandas as pd 

directory = 'E:/A-Plan April 2020/A-Plan April Removes/'
file1 = '00006_3902_A_Plan_April_Home_insurance_ESB_removed.csv'
file2 = '00005B_3902_A_Plan_April_Car_insurance_National_ESB_removed.csv'
file3 = '00005A_3902_A_Plan_April_Car_insurance_Branch_ESB_removed.csv'

df1 = pd.read_csv(directory + file1, low_memory=False)
df2 = pd.read_csv(directory + file2, low_memory=False)
df3 = pd.read_csv(directory + file3, low_memory=False)

df = pd.concat([df1,df2,df3], axis = 0)

print(df1.shape)
print(df2.shape)
print(df3.shape)
print(df.shape)
df4 = pd.read_csv(directory + 'all_removes.csv' , low_memory=False)
counts = df4['status'].value_counts().rename_axis('Reason').reset_index(name='Counts')
counts.to_csv(directory + 'remove_ reasons_v2.csv', index= False)
