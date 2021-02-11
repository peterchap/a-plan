import pandas as pd

directory1 = 'E:/A-Plan/A-Plan March 2021/A-Plan March Renewal Data/'
directory2 = 'E:/A-Plan/A-Plan March 2021/A-Plan March Final Data/'
directory = 'E:/A-Plan/A-Plan March 2021/'

month = 'Mar21'

file1 = '4140_A_Plan_Mar_Car_insurance_Branch_ESB_R.csv'
file2 = '4140_A_Plan_Mar_Car_insurance_National_ESB_R.csv'
file3 = '4140_A_Plan_Mar_Home_insurance_ESB_R.csv'
file4 = '4140_A_Plan_Mar_Home_insurance_ESB_DPH.csv'


df1 = pd.read_csv(directory1 + file1,encoding = "ISO-8859-1",low_memory=False)
print(df1.shape)

df2 = pd.read_csv(directory1 + file2,encoding = "ISO-8859-1",low_memory=False)
print(df2.shape)

df3 = pd.read_csv(directory1 + file3,encoding = "ISO-8859-1",low_memory=False)
print(df3.shape)

df4 = pd.read_csv(directory1 + file4,encoding = "ISO-8859-1",low_memory=False)
print(df4.shape)

duplicates = pd.DataFrame(columns = ['product', 'count'])
data1 = df1.merge(df2, on='EMAIL', how='inner')
print( "Car Duplicates", data1.shape[0])
duplicates = duplicates.append(pd.Series(['Car - Branch-National' ,  data1.shape[0]],index=duplicates.columns), ignore_index=True)

data2 = df1.merge(df3, on='EMAIL', how='inner')
print( "CB Home Duplicates", data2.shape[0])  
duplicates = duplicates.append(pd.Series(['Branch-Home' ,  data2.shape[0]],index=duplicates.columns), ignore_index=True)

data3 = df2.merge(df3, on='EMAIL', how='inner')
print( "CN Home Duplicates", data3.shape[0])
duplicates = duplicates.append(pd.Series(['National-Home' ,  data3.shape[0]],index=duplicates.columns), ignore_index=True)

print(duplicates)

dphdups = pd.DataFrame(columns = ['product', 'count'])

data4 = df1.merge(df4, on='EMAIL', how='inner')
print( "CB Home Duplicates", data4.shape[0])  
dphdups = dphdups.append(pd.Series(['Branch-Home' ,  data4.shape[0]],index=duplicates.columns), ignore_index=True)

data5 = df2.merge(df4, on='EMAIL', how='inner')
print( "CN Home Duplicates", data5.shape[0])
dphdups = dphdups.append(pd.Series(['National-Home' ,  data5.shape[0]],index=duplicates.columns), ignore_index=True)

data6 = df3.merge(df4, on='EMAIL', how='inner')
print( "DPH-R Home Duplicates", data6.shape[0])
dphdups = dphdups.append(pd.Series(['DPH-R-Home' ,  data6.shape[0]],index=duplicates.columns), ignore_index=True)

print(dphdups)

duplicates.to_csv(directory + "R_duplicates" + month +".csv", index = False)
dphdups.to_csv(directory + "DPH_duplicates" + month +".csv", index = False)