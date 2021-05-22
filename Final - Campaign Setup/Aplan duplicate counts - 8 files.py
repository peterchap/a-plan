import pandas as pd

directory1 = "E:/A-Plan/A-Plan June 2021/A-Plan June Renewal Data/"
directory2 = "E:/A-Plan/A-Plan June 2021/A-Plan June Final Data/"
directory = "E:/A-Plan/A-Plan June 2021/"

month = "JUN21"

file1 = "4206_A_Plan_Jun_Car_insurance_Branch_ESB_DPM.csv"
file2 = "4206_A_Plan_Jun_Car_insurance_Branch_ESB_MG1.csv"
file3 = "4206_A_Plan_Jun_Car_insurance_Branch_ESB_MUK.csv"
file4 = "4206_A_Plan_Jun_Car_insurance_National_ESB_DPM.csv"
file5 = "4206_A_Plan_Jun_Car_insurance_National_ESB_MG1.csv"
file6 = "4206_A_Plan_Jun_Car_insurance_National_ESB_MUK.csv"
file7 = "4206_A_Plan_Jun_Home_insurance_ESB_DPH.csv"
file8 = "4206_A_Plan_Jun_Home_insurance_ESB_H1.csv"


df1 = pd.read_csv(
    directory1 + file1, encoding="ISO-8859-1", low_memory=False, usecols=["EMAIL"]
)
print(df1.shape)

df2 = pd.read_csv(
    directory1 + file2, encoding="ISO-8859-1", low_memory=False, usecols=["EMAIL"]
)
print(df2.shape)

df3 = pd.read_csv(
    directory1 + file3, encoding="ISO-8859-1", low_memory=False, usecols=["EMAIL"]
)
print(df3.shape)

df4 = pd.read_csv(
    directory1 + file4, encoding="ISO-8859-1", low_memory=False, usecols=["EMAIL"]
)
print(df4.shape)

df5 = pd.read_csv(
    directory1 + file5, encoding="ISO-8859-1", low_memory=False, usecols=["EMAIL"]
)
print(df5.shape)

df6 = pd.read_csv(
    directory1 + file6, encoding="ISO-8859-1", low_memory=False, usecols=["EMAIL"]
)
print(df6.shape)

df7 = pd.read_csv(
    directory1 + file7, encoding="ISO-8859-1", low_memory=False, usecols=["EMAIL"]
)
print(df7.shape)

df8 = pd.read_csv(
    directory1 + file8, encoding="ISO-8859-1", low_memory=False, usecols=["EMAIL"]
)
print(df8.shape)

duplicates = pd.DataFrame(columns=["product", "count"])
data1 = pd.concat([df1, df2, df3])
print("Car Branch", data1.shape[0])
CBdedup = data1.drop_duplicates()
cbdups = data1.shape[0] - CBdedup.shape[0]
print(cbdups)

data2 = pd.concat([df4, df5, df6])
print("Car National", data2.shape[0])
CNdedup = data2.drop_duplicates()
cndups = data2.shape[0] - CNdedup.shape[0]
print(cndups)

data3 = pd.concat([df7, df8])
print("Home", data1.shape[0])
homededup = data3.drop_duplicates()
homedups = data3.shape[0] - homededup.shape[0]
print(homedups)

duplicates = pd.DataFrame(columns=["product", "count"])

data4 = data1.merge(data2, on="EMAIL", how="inner")
print("CB CN Duplicates", data4.shape[0])
duplicates = duplicates.append(
    pd.Series(["Branch-National", data4.shape[0]], index=duplicates.columns),
    ignore_index=True,
)

data5 = df1.merge(df3, on="EMAIL", how="inner")
print("CB Home Duplicates", data5.shape[0])
duplicates = duplicates.append(
    pd.Series(["Branch-Home", data5.shape[0]], index=duplicates.columns),
    ignore_index=True,
)

data6 = data2.merge(data3, on="EMAIL", how="inner")
print("National Home Duplicates", data6.shape[0])
duplicates = duplicates.append(
    pd.Series(["National-Home", data6.shape[0]], index=duplicates.columns),
    ignore_index=True,
)

print(duplicates)

duplicates.to_csv(directory + "product_duplicates" + month + ".csv", index=False)

