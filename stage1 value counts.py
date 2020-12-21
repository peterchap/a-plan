import pandas as pd

directory = 'E:/MDEG/'

file = 'main.csv'

df = pd.read_csv(directory + file,encoding = "utf-8",low_memory=False)

print(df['date_added'].value_counts(dropna=False))
print(df.columns)

result = pd.DataFrame(df['date_added'].value_counts()).reset_index()
result.rename(columns={'index' : 'List', 'Unique' : 'count'}, inplace=True)
print(result)

#result.to_csv(directory + "A_Plan_" + "status_counts.csv", index=False)

