import pandas as pd

directory = "E:/A-Plan/A-Plan May 2021/"
onedrive = "C:/Users/Peter/OneDrive - Email Switchboard Ltd/Data Cleaning Project/"

file = "click_report_may21.csv"
statusfile = "domain_status.csv"

df1 = pd.read_csv(directory + file, encoding="utf-8", low_memory=False)
df2 = pd.read_csv(
    onedrive + statusfile,
    encoding="utf-8",
    low_memory=False,
    usecols=["name", "mxdomain", "owner"],
)

new = df1["Email"].str.split(pat="@", expand=True)
df1["domain"] = new[1]
print(df1.shape)
print(df2.shape)

clickmx = df1.merge(df2, left_on="domain", right_on="name", how="left")
print(clickmx.shape)
clickmx.to_csv(directory + "A-Plan_click_detail" + ".csv", index=False)
