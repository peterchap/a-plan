import pandas as pd
import os

directory = "E:/A-Plan/A-Plan July 2021/"
finaldir = "A-Plan July Final Data/"
oemprofile = "oempro_data_export_20210617.csv"
cleanstats = "oempro_cleaning_jul21_Delivery_stats.csv"
prodstats = "oempro_production_jul21_Delivery_stats.csv"
openstats = "oempro_data_export_openers_20210617.csv"
clickstats = (
    "oempro_data_export_clicks_20210617.csv"  # convert to csv and save as  utf-8
)
month = "jul21"

df1 = pd.read_csv(directory + oemprofile, encoding="ISO-8859-1")
df2 = pd.read_csv(
    directory + cleanstats,
    encoding="ISO-8859-1",
    usecols=["m_CampaignId", "HARD", "SOFT"],
)
df3 = pd.read_csv(
    directory + prodstats,
    encoding="ISO-8859-1",
    usecols=["m_CampaignId", "HARD", "SOFT"],
)

df1["Group"] = ""
df1["Product"] = ""
df1["List"] = ""
df1["Filter"] = ""
df1["MSP"] = ""


df1 = df1[
    ~df1["CampaignName"].str.contains(
        ": Aplan|test|SEED|Seed|ReAd", case=True, na=False
    )
]
print("check", df1.shape)
df1 = df1[df1["CampaignName"].str.contains("AUTOCAMPAIGN:", case=True, na=False)]

df1.loc[df1["CampaignName"].str.contains("LIST: DPH_", case=True), ["Group"]] = "DPH"
df1.loc[df1["CampaignName"].str.contains("LIST: DPM_", case=True), ["Group"]] = "DPM"
df1.loc[df1["CampaignName"].str.contains("LIST: H1_", case=True), ["Group"]] = "H1"
df1.loc[df1["CampaignName"].str.contains("LIST: MG_", case=True), ["Group"]] = "MG"
df1.loc[df1["CampaignName"].str.contains("LIST: MUK_", case=True), ["Group"]] = "MUK"

df1.loc[df1["CampaignName"].str.contains("_Home_", case=True), ["Product"]] = "Home"
df1.loc[
    df1["CampaignName"].str.contains("_LocalCar_", case=True), ["Product"]
] = "LocalCar"
df1.loc[
    df1["CampaignName"].str.contains("_NationalCar_", case=True), ["Product"]
] = "NationalCar"

df1.loc[df1["CampaignName"].str.contains("_GBO_", case=True), ["List"]] = "GBO"
df1.loc[df1["CampaignName"].str.contains("_MPO1_", case=True), ["List"]] = "MPO1"
df1.loc[df1["CampaignName"].str.contains("_MPO2_", case=True), ["List"]] = "MPO2"
df1.loc[df1["CampaignName"].str.contains("_SSU_", case=True), ["List"]] = "SSU"
df1.loc[df1["CampaignName"].str.contains("_TPL_", case=True), ["List"]] = "TPL"
df1.loc[df1["CampaignName"].str.contains("_UKBO_", case=True), ["List"]] = "UKBO"

df1.loc[df1["CampaignName"].str.contains("_active_", case=True), ["Filter"]] = "active"
df1.loc[
    df1["CampaignName"].str.contains("_repossess_", case=True), ["Filter"]
] = "repossess"
df1.loc[
    df1["CampaignName"].str.contains("_cleaning_", case=True), ["Filter"]
] = "cleaning"

df1.loc[
    df1["CampaignName"].str.contains("_Non_Microsoft", case=True), ["MSP"]
] = "Other"
df1.loc[
    ~df1["CampaignName"].str.contains("_Non_Microsoft", case=True), ["MSP"]
] = "Microsoft"

df1["ID"] = "OemPro-" + df1["CampaignID"].astype(str)

dropcols = ["CampaignID", "CampaignName", "CampaignShortName", "Sender", "MTA"]
df1.drop(columns=dropcols, inplace=True)

# Create add send date and openers files

sd1 = df1[["Group", "Product", "List", "Filter", "MSP", "SendDate", "Total"]].copy()
sd1.drop_duplicates(
    subset=["Group", "Product", "List", "Filter", "MSP"],
    keep="first",
    inplace=True,
    ignore_index=True,
)

# Read campaign files
basepath = directory + finaldir
home = pd.DataFrame()
car = pd.DataFrame()
with os.scandir(basepath) as files:
    for f in files:
        df0 = pd.read_csv(basepath + f.name, encoding="utf-8", usecols=["URN", "Email"])
        file = f.name
        x = file.split("_")[:5]
        df0["Group"] = x[0]
        df0["Product"] = x[1]
        df0["List"] = x[2]
        df0["Filter"] = x[3]
        if x[4] == "Microsoft":
            df0["MSP"] = x[4]
        else:
            df0["MSP"] = "Other"
        df0 = df0.merge(
            sd1,
            left_on=["Group", "Product", "List", "Filter", "MSP"],
            right_on=["Group", "Product", "List", "Filter", "MSP"],
            how="left",
        )
        df0.drop(columns=[], inplace=True)
        if x[1] == "Home":
            home = home.append(df0)
        else:
            car = car.append(df0)

print("Car", car.shape)
print("Home", home.shape)

# Import  opener  and clicker file

openers = pd.read_csv(
    directory + openstats,
    encoding="utf-8",
    sep="\t",
    names=["Email", "campign_Id", "campaign", "opened"],
    usecols=["Email", "campaign", "opened"],
)

openers = openers[
    ~openers["campaign"].str.contains(
        ": Aplan|test|SEED|Seed|ReAd", case=True, na=False
    )
]
openers.drop(columns=["campaign"], inplace=True)

clickers = pd.read_csv(
    directory + clickstats,
    engine="python",
    encoding="utf-8",
    sep=",",
    names=["Email", "campign_Id", "campaign", "date", "url"],
    usecols=["Email", "campaign", "url"],
)
print(clickers.shape)
clickers["campaign"] = clickers["campaign"].astype(str)
clickers = clickers[
    ~clickers["campaign"].str.contains(
        ": Aplan|test|SEED|Seed|ReAd", case=True, na=False
    )
]
print(clickers.shape)
print(clickers.head())
# clickers.to_csv(directory + "clickers_" + month + ".csv", index=False)
clickers.drop(columns=["campaign"], inplace=True)
clickers["url"] = clickers["url"].astype(str)
test = clickers[clickers["url"].str.contains("facebook")]
print("test", test.shape)
clickers.loc[
    clickers["url"].str.contains("facebook|twitter", regex=True), ["category"]
] = "social"
clickers.loc[
    clickers["url"].str.contains("tracking|utm_source", regex=True), ["category"]
] = "tracking link"

clickers.loc[clickers["url"].str.contains("unsub"), ["category"]] = "unsubscribe"

clickers.loc[clickers["url"].str.contains("privacy"), ["category"]] = "privacy"
clickers.loc[clickers["url"].str.contains("oempro"), ["category"]] = "view message"
clickers.loc[clickers["url"].str.contains("esbtracker"), ["category"]] = "unsubscribe"
clickers.loc[clickers["category"].isnull(), ["category"]] = "other"
print(clickers.shape)
print(clickers["category"].value_counts())
clickers.to_csv(directory + "click_report_" + month + ".csv", index=False)
openclick = openers.merge(clickers, on="Email", how="outer")


openercounts = (
    openers["Email"].value_counts().rename_axis("Email").reset_index(name="total opens")
)

clickok = clickers[clickers["category"] == "tracking link"]
print("tracked", clickok.shape)

clickunique = clickok.drop_duplicates(subset=["url"], keep="last").copy()
clickunique.drop(columns=["url", "category"], inplace=True)
clickok.drop(columns=["url", "category"], inplace=True)
clickokcounts = (
    clickok["Email"]
    .value_counts()
    .rename_axis("Email")
    .reset_index(name="total clicks")
)
clickuniquecounts = (
    clickunique["Email"]
    .value_counts()
    .rename_axis("Email")
    .reset_index(name="unique clicks")
)

clicksum = clickokcounts.merge(clickuniquecounts, on=["Email"], how="left")

openclicksum = openercounts.merge(clicksum, on=["Email"], how="outer")


# Merge click with opener stats

home = home.merge(openclicksum, on="Email", how="left")
car = car.merge(openclicksum, on="Email", how="left")

home.drop(columns=["Product", "Filter", "MSP", "Total"], inplace=True)
car.drop(columns=["Product", "Filter", "MSP", "Total"], inplace=True)


home.to_csv(directory + "A-Plan_Home_send_date_new" + month + ".csv", index=False)
car.to_csv(directory + "A-Plan_Car_send_date_new" + month + ".csv", index=False)


# Create Delivery report

df4 = pd.concat([df2, df3])
df5 = df1.merge(df4, left_on="ID", right_on="m_CampaignId", how="left")
df5.fillna(0, inplace=True)
df5["delta"] = df5["Bounced"] - (df5["HARD"] + df5["SOFT"])
df5["Delivered"] = df5["Delivered"] + df5["delta"]
df5["Deferred"] = df5["Expired"] + df5["Queued"]
print(df5)
df6 = df5.groupby(["Group", "Product", "List", "Filter", "MSP"]).sum().reset_index()

dropcols = ["Bounced", "Queued", "Expired", "delta"]
df6.drop(columns=dropcols, inplace=True)
rename = {"Total": "Sent"}
df6.rename(columns=rename, inplace=True)
df6 = df6.append(df6.sum(numeric_only=True), ignore_index=True)
df6["pc_delivered"] = df6["Delivered"] / df6["Sent"]
df6["pc_Hard Bounce"] = df6["HARD"] / df6["Delivered"]
df6["pc_Open"] = df6["UniqueOpen"] / df6["Delivered"]
df6["CTR"] = df6["UniqueClick"] / df6["UniqueOpen"]
print(df6.columns)
df7 = df6[
    [
        "Group",
        "Product",
        "List",
        "Filter",
        "MSP",
        "Sent",
        "Delivered",
        "pc_delivered",
        "HARD",
        "pc_Hard Bounce",
        "SOFT",
        "Filtered",
        "Deferred",
        "TotalOpen",
        "UniqueOpen",
        "pc_Open",
        "TotalClick",
        "UniqueClick",
        "CTR",
    ]
]

df7.to_csv(directory + "delivery_report.csv", index=False)

