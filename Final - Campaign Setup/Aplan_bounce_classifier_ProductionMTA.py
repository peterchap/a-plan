import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.model_selection import train_test_split

# Import LabelEncoder
from sklearn import preprocessing

# Import scikit-learn metrics module for accuracy calculation
from sklearn import metrics

directory = "E:/A-plan June 2021/"

df = pd.read_csv("C:\\new bounce model data.csv")


print(df.shape)

# Split dataset into training set and test set
X_train, X_test, y_train, y_test = train_test_split(
    df["Desc1"], df["Bounce"], test_size=0.3, random_state=109
)  # 70% training and 30% test

# Creating labelEncoder
le = preprocessing.LabelEncoder()

# Converting string labels into numbers.
bounce_encoded = le.fit_transform(y_train)

# Creating Model
vectorizer = CountVectorizer()
counts = vectorizer.fit_transform(X_train)

classifier = MultinomialNB()
classifier.fit(counts, bounce_encoded)

counts_test = vectorizer.transform(X_test)
y_pred = classifier.predict(counts_test)


# Model Accuracy, how often is the classifier correct?
y_test_encoded = le.fit_transform(y_test)
print("Accuracy:", metrics.accuracy_score(y_test_encoded, y_pred))

directory2 = "E:/A-Plan/A-Plan June 2021/"

delivery = "Aplan_production_MTA_Delivery210521.csv"

df = pd.read_csv(
    directory2 + delivery, sep=",", encoding="ISO-8859-1", low_memory=False
)
print(df.shape)


bounces = df.loc[(df["m_Status"] == "BOUNCED"), ["m_StatusCode", "m_LogEntry", "m_To"]]
print(bounces.shape)
print(list(bounces.columns.values))
examples = bounces["m_LogEntry"].values.astype("U")


bounce_counts = vectorizer.transform(examples)

bounces["Status"] = classifier.predict(bounce_counts)
bounces["Status"] = bounces["Status"].replace([0, 1], ["HARD", "SOFT"])

# Output to CSV file
print("Writing")
print(bounces.head(5))
bounces.to_csv(directory2 + "decisionproductionMTA_jun21.csv", index=False)
print("Processing Complete")
