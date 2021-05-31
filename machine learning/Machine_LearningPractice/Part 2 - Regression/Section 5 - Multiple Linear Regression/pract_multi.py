import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
dataset = pd.read_csv("50_Startups.csv")
X = dataset.iloc[:,:-1].values
y = dataset.iloc[:,4].values
# print(y)
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
X_labelencoder = LabelEncoder()
X[:,3] = X_labelencoder.fit_transform(X[:,3])
onehot = OneHotEncoder(categorical_features=[3])
X = onehot.fit_transform(X).toarray()
# print(X)
X = X[:,1:]
from sklearn.model_selection import train_test_split
X_train,X_test,Y_train,Y_test = train_test_split(X,y,test_size=.2)

from sklearn.linear_model import LinearRegression
regressor = LinearRegression()
regressor.fit(X_train, Y_train)
y_pred = regressor.predict(X_test)

print(y_pred)
print(Y_test)