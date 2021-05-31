import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from  sklearn.linear_model import LinearRegression
dataset = pd.read_csv("Salary_Data.csv")
# print(dataset)
X = dataset.iloc[:,:-1]
y = dataset.iloc[:,1]
# print (X, y)
X_train,X_test, Y_train,Y_test = train_test_split(X,y,test_size=0.2)
# print(X_train,Y_train)
regressor = LinearRegression()
regressor.fit(X,y)
y_pred = regressor.predict(X_test)
print(Y_test,y_pred)

# plt.scatter(X_train,Y_train, color="red")
# plt.plot(X_train,regressor.predict(X_train),color="green")
# plt.title("Experince vs Salary (train test)")
# plt.xlabel('Years of Experience')
# plt.ylabel('Salary')
# plt.show()

plt.scatter(X_test,Y_test, color="red")
plt.plot(X_train,regressor.predict(X_train),color="green")
plt.title("Experince vs Salary (train test)")
plt.xlabel('Years of Experience')
plt.ylabel('Salary')
plt.show()