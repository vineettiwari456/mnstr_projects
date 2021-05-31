import pandas as pd
import matplotlib.pyplot as plt

dataset = pd.read_csv("Salary_Data.csv")
# print(dataset)
X = dataset.iloc[:,:-1].values
y= dataset.iloc[:,1].values


from sklearn.model_selection import train_test_split
X_train,X_test,Y_train,Y_test = train_test_split(X,y,test_size=.2)
from sklearn.linear_model import LinearRegression
regressor = LinearRegression()
regressor.fit(X_train, Y_train)
y_pred = regressor.predict(X_test)
print(y_pred)
print(Y_test)

plt.scatter(X_train,Y_train, color='red')
plt.plot(X_train,regressor.predict(X_train),color='green')
plt.title('Salary vs Experience (Training set)')
plt.xlabel('Years of Experience')
plt.ylabel('Salary')
plt.show()

plt.scatter(X_test,Y_test, color='red')
plt.plot(X_train,regressor.predict(X_train),color='green')
plt.title('Salary vs Experience (Test set)')
plt.xlabel('Years of Experience')
plt.ylabel('Salary')
plt.show()





