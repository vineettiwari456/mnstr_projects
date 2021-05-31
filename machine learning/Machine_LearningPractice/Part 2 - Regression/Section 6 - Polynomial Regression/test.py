import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
dataset = pd.read_csv("Position_Salaries.csv")
# print(dataset)

X = dataset.iloc[:,1:2].values
y = dataset.iloc[:,2].values

# print(X)
from sklearn.linear_model import LinearRegression
lin_reg = LinearRegression()
lin_reg.fit(X,y)
#
# # Visualising the Linear Regression results
# plt.scatter(X, y, color = 'red')
# plt.plot(X, lin_reg.predict(X), color = 'blue')
# plt.title('Truth or Bluff (Linear Regression)')
# plt.xlabel('Position level')
# plt.ylabel('Salary')
# plt.show()

from sklearn.preprocessing import PolynomialFeatures
poly_reg = PolynomialFeatures(degree=5)
X_poly = poly_reg.fit_transform(X)
poly_reg.fit(X_poly,y)
lin_reg2= LinearRegression()

lin_reg2.fit(X_poly,y)
plt.scatter(X,y,color="red")
plt.plot(X,lin_reg2.predict(X_poly))
plt.title("Polynomial Regression")
plt.xlabel("lvel")
plt.xlabel("salary")
plt.show()













# print(X_poly)
# poly_reg.fit(X_poly,y)
#
# lin_reg_2 = LinearRegression()
# lin_reg_2.fit(X_poly, y)
#
# plt.scatter(X, y, color = 'red')
# plt.plot(X, lin_reg_2.predict(X_poly), color = 'blue')
# plt.title('Truth or Bluff (Polynomial Regression)')
# plt.xlabel('Position level')
# plt.ylabel('Salary')
# plt.show()
