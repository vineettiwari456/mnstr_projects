import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import Imputer
import matplotlib.pyplot as plt
training_set = pd.read_csv("train.csv")
# dataset = dataset.fillna(dataset.mean())
training_set = training_set.dropna()
X_train = training_set.as_matrix(['x'])
y_train = training_set.as_matrix(['y'])
# imputer = Imputer(missing_values="NaN",strategy='mean',axis=0)
# imputer = imputer.fit(y)
# dataset=imputer.fit_transform(dataset)
# print(dataset)
# X=dataset.iloc[:,:-1]
# y=dataset.iloc[:,-1]
# X=dataset[:,0]
# y=dataset[:,-1]

# X_train,X_test,y_train,y_test=train_test_split(X,y,test_size=0.1,random_state=0)
regressor= LinearRegression()
regressor.fit(X_train,y_train)
# y_pred = regressor.predict(X_test)
# print(y_pred)
# Visualising the Training set results
plt.scatter(X_train, y_train, color = 'red')
plt.plot(X_train, regressor.predict(X_train), color = 'blue')
plt.title('X vs Y (Training set)')
plt.xlabel('Years of X')
plt.ylabel('Y')
plt.show()
print('R sq: ',regressor.score(X_train,y_train))
import math
# and so the correlation is..
print('Correlation: ', math.sqrt(regressor.score(X_train,y_train)))