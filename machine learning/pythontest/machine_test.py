import pandas as pd
# dataset = pd.read_csv("Social_Network_Ads.csv")
# X = dataset.iloc[:,[2,3]]
# y = dataset.iloc[:,-1]

# X = [["hi", "what is your name? this's loved ",]]
# love
# y=[["hello","vineet"]]
# import nltk
# import string
#
# # data cleaning process
# X = []
# for i in X:
#     for j in i:
#         j.split
#
#
#
# from sklearn.model_selection import train_test_split
# X_train,x_text, y_train, y_test = train_test_split(X,y, test_size=0.2)
#
#

# decorator 9990710689

# https://www.thecodeship.com/patterns/guide-to-python-function-decorators/


from functools import wraps
import time

def wrapper(func):
    @wraps(func)
    def time_it(*args,**kwargs):
        start=time.time()
        result = func(*args,**kwargs)
        end = time.time()
        print("Taken Time : ",end-start)
        return result
    return time_it
@wrapper
def hello(j):
    for i in range(j):
        pass
print (hello(900000000))
