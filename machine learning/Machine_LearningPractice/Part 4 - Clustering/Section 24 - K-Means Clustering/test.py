import nltk
# import pandas as pd
# import matplotlib.pyplot as plt
# dataset = pd.read_csv("Mall_Customers.csv")
# # print(dataset)
# X = dataset.iloc[:,[3,4]].values
# # print(X)
# from sklearn.cluster import KMeans
# # wcss = []
# # for i in range(1,11):
# #     kmeans =  KMeans(n_clusters=i,init="k-means++",random_state=42)
# #     kmeans.fit(X)
# #     wcss.append(kmeans.inertia_)
# # print(wcss)
# # plt.plot(range(1,11),wcss)
# # plt.title("Elbow method")
# # plt.xlabel("Number of cluster")
# # plt.ylabel("SCSS")
# # plt.show()
#
# kmeans =  KMeans(n_clusters=5,init="k-means++",random_state=42)
# y_kmeans = kmeans.fit_predict(X)
# print(y_kmeans)
# print(X[y_kmeans == 0, 0],X[y_kmeans == 0, 1])
# plt.scatter(X[y_kmeans == 0, 0], X[y_kmeans == 0, 1], s = 100, c = 'red', label = 'Cluster 1')
# plt.scatter(X[y_kmeans == 1, 0], X[y_kmeans == 1, 1], s = 100, c = 'green', label = 'Cluster 2')
# plt.scatter(X[y_kmeans == 2, 0], X[y_kmeans == 2, 1], s = 100, c = 'orange', label = 'Cluster 3')
# plt.scatter(X[y_kmeans == 3, 0], X[y_kmeans == 3, 1], s = 100, c = 'pink', label = 'Cluster 4')
# plt.scatter(X[y_kmeans == 4, 0], X[y_kmeans == 4, 1], s = 100, c = 'black', label = 'Cluster 5')
# plt.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s = 300, c = 'yellow', label = 'Centroids')
# plt.title('Clusters of customers')
# plt.xlabel('Annual Income (k$)')
# plt.ylabel('Spending Score (1-100)')
# plt.legend()
# plt.show()
# import datetime
# list=[{u'updated': datetime.datetime(2019, 8, 27, 18, 43, 38), u'created': datetime.datetime(2019, 8, 27, 0, 0), u'subchannel_id': 1, u'enabled': u'0', u'expiry': datetime.datetime(2020, 9, 12, 0, 0), u'channel_id': 1, u'webadmin_id': 219, u'corp_id': 395840, u'type': u'CP', u'id': 6394, u'extra_info': u'URL:http://company.monsterindia.com/fujitsucin/'}, {u'updated': datetime.datetime(2019, 8, 27, 18, 45, 8), u'created': datetime.datetime(2019, 8, 27, 0, 0), u'subchannel_id': 1, u'enabled': u'1', u'expiry': datetime.datetime(2020, 9, 12, 0, 0), u'channel_id': 1, u'webadmin_id': 219, u'corp_id': 395840, u'type': u'CP', u'id': 6395, u'extra_info': u'URL:http://company.monsterindia.com/fujitsucin/'}]
# new_enabled = []
# new_disabled = []
# for jk in list:
#     if jk.get("enabled","")=='1':
#         new_enabled.append(jk)
#     else:
#         new_disabled.append(jk)
# print(new_enabled)
# print(new_disabled)
# d=([1,2],)
# d[0][1]=5
# print(d)

# d = (1,2,3,[4,5],"6,7,8")
strg = "vineet"
strg[0]="p"
print(strg)











