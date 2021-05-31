#-*-encoding:utf-8-*-
import pandas as pd
import numpy as np
# print(np.random.randn(6, 4))
daterange= pd.date_range("20201101",periods=6)
# df = pd.DataFrame(np.random.randn(6,4),index=daterange,columns=list("ABCD"))
# print(df.T)
# print(df)
# print(df.apply(np.cumsum))
# df2=df.copy()
# df2['E'] = ['one', 'one', 'two', 'three', 'four', 'three']
# print(df2[df2["E"].isin(['three'])])
# # print(df.loc['20201101':'20201102','A':'B'])
# print(df.iloc[:,3])
# print(np.cumsum)
# s = pd.Series([1,2,3])
# print(s)
# imputpath="C:\\Users\\vktiwari\Downloads\\Scrape Logins - Agg. list (1).xlsx"
# imputpath="newdata.xlsx"
# dfs = pd.read_excel(imputpath,sheet_name='Master list')
# # dfs.to_excel("newdata.xlsx",index=False,sheet_name='Master list')
# print(dfs)

# df = pd.DataFrame({'A': ['one', 'one', 'two', 'three'] * 3,
#                        'B': ['A', 'B', 'C'] * 4,
#                        'C': ['foo', 'foo', 'foo', 'bar', 'bar', 'bar'] * 2,
#                        'D': np.random.randn(12),
#                        'E': np.random.randn(12)})
# # print(df.A)
# for d,g in df.items():
#     print("=====>",d)
#     print(g)
# print(pd.pivot_table(df,values='D',index=['A','B'],columns=['C']))

# rng = pd.date_range('1/1/2012', periods=100, freq='H')
# print(rng)
# print(np.ones((5,2,3),dtype='int16'))
# print(np.empty((2,3),dtype='int16'))
# print(np.arange(10,20))
# s = pd.Series([1, 3, 5, np.nan, 6, 8],index=list("abcdef"),dtype='str')
#
# print(s)
# path="C:\\Users\\vktiwari\\Downloads\\JobCategory.csv"
# data_iterator = pd.read_csv(path, chunksize=10)
#
# chunk_list = []
#
# # Each chunk is in dataframe format
# for data_chunk in data_iterator:
#     # filtered_chunk = chunk_filtering(data_chunk)
#     chunk_list.append(data_chunk)
#     print(len(chunk_list))
# filtered_data = pd.concat(chunk_list)
# print(filtered_data)
# s = pd.Series(['A', 'B', 'C', 'Aaba', 'Baca', np.nan, 'CABA', 'dog', 'cat'])
# print(s.str.upper())
# df = pd.DataFrame({'A': ['foo', 'bar', 'foo', 'bar',
#                             'foo', 'bar', 'foo', 'foo'],
#                        'B': ['one', 'one', 'two', 'three',
#                            'two', 'two', 'one', 'three'],
#                        'C': np.random.randn(8),
#                        'D': np.random.randn(8)})
# print(type(df.groupby("A").sum()))

df = pd.DataFrame({'AAA': [1, 1, 1, 2, 2, 2, 3, 3],
                     'BBB': [2, 1, 3, 4, 5, 1, 2, 3]})
print(df.groupby("AAA")['BBB'].idxmax())