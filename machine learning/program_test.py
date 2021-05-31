# from location_mapping import Location_Mapping
import pymongo
import urllib
import copy

# mapperObj = Location_Mapping()
#
#
# mongo_uri = "mongodb://db:" + urllib.quote("m@ng@d954") + "@10.216.240.154/bazooka"
# client = pymongo.MongoClient(mongo_uri)
# db = client.bazooka
# coll = db.crawled_jobs_unmapped
# crawled_jobs = []
#
# for d in coll.find({"source_url":"https://www.naukri.com/ciel-hr-services-jobs-careers-2089678"}):
#     crawled_jobs.append(d)
#     print(len(crawled_jobs))
# mapped_jobs = []
# print("mapping now")
# for cj in crawled_jobs:
#     mj = mapperObj.map_job(copy.deepcopy(cj))
#     mapped_jobs.append(copy.deepcopy(mj))
#     print(len(mapped_jobs))
# print("getting unique")
# def get_unique_array(mapped_job_array):
#     try:
#         unique_mapped_job_array = []
#         for mj in mapped_job_array:
#             is_duplicate = False
#             for k in range(len(unique_mapped_job_array)):
#                 if mj["_id"] == unique_mapped_job_array[k]["_id"]:
#                     source_urls = [mj["source_url"]]
#                     job_urls = [mj["job_url"]]
#                     source_urls.extend(unique_mapped_job_array[k]["source_url"].split(","))
#                     job_urls.extend(unique_mapped_job_array[k]["job_url"].split(","))
#                     unique_mapped_job_array[k]["source_url"] = ",".join(set(source_urls))
#                     unique_mapped_job_array[k]["job_url"] = ",".join(set(job_urls))
#                     is_duplicate = True
#                     break
#             print(len(unique_mapped_job_array))
#             if not is_duplicate:
#                 unique_mapped_job_array.append(mj)
#         return unique_mapped_job_array
#     except Exception as e:
#         raise Exception("unique mapped jobs error : ", str(e))
# get_unique_array(mapped_jobs)

#  Anagrma program
#
def anagramSolution1(s1,s2):
    alist = list(s2)
    # alist = s2
    pos1=0
    stillOK = True
    while pos1<len(s1) and stillOK:
        pos2=0
        found = False
        while pos2 < len(alist) and not found:
            if s1[pos1] ==alist[pos2]:
                found = True
            else:
                pos2+=1
        if found:
            alist[pos2] = None
        else:
            stillOK = False
        pos1 +=1
    return stillOK


print anagramSolution1("vineet", "tineev")
# exit(0)
from pythonds.basic.stack import Stack

# def checkbalance(symbolString):
#     s = Stack()
#     index=0
#     balance = True
#     while index<len(symbolString) and balance:
#         symbol = symbolString[index]
#         if symbol in "({[":
#             s.push(symbol)
#         else:
#             if s.isEmpty():
#                 balance = False
#             else:
#                 s.pop()
#         index +=1
#     if balance and s.isEmpty():
#         return  True
#     else:
#         return False
#
# print (checkbalance('{{([][])}()}'))



# def distance_from_zero(arg):
#     print type(arg)
#     if ((str(type(arg)) == "<type 'int'>") or (str(type(arg)) == "<type 'float'>")):
#         return abs(arg)
#     else:
#         return None
#
#
# print distance_from_zero('hjh')


# def sequentialSearch(alist, item):
#     pos = 0
#     found = False
#     while pos<len(alist) and not found:
#         if alist[pos]==item:
#             found = True
#         else:
#             pos +=1
#
#     return found
#
# testlist = [1, 2, 32, 8, 17, 19, 42, 13, 0]
# print sequentialSearch(testlist, 13)

# def binarySearch(alist, item):
# 	    first = 0
# 	    last = len(alist)-1
# 	    found = False
#
# 	    while first<=last and not found:
# 	        midpoint = (first + last)//2
# 	        if alist[midpoint] == item:
# 	            found = True
# 	        else:
# 	            if item < alist[midpoint]:
# 	                last = midpoint-1
# 	            else:
# 	                first = midpoint+1
#
# 	    return found
#
# testlist = [0, 1, 2, 8, 13, 17, 19, 32, 42,]
# print(binarySearch(testlist, 19))
# print(binarySearch(testlist, 13))


# bubble sort
# def bubbleSort(alist):
#     for passnum in range(len(alist)-1,0,-1):
#         for i in range(passnum):
#             if alist[i]>alist[i+1]:
#                 temp = alist[i]
#                 alist[i] = alist[i+1]
#                 alist[i+1] = temp
#
# alist = [54,26,93,17,77,31,44,55,20]
# bubbleSort(alist)
# print(alist)
# def shortBubbleSort(alist):
#     exchanges = True
#     passnum = len(alist)-1
#     while passnum > 0 and exchanges:
#        exchanges = False
#        for i in range(passnum):
#            if alist[i]>alist[i+1]:
#                exchanges = True
#                temp = alist[i]
#                alist[i] = alist[i+1]
#                alist[i+1] = temp
#        passnum = passnum-1
#
# alist=[20,30,40,90,50,60,70,80,100,110]
# shortBubbleSort(alist)
# print(alist)

# def selectionSort(alist):
#     for pos in range(len(alist)-1,0,-1):
#         maxpos = 0
#         for i in range(1,pos+1):
#             if alist[i]>alist[maxpos]:
#                 maxpos = i
#         temp = alist[pos]
#         alist[pos] = alist[maxpos]
#         alist[maxpos] = temp
#     return alist
#
# alist = [54,26,93,17,77,31,44,55,20]
# print selectionSort(alist)

# def insertionSort(alist):
#     for index in range(1, len(alist)):
#         currentvalue = alist[index]
#         position = index
#         while position>0 and alist[position-1]>currentvalue:
#             alist[position]= alist[position-1]
#             position -=1
#         alist[position] = currentvalue
#     return alist
#
# alist = [54,26,93,17,77,31,44,55,20]
# print insertionSort(alist)


# [54,26,93,17,77,31,44,55,20]


# def mergeSort(alist):
#     print("Splitting ",alist)
#     if len(alist)>1:
#         mid = len(alist)//2
#         lefthalf = alist[:mid]
#         righthalf = alist[mid:]
#         mergeSort(lefthalf)
#         mergeSort(righthalf)
#         i=0
#         j=0
#         k=0
#         while i < len(lefthalf) and j < len(righthalf):
#             if lefthalf[i] < righthalf[j]:
#                 alist[k]=lefthalf[i]
#                 i=i+1
#             else:
#                 alist[k]=righthalf[j]
#                 j=j+1
#             k=k+1
#
#         while i < len(lefthalf):
#             alist[k]=lefthalf[i]
#             i=i+1
#             k=k+1
#
#         while j < len(righthalf):
#             alist[k]=righthalf[j]
#             j=j+1
#             k=k+1
#     print("Merging ",alist)
#
# alist = [54,26,93,17,77,31,44,55,20]
# mergeSort(alist)
# print(alist)


# data_list = [-5, -23, 5, 0, 23, -6, 23, 67]
# new_list = []
#
# while data_list:
#     minimum = data_list[0]  # arbitrary number in list
#     for x in data_list:
#         if x < minimum:
#             minimum = x
#     new_list.append(minimum)
#     data_list.remove(minimum)
#
# print new_list




# def fibnoci(nterms):
#     n1=0
#     n2=1
#     count = 0
#     if nterms<=0:
#         print("Please enter a positive integer")
#     elif nterms==1:
#         print n1
#     else:
#         while count<nterms:
#             print n1,
#             nth  = n1+n2
#             n1 = n2
#             n2= nth
#             count +=1
#
# print fibnoci(5)

# def rec_fib(n):
#     if n<=1:
#         return n
#     else:
#         return rec_fib(n-1)+rec_fib(n-2)
#
# for i in range(10):
#     print rec_fib(i)

# def checkprimenumber(num):
#     if num>1:
#         for j in range(2,num+1):
#             for i in range(2,j):
#                 if (j%i)==0:
#                     print ('%d%d Number is not prime'%(j,i))
#                     break
#             else:
#                 print ("number is prime %d"%(j))
#     else:
#         print 'number is not prime'
# print checkprimenumber(11)

# def reverse(text):
#     if len(text) <= 1:
#         return text
#     return reverse(text[1:]) + text[0]
#
# # n = str(input("ENTER  STRING\n"))
# n = "vineet"
# if n==reverse(n):
#     print ("It's a palindrome")
# else:
#     print ("It's not a palindrome")

# pattern match program
# for i in range(0,5):
#     for j in range(0,i+1):
#         print "* ",
#         # print j,
#     print
# *
# *  *
# *  *  *
# *  *  *  *
# *  *  *  *  *

# k=1
# for i in range(0,5):
#     for j in range(0,k):
#         print '* ',
#     k +=2
#     print
#
# *
# *  *  *
# *  *  *  *  *
# *  *  *  *  *  *  *
# *  *  *  *  *  *  *  *  *

# for i in range(5,0,-1):
#     for j in range(i,0,-1):
#         print '* ',
#     print
# *  *  *  *  *
# *  *  *  *
# *  *  *
# *  *
# *

# k = 1
# for i in range(5,0,-1):
#     for j in range(0,k):
#         print ' '*(i-1)+'*'*k,
#         break
#     k+=1
#     print ''
#
#    *
#    **
#   ***
#  ****
# *****

# k = 1
# for i in range(5,0,-1):
#     for j in range(0,k):
#         print ' '*(2*i-2)+'*'*k,
#         break
#     k+=2
#     print ''
#         *
#       ***
#     *****
#   *******
# *********
# n=1
# for i in range(0,5):
#     for j in range(0,i+1):
#         print n,
#         n +=1
#     print
# 1
# 2 3
# 4 5 6
# 7 8 9 10
# 11 12 13 14 15


# n=1
# for i in range(0,5):
#     for j in range(0,i+1):
#         print n,
#     n +=1
#     print
# 1
# 2 2
# 3 3 3
# 4 4 4 4
# 5 5 5 5 5


# for i in range(0,5):
#     n=1
#     for j in range(0,i+1):
#         print n,
#         n +=1
#     print
#
# 1
# 1 2
# 1 2 3
# 1 2 3 4
# 1 2 3 4 5
# k=1
# for i in reversed(range(0,5)):
#     for j in range(0,i+1):
#         print ' '*(2*i+1)+"* "*k+' '*(2*i+1)
#         break
#     k +=2
#     print
#
#          *
#
#        * * *
#
#      * * * * *
#
#    * * * * * * *
#
#  * * * * * * * * *
#
# n = 65
# for i in range(0,5):
#     for j in range(0,i+1):
#         print chr(n),
#         n +=1
#     print
# A
# B C
# D E F
# G H I J
# K L M N O

# k=1
# for i in reversed(range(0,5)):
#     for j in range(0,i+1):
#         print ' '*(2*i+1)+"* "*k+' '*(2*i+1)
#         break
#     k +=2
#     print
# k=1
# for i in reversed(range(0,5)):
#     for j in range(0,i+1):
#         print ' '*k+"* "*(2*i+1)+' '*k
#         break
#     k +=1
#     print

import requests
headers1={
"Host": "wshcareer.com",
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
"Accept": "application/json, text/javascript, */*; q=0.01",
"Accept-Language": "en-US,en;q=0.5",
"Accept-Encoding": "gzip, deflate, br",
"Referer": "https://wshcareer.com/",
"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
"X-Requested-With": "XMLHttpRequest",
# "Content-Length": "347",
"Connection": "keep-alive",
"Cookie": "wpjb_transient_id=1554701571-8153",
}

import pymongo
mongo_username ="db"
mongo_pwd ="m@ng@d954"
mongoip = "10.216.240.220"
db_name = "recruiter"
crawled_job_collection = "recruiter_searches"
mongo_uri = "mongodb://" + mongo_username + ":" + urllib.quote(mongo_pwd) + "@" + mongoip + "/" + db_name
# client = pymongo.MongoClient(mongoip, mongoport)
client = pymongo.MongoClient(mongo_uri)
print client
db_crawl = eval("client." + db_name)
collection_crawl = eval("db_crawl." + crawled_job_collection)
# collection_crawl.insert(crawled_jobs_array, check_keys=False)
s_date = "2019-05-02 00:00:00"
e_date = "2019-05-02 23:59:59"
print collection_crawl.find({'logtime': {'$gte': s_date, '$lte': e_date},'channel': "1"}).count()
































