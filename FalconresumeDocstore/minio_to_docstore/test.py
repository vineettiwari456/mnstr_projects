import requests,json

url = "http://c.docstore.monsterindia.com/myscripts/serverhttp.html"

# payload = "------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"upfile\"; filename=\"36190100.doc\"\r\nContent-Type: application/msword\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"uid\"\r\n\r\n40610484\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"resid\"\r\n\r\n36190100\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"create_flag\"\r\n\r\n1\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"
payload = {"Content-Type": "application/msword","uid":40610484,"resid":36190100,"create_flag":1}


headers = {
    'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0",
    'content-type': "multipart/form-data",
    'cache-control': "no-cache",
    # 'postman-token': "dff63f62-069c-511d-33a8-e208420c1e29"
    }

# response = requests.request("POST", url, data=payload, headers=headers)
# data=json.loads(json.dumps(payload))
# print(type(data))
# response = requests.post( url, data=data, headers=headers)
#
# print(response.text)
# filepath = "E:/PycharmProjects/FalconresumeDocstore/venv/src/minio_to_docstore/tmp/36190100.doc"
# # filepath = "C:\\Users\\vktiwari\\Documents\\backupC\personal\\vineettiwari_newdocx.docx"
# files = {'upfile': open(filepath, 'rb')}
# res = requests.post(url, data=data,files = files, headers = headers)
# print(res.text)

# import pymongo
# # from urllib.parse import quote
# db_name = "falconconsumerapp"
# mongo_username = "vineet"
# mongo_pwd = "vineettiwari"
# mongoip = "10.216.248.101"
# mongoport = "31802"
# import urllib
# # from urllib.parse import quote
# mongo_uri = "mongodb://" + mongo_username + ":" + mongo_pwd + "@" + mongoip+":"+str(mongoport) + "/?authSource=" + db_name
# # mongo_uri = "mongodb://" + mongo_username + ":" + quote(mongo_pwd) + "@" + mongoip+":"+str(mongoport) + "/"
# # mongo_uri ="mongodb://vineet:vineettiwari@10.216%2C248.101:31802/?authSource=falconconsumerapp"
# # client = pymongo.MongoClient(mongoip, mongoport)
# print(mongo_uri)
# client = pymongo.MongoClient(mongo_uri)
# db = client.falconconsumerapp
# collection = db.resume_profile_id
#
# # collection = client.resume_profile_id
# # client = pymongo.MongoClient(mongoip, int(mongoport))
# # client.admin.authenticate(mongo_username, mongo_pwd, mechanism = 'SCRAM-SHA-1', source=db_name)
# # db_name = client[db_name]
# # # col_name = col_name
# # collection = db_name["resume_profile_id"]
# profileIds=[]
# # for coll in collection.find({}):
# #     profileIds.append(str(int(coll.get("profileId"))))
# # unique_profiles = list(set(profileIds))
# f=open("insert_log",'r',newline='')
# data = f.readlines()
# f.close()
# # matched_ids=[]
# # unmatched_ids=[]
# # for da in data:
# #     match_id = da.split("data::")[0].strip()
# #     matched_ids.append(match_id)
# # for ma in unique_profiles:
# #     if ma not in matched_ids:
# #         unmatched_ids.append(ma)
# # print(len(unmatched_ids),unmatched_ids)
# # f=open("unmatch_profile_ids",'w')
# # f.write(str(unmatched_ids))
# # f.close()
# #
# # import pymongo
# # conn = pymongo.MongoClient(mongo_uri)
# # db = conn['falconconsumerapp']
# # coll = db['resume_profile_id']
# # for col in coll.find({}):
# #     print(col)
# matched_ids=[]
# import ast
# for da in data:
#     match_id = ast.literal_eval(da.split("data::")[1].strip())
#     print(match_id[-1])
#     if match_id[-1]:
#         matched_ids.append(str(match_id[-1]))
# fd=open("resumeids",'w')
# fd.write(str(matched_ids))
# fd.close()
# print(len(matched_ids))


import requests
url_link = "http://c.docstore.monsterindia.com/971/M11/87618970/103042958/103042958.txt"
response = requests.get(url_link, timeout=60)
print(response)