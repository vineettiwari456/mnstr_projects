import os, json
import mysql.connector
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)
import requests
import pymongo
import concurrent
import concurrent.futures
from decouple import config
import ast


class MinioToDocstore:
    def __init__(self):
        self.headers = {
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0",
            'content-type': "multipart/form-data",
            'cache-control': "no-cache",
        }
        self.conf_falcon = {"user": config("FALCON_USER"), "password": config("FALCON_PASS"),"host":config("FALCON_HOST"),
                            "database": config("FALCON_DATABASE")}
        self.conf_db2 = {"user": config("DB2_USER"), "password": config("DB2_PASS"), "host": config("DB2_HOST"),
                         "database": config("DB2_DATABASE")}
        self.bucket_name = config("BUCKET_NAME")
        self.minioClient = Minio(config("MINIO_URL"),
                                 access_key=config("ACCESS_KEY"),
                                 secret_key=config("SECRET_KEY"),
                                 secure=False)
        buckets = self.minioClient.list_buckets()
        if self.bucket_name.lower() not in [bucket.name for bucket in buckets]:
            self.minioClient.make_bucket(self.bucket_name)
        self.dirs = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp")
        if not os.path.exists(self.dirs):
            os.mkdir(self.dirs)
        self.HTTP_CONSTANT = "http://"
        self.A_DOCSTORE = "a.docstore.monsterindia.com/myscripts/serverhttp.html"
        self.C_DOCSTORE = "c.docstore.monsterindia.com/myscripts/serverhttp.html"
        self.A_Domain = "a.docstore.monsterindia.com"
        self.C_Domain = "c.docstore.monsterindia.com"
        self.A_server = "a.docstore"
        self.C_server = "c.docstore"
        self.mongo_uri = "mongodb://" + str(config("mongo_username")) + ":" + str(config("mongo_pwd")) + "@" + config("mongoip") + ":" + str(
            config("mongoport")) + "/" + str(config("db_name"))
        main_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Log")
        if not os.path.exists(main_directory):
            os.mkdir(main_directory)
        self.logfilename_insert = main_directory + "/insert_log"
        self.logfilename_error = main_directory + "/error_log"
        self.logfilename_query = main_directory + "/query_error_log"

    def write_log(self, filepath,logtext):
        logopenfile = open(filepath, 'a')
        logopenfile.write(str(logtext))
        logopenfile.write("\n")
        logopenfile.close()

    def falcon_connection(self):
        connection_falcon = mysql.connector.connect(user=self.conf_falcon['user'],
                                                    password=self.conf_falcon['password'],
                                                    host=self.conf_falcon['host'],
                                                    database=self.conf_falcon['database'])
        cursor_falcon = connection_falcon.cursor(dictionary=True)
        return connection_falcon, cursor_falcon

    def db2_connection(self):
        connection_db2 = mysql.connector.connect(user=self.conf_db2['user'],
                                                 password=self.conf_db2['password'],
                                                 host=self.conf_db2['host'],
                                                 database=self.conf_db2['database'])
        cursor_db2 = connection_db2.cursor(dictionary=True)
        return connection_db2, cursor_db2

    def fetch_falcon_records(self, query):
        connection, cursor = self.falcon_connection()
        cursor.execute(query)
        all_data = cursor.fetchall()
        connection.close()
        return all_data

    def fetch_db2_records(self, query):
        connection, cursor = self.db2_connection()
        cursor.execute(query)
        all_data = cursor.fetchall()
        connection.close()
        return all_data

    def downlad_resume_from_minio(self, sourcepath, destinationpath):
        is_success = False
        try:
            destination_path = os.path.join(self.dirs, destinationpath)
            self.minioClient.fget_object(self.bucket_name, sourcepath, destination_path)
            is_success = True
        except Exception as minio_e:
            print(minio_e)
            log_text = "Error in downlad_resume_from_minio :: " + str(minio_e) + " " + str(sourcepath)
            self.write_log(self.logfilename_error, log_text)
            pass
        return is_success

    def get_kiwi_user_id(self, resumeid):
        query = "select uid from resumes where id=%s" % (resumeid)
        print(query)
        records = self.fetch_db2_records(query)
        print(records)
        uid = ""
        if len(records) > 0:
            uid = records[0].get("uid")
        return resumeid, uid

    # def update_resume_url_db2(self, filename, fileext, server, resume_id):
    #     conn, cursor = self.db2_connection()
    #     # ssourcesql = 'UPDATE image_resume_url SET resume_url=%s where resume_id=%s'
    #     ssourcesql = 'UPDATE resumes SET updated =now(), server=%s,type=%s,filename=%s where id=%s'
    #     ssourceval = tuple([server, fileext, filename, resume_id])
    #     cursor.execute(ssourcesql, ssourceval)
    #     conn.commit()
    #     conn.close()
    def update_resume_url_db2(self, total_data):
        conn, cursor = self.db2_connection()
        # ssourcesql = 'UPDATE image_resume_url SET resume_url=%s where resume_id=%s'
        ssourcesql = 'UPDATE resumes SET updated =now(), server=%s,type=%s,filename=%s where id=%s'
        ssourceval = total_data
        cursor.executemany(ssourcesql, ssourceval)
        conn.commit()
        conn.close()

    def upload_to_docstore(self, uid, resumeid, filepath, fileext, file_name,profileid):
        data_value=[]
        payload = {"Content-Type": "multipart/form-data", "uid": uid, "resid": resumeid, "create_flag": 1}
        payload = json.loads(json.dumps(payload))
        db_url = ""
        if (uid % 2 == 1):
            server = self.A_server
            db_url = self.HTTP_CONSTANT + self.A_Domain
            mainurl = self.HTTP_CONSTANT + self.A_DOCSTORE
        else:
            server = self.C_server
            db_url = self.HTTP_CONSTANT + self.C_Domain
            mainurl = self.HTTP_CONSTANT + self.C_DOCSTORE
        # print(mainurl)
        try:
            files = {'upfile': open(filepath, 'rb')}
            resp = requests.post(mainurl, data=payload, files=files, headers=self.headers,timeout=180)
            print(resp.text, resp.headers['content-type'])
            if resp.status_code == 200:
                dirno = str((int(uid) % 1000) + 1)
                uidMode = str((int(uid) % 99) + 1)
                secLevelDir = "M" + uidMode
                filename = str(resumeid) + fileext
                docPath = db_url + "/" + str(dirno) + "/" + str(secLevelDir) + "/" + str(uid) + "/" + str(
                    resumeid) + "/" + filename
                print(docPath)
                ext = fileext.strip()[1:].upper()
                # print(file_name, ext.upper(), server, resumeid)
                # self.update_resume_url_db2(file_name, ext, server, resumeid)
                data_value = [server, ext, file_name, resumeid]
                self.write_log(self.logfilename_insert, str(profileid)+" data:: "+str(data_value))
        except Exception as ex:
            log_text = "Error in upload resume in docstore :: "+str(ex)+" "+str(profileid)
            self.write_log(self.logfilename_error,log_text)
            pass
        return data_value
    def all_response_value(self,jd):
        data=[]
        try:
            resumefilename = str(str(jd[0]) + str(jd[1]))
            is_success = self.downlad_resume_from_minio(jd[3], resumefilename)
            if is_success:
                filepath = os.path.join(self.dirs, resumefilename)
                data = self.upload_to_docstore(jd[-2], jd[0], filepath, str(jd[1]), resumefilename, jd[-1])
        except Exception as ed:
            lgval = str(ed)+"  "+ str(jd)
            self.write_log(self.logfilename_error,lgval)
        return data

    def download(self):
        ids_list = self.get_falcon_profile_ids()
        ids = ','.join(ids_list)
        # sql = """select up.id as upid,usr.kiwi_user_id as kiwi_user_id,up.kiwi_profile_id as kiwi_profile_id,up.resume_bucket as resume_bucket,up.id as userprofileid,up.resume_file_path as resumepath,up.resume_filename as filename,up.user_id as userid,usr.uuid as uuid from user_profiles as up inner join users as usr on up.user_id = usr.id where up.id in('89898862') order by up.id asc limit 10;"""
        sql = """select up.id as upid,usr.kiwi_user_id as kiwi_user_id,up.kiwi_profile_id as kiwi_profile_id,up.resume_bucket as resume_bucket,up.id as userprofileid,up.resume_file_path as resumepath,up.resume_filename as filename,up.user_id as userid,usr.uuid as uuid from user_profiles as up inner join users as usr on up.user_id = usr.id where up.id in(%s) order by up.id asc;"""%(ids)
        # sql = """select up.id as upid,usr.kiwi_user_id as kiwi_user_id,up.kiwi_profile_id as kiwi_profile_id,up.resume_bucket as resume_bucket,up.id as userprofileid,up.resume_file_path as resumepath,up.resume_filename as filename,up.user_id as userid,usr.uuid as uuid from user_profiles as up inner join users as usr on up.user_id = usr.id where up.id in(90411334) order by up.id asc;"""
        print(sql)
        results = self.fetch_falcon_records(sql)
        print(len(results))
        All_urls = []
        for j_temp in results:
            temp = []
            filename, file_extension = os.path.splitext(j_temp.get("filename", None))
            # print(j_temp.get("resume_bucket"))
            temp.append(j_temp.get("kiwi_profile_id", None))
            temp.append(file_extension)
            temp.append(j_temp.get("resume_bucket", None))
            temp.append(j_temp.get("resumepath", None))
            # temp.append("/resume/111c5cd1-0d8a-11e9-beca-70106fbef802/36394194/90456050/19781779_1591696539224.doc")
            temp.append(j_temp.get("uuid", None))
            temp.append(j_temp.get("userid", None))
            temp.append(j_temp.get("userprofileid", None))
            temp.append(j_temp.get("filename", None))
            temp.append(j_temp.get("kiwi_user_id", None))
            temp.append(j_temp.get("upid"))
            print(j_temp.get("upid"))
            All_urls.append(temp)
        print("length of urls : ", len(All_urls))
        # for jd in All_urls:
        #     print(jd)
        #     resumefilename = str(str(jd[0]) + str(jd[1]))
        #     is_success = self.downlad_resume_from_minio(jd[3], resumefilename)
        #     if is_success:
        #         filepath = os.path.join(self.dirs, resumefilename)
        #         self.upload_to_docstore(jd[-2], jd[0], filepath, str(jd[1]), resumefilename, jd[-1])
            # self.upload_to_docstore(userid, resumeid, filepath, str(jd[1]), resumefilename)
        Total_data = []
        # All_urls = [[u'https://www.kellyservices.com.sg/i-t-and-t-jobs/java-developer/2732036', 'Singapore', 'Contract', '$5000 - $6000 Annual Salary']]
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            future_to_url = {executor.submit(self.all_response_value, url_jb): url_jb
                             for url_jb in
                             All_urls[:]}
            for future in concurrent.futures.as_completed(future_to_url):
                url2 = future_to_url[future]
                try:
                    data = future.result()
                    if len(data)>0:
                        Total_data.append(tuple(data))
                except Exception as exc:
                    errorlog = str(exc)
                    self.write_log(self.logfilename_error,errorlog)
                    pass
        print ("length of data :",len(Total_data))
        if len(Total_data)>0:
            chunk_data = self.chunks(Total_data,1000)
            for record in chunk_data:
                try:
                    self.update_resume_url_db2(record)
                    print("updated records",len(record))
                except Exception as ed:
                    textlog = "Error in insert:: "+str(ed)+str(record)
                    self.write_log(self.logfilename_query,textlog)
                    pass

    def chunks(self,lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
    def get_falcon_profile_ids(self):
        f=open("profile_ids",'r')
        f = open("unmatch_profile_ids", 'r')
        unique_profiles = ast.literal_eval(f.read())
        f.close()
        # profileIds=[]
        # print(self.mongo_uri)
        # client = pymongo.MongoClient(self.mongo_uri)
        # db = client.falconconsumerapp
        # collection = db.resume_profile_id
        # for coll in collection.find({}):
        #     profileIds.append(str(int(coll.get("profileId"))))
        # unique_profiles = list(set(profileIds))
        # print(len(unique_profiles))
        print(len(unique_profiles))
        return unique_profiles


if __name__ == "__main__":
    obj = MinioToDocstore()
    obj.download()
    # obj.get_falcon_profile_ids()
