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
class UpdateResumeView:
    def __init__(self):
        self.conf_db2 = {"user": config("DB2_USER"), "password": config("DB2_PASS"), "host": config("DB2_HOST"),
                         "database": config("DB2_DATABASE")}
        self.conf_db3 = {"user": config("DB3_USER"), "password": config("DB3_PASS"), "host": config("DB3_HOST"),
                         "database": config("DB3_DATABASE")}
    def db2_connection(self):
        connection_db2 = mysql.connector.connect(user=self.conf_db2['user'],
                                                 password=self.conf_db2['password'],
                                                 host=self.conf_db2['host'],
                                                 database=self.conf_db2['database'])
        cursor_db2 = connection_db2.cursor(dictionary=True)
        return connection_db2, cursor_db2

    def db3_connection(self):
        connection_db2 = mysql.connector.connect(user=self.conf_db3['user'],
                                                 password=self.conf_db3['password'],
                                                 host=self.conf_db3['host'],
                                                 database=self.conf_db3['database'])
        cursor_db2 = connection_db2.cursor(dictionary=True)
        return connection_db2, cursor_db2

    # def check_deleted(self,is_enable):
    #     enable = '0'
    #     if is_enable:
    #         if '0' in str(is_enable):
    #             enable = "1"
    #     return enable
    def insert_db3_db(self, records):
        insert_query = """insert into resumeviewed (`resid`,`subuid`,`viewdate`,`tobedeleted`,`channel_id`) values(%s,%s,%s,%s,%s);"""
        conn, curr = self.db3_connection()
        curr.executemany(insert_query, records)
        conn.commit()
        conn.close()

    def chunks(self,lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
    
    def get_resume_id_with_channel(self):
        from datetime import datetime
        ids_list = self.get_resume_ids()
        ids = ','.join(ids_list)
        print(ids)
        query = "select id as resume_id,channel_id from resumes where id in (%s)"%(ids)
        print(query)
        conn,curr = self.db2_connection()
        curr.execute(query)
        records = curr.fetchall()
        conn.close()
        print(len(records))
        total_records =[]
        for mk in records:
            temp=[]
            temp.append(mk.get("resume_id"))
            temp.append("113486")
            temp.append(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            temp.append("0")
            temp.append(mk.get("channel_id"))
            total_records.append(tuple(temp))
        print(len(total_records))
        print(total_records[:10])
        chunk_data = self.chunks(total_records, 1000)
        for record in chunk_data:
            print(len(record))
            self.insert_db3_db(record)

    def get_resume_ids(self):
        f = open("resumeids", 'r')
        unique_profiles = ast.literal_eval(f.read())
        f.close()
        return unique_profiles
        
if __name__=="__main__":
    obj = UpdateResumeView()
    obj.get_resume_id_with_channel()
         