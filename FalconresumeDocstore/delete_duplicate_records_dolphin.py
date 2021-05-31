import os
import mysql
import mysql.connector
import concurrent.futures
import requests, zlib
import psutil
from datetime import datetime

class UpdateDolphin:
    def __init__(self):
        self.conf_dove = {"user": "vtiwari", "password": "GhR42wMRQ}", "host": "10.216.247.119", "database": "dolphin"}



    def dove_insert_connection(self):
        connection = mysql.connector.connect(user=self.conf_dove['user'],
                                             password=self.conf_dove['password'],
                                             host=self.conf_dove['host'],
                                             database=self.conf_dove['database'])
        cursor = connection.cursor(dictionary=True)
        return connection, cursor

    def execute_dolphin_query(self, query):
        connection, cursor = self.dove_insert_connection()
        cursor.execute(query)
        all_data = cursor.fetchall()
        connection.close()
        return all_data

    def run(self):
        for ind in range(0,1):
            query = "select profile_id,count(*) as count from user_active_resume_text_%s group by profile_id having count(*)>1;"%(ind)
            print(query)
            records = self.execute_dolphin_query(query)
            print(len(records))
            if len(records)>0:
                profile_ids = ','.join([str(m.get("profile_id")) for m in records])
                profile_query = "select id,status, profile_id,updated from user_active_resume_text_%s where profile_id in (%s);"%(ind,profile_ids)
                # print(profile_query)
                profile_records = self.execute_dolphin_query(profile_query)
                print(len(profile_records))
                deletion_ids=[]
                for md in profile_records:
                    # print(md)
                    if md.get("status")==0:
                        deletion_ids.append(str(md.get("id")))
                deletion_id = ','.join(deletion_ids)
                update_query = "delete from user_active_resume_text_%s where id in (%s);"%(ind,deletion_id)
                print(update_query)
                conn,curr = self.dove_insert_connection()
                curr.execute(update_query)
                conn.commit()
                conn.close()
                # break

            # break

if __name__=="__main__":
    obj = UpdateDolphin()
    obj.run()
