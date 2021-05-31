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
            query = "select profile_id,count(*) as count from user_active_resume_text_%s where status=1 group by profile_id having count(*)>1;"%(ind)
            print(query)
            records = self.execute_dolphin_query(query)
            print(len(records))
            if len(records)>0:
                profile_ids = ','.join([str(m.get("profile_id")) for m in records])
                profile_query = "select id, profile_id,updated from user_active_resume_text_%s where status=1 and profile_id in (%s);"%(ind,profile_ids)
                # print(profile_query)
                profile_records = self.execute_dolphin_query(profile_query)
                print(len(profile_records))
                dicts = {}
                for md in profile_records:
                    try:
                        dicts[md.get("profile_id")].append(md.get("id"))
                    except:
                        dicts[md.get("profile_id")]=[md.get("id")]
                print("dictss : ",len(dicts))
                Total_records = []
                for val in dicts:
                    if len(dicts.get(val))>=2:
                        temp = []
                        maxval = max(dicts.get(val))
                        # print(val,maxval)
                        temp.append(ind)
                        temp.append(maxval)
                        temp.append(val)
                        Total_records.append(tuple(temp))
                print(Total_records)
                update_query = "update user_active_resume_text_%s set status=0 where id=%s and profile_id=%s;"
                print(update_query)
                conn,curr = self.dove_insert_connection()
                curr.executemany(update_query,Total_records)
                conn.commit()
                conn.close()

            # break

if __name__=="__main__":
    obj = UpdateDolphin()
    obj.run()
