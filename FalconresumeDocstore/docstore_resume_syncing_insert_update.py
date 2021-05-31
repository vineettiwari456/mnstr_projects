import os
import mysql
import mysql.connector
import concurrent.futures
import requests, zlib
import psutil
from datetime import datetime

main_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sync_ids")
resume_insertpid = main_directory + "/resume_insert_pid"
fd = open(resume_insertpid)
pidval = fd.read()
fd.close()
if pidval:
    if int(pidval) in [p.info["pid"] for p in psutil.process_iter(attrs=['pid'])]:
        print('Process is already running------', pidval)
        exit(0)
pidfile = open(resume_insertpid, 'w')
pidfile.write(str(os.getpid()))
pidfile.close()


class ResumeDocstore:
    def __init__(self):
        self.chunk_size = 1000
        self.conf_falcon = {"user": "vtiwari", "password": "vtiwari@123##", "host": "10.216.247.108",
                            "database": "falcon"}
        # self.conf = {"user": "db", "password": "bazookadb", "host": "10.216.204.7", "database": "bazooka"}
        self.conf = {"user": "asteam", "password": "Asteam@123##", "host": "10.216.240.118", "database": "bazooka"}
        # self.conf_dove = {"user": "vtiwari", "password": "GhR42wMRQ}", "host": "10.216.247.119", "database": "dolphin"}
        self.conf_dove = {"user": "db", "password": "bazookadb", "host": "10.216.204.20", "database": "bazooka"}
        # self.conf_fal = {"user": "vtiwari", "password": "vtiwari@123#", "host": "10.216.204.150", "database": "falcon"}

    def falcon_connection(self):
        connection_falcon = mysql.connector.connect(user=self.conf_falcon['user'],
                                                    password=self.conf_falcon['password'],
                                                    host=self.conf_falcon['host'],
                                                    database=self.conf_falcon['database'])
        cursor_falcon = connection_falcon.cursor(dictionary=True)
        return connection_falcon, cursor_falcon

    def dove_insert_connection(self):
        connection = mysql.connector.connect(user=self.conf_dove['user'],
                                             password=self.conf_dove['password'],
                                             host=self.conf_dove['host'],
                                             database=self.conf_dove['database'])
        cursor = connection.cursor(dictionary=True)
        return connection, cursor

    def db2sl_connection(self):
        connection_db2sl = mysql.connector.connect(user=self.conf['user'], password=self.conf['password'],
                                                   host=self.conf['host'],
                                                   database=self.conf['database'])
        cursor_db2sl = connection_db2sl.cursor(dictionary=True)
        return connection_db2sl, cursor_db2sl

    def execute_falcon_query(self, query):
        connection, cursor = self.falcon_connection()
        cursor.execute(query)
        all_data = cursor.fetchall()
        connection.close()
        return all_data

    def execute_db2_query(self, query):
        connection, cursor = self.db2sl_connection()
        cursor.execute(query)
        all_data = cursor.fetchall()
        connection.close()
        return all_data

    def execute_dolphin_query(self, query):
        connection, cursor = self.dove_insert_connection()
        cursor.execute(query)
        all_data = cursor.fetchall()
        connection.close()
        return all_data

    def check_exist(self, alluaddata):
        maindata = []
        update_data = []
        for f_data in alluaddata:
            profileid = f_data.get("profile_id", "")
            tableindex = profileid % 10
            print(profileid,tableindex)
            dolphin_query = "select profile_id from user_active_resume_text_{0} where profile_id in ({1});".format(tableindex,
                                                                                                           profileid)
            # print(query)
            records = self.execute_dolphin_query(dolphin_query)
            print(records)
            if len(records)==0:
                maindata.append(f_data)
            else:
                update_data.append(f_data)
        return maindata, update_data

    def kiwi_profile_ids(self, datas, is_update=False):
        resume_ids = ','.join([str(j.get("kiwi_profile_id")) for j in datas if j.get("kiwi_profile_id", None)])
        if is_update:
            query = "select resume_id as kiwi_profile_id,resume_url from image_resume_url where resume_id in (%s)" % (
                resume_ids)
        else:
            query = "select resume_id as kiwi_profile_id,resume_url from image_resume_url where resume_url !='' and resume_url is not null and resume_id in (%s)" % (
                resume_ids)
        bvalue = self.execute_db2_query(query)
        # print(len(datas),len(bvalue))
        mapdata = self.mappingdata(datas, bvalue)
        print("Map Resume length", len(mapdata))
        Total_records = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            future_to_url = {executor.submit(self.download_doc, url_jb[0], url_jb[1], url_jb[2], url_jb[3], url_jb[4],
                                             url_jb[5]): url_jb
                             for url_jb in
                             mapdata[:]}
            for future in concurrent.futures.as_completed(future_to_url):
                url2 = future_to_url[future]
                try:
                    dataout = future.result()
                    if len(dataout) > 0:
                        Total_records.append(tuple(dataout))
                    # print("completed for record: ", len(Total_records))
                except Exception as exc:
                    print(exc)
                    pass
        print("Total inserted resume text :", len(Total_records))
        if len(Total_records) > 0:
            if is_update:
                self.update_dolphin_db(Total_records, folder_arg)
            else:
                self.insert_dove_db(Total_records, folder_arg)

    def run_process(self):
        last_modified_time = main_directory + "/last_modified_time"
        read_file = open(last_modified_time)
        last_modified_at = read_file.read()
        read_file.close()
        print(last_modified_at)
        lastmodify_at_time = str(last_modified_at)
        print("start time :", lastmodify_at_time)
        query = "select resume_id as kiwi_profile_id,resume_url,modified_time from image_resume_url where modified_time>='%s' order by modified_time asc limit 10" % (
            lastmodify_at_time)
        print(query)
        bazooka_data = self.execute_db2_query(query)
        print(len(bazooka_data), bazooka_data)
        kiwiid_list = []
        for b_value in bazooka_data:
            lastmodify_at_time = b_value.get("modified_time")
            kiwiid_list.append(str(b_value.get("kiwi_profile_id", "")))
        print(lastmodify_at_time)
        if len(kiwiid_list) > 0:
            kiwiid_ids = ','.join(kiwiid_list)
            print(kiwiid_ids)
            falcon_query = "select uad.id as useractivedataid,uad.user_id,up.id as profile_id,up.kiwi_profile_id,up.resume_exists as enabled,uad.active_at as activedate from user_profiles as up left join user_active_data as uad on uad.profile_id=up.id where up.kiwi_profile_id in (%s);" % (
                kiwiid_ids)
            print(falcon_query)
            useractivedata = self.execute_falcon_query(falcon_query)
            print('data : ', len(useractivedata))
            if len(useractivedata) > 0:
                process_data, need_to_update_data = self.check_exist(useractivedata)
                if len(process_data) > 0:
                    self.kiwi_profile_ids(process_data)
                print("Process for update data length ::", str(len(need_to_update_data)) + " Date : " + str(
                    datetime.now()) + " Table index : " + str(arg_val))
                if len(need_to_update_data) > 0:
                    self.kiwi_profile_ids(need_to_update_data, is_update=True)


if __name__ == "__main__":
    obj = ResumeDocstore()
    obj.run_process()
