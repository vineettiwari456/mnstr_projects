import os
import mysql
import mysql.connector
import concurrent.futures
import requests, zlib
import psutil,time

main_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "txtfiles")
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
        self.maxdate = int(time.time())*1000
        self.conf_falcon = {"user": "vtiwari", "password": "vtiwari@123##", "host": "10.216.247.108",
                            "database": "falcon"}
        # self.conf = {"user": "db", "password": "bazookadb", "host": "10.216.204.7", "database": "bazooka"}
        self.conf = {"user": "asteam", "password": "Asteam@123##", "host": "10.216.240.118", "database": "bazooka"}
        self.conf_dove = {"user": "vtiwari", "password": "GhR42wMRQ}", "host": "10.216.247.119", "database": "dolphin"}
        # self.conf_dove = {"user": "db", "password": "bazookadb", "host": "10.216.204.20", "database": "bazooka"}
        # self.conf_fal = {"user": "vtiwari", "password": "vtiwari@123#", "host": "10.216.204.150", "database": "falcon"}
        self.headers = {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; WOW64)\
                AppleWebKit/537.36 (KHTML'}
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

    def insert_log_into_table(self, profile_id, error_text):
        conn, curr = self.dove_insert_connection()
        insert_log_query = "insert into resume_profile_text_logs (`profile_id`,`log_text`) values(%s,%s)"
        records = (profile_id, error_text)
        curr.execute(insert_log_query, records)
        conn.commit()
        conn.close()

    def download_doc(self, url_link, kiwi_profileid, fprofile_id, fuser_id, fuseractivedataid, status):
        temp = []
        resume_text = ""
        try:
            response = requests.get(url_link, headers=self.headers,timeout=60)
            if response.status_code==200:
                resume_text1 = response.text.encode("utf8").decode("utf-8")
                resume_text = zlib.compress(bytes(resume_text1.encode("utf8"))).decode('latin1')
        except Exception as e:
            print(e, url_link)
            logfilename = main_directory + "/error_log"
            logtext = "Error in download_doc profile id: " + str(fprofile_id) + " kiwi_profileid: " + str(
                kiwi_profileid) + " " + str(e) + " " + str(url_link)
            logopenfile = open(logfilename, 'a')
            logopenfile.write(str(logtext))
            logopenfile.write("\n")
            logopenfile.close()
            self.insert_log_into_table(fprofile_id, logtext)
            pass
        if len(resume_text) > 5:
            temp.append(resume_text)
            temp.append(kiwi_profileid)
            temp.append(fuser_id)
            temp.append(fprofile_id)
            temp.append(fuseractivedataid)
            temp.append(status)
            # temp.append(resume_text)
        return temp

    def mappingdata(self, mainlistdict, blistdict):
        Total_records = []
        for mn in mainlistdict:
            temp = []
            for nm in blistdict:
                if mn.get("kiwi_profile_id") == nm.get("kiwi_profile_id"):
                    if nm.get("resume_url", None):
                        if str(nm.get("resume_url", "")).strip().endswith("."):
                            self.insert_log_into_table(mn.get("profile_id"), "Invalid extension")
                        else:
                            heads, tail = os.path.split(nm.get("resume_url", ""))
                            tails = tail.split(".")[0] + ".txt"
                            filepath = heads + "/" + tails
                            temp.append(filepath)
                            temp.append(nm.get("kiwi_profile_id"))
                            temp.append(mn.get("profile_id"))
                            temp.append(mn.get("user_id"))
                            temp.append(mn.get("useractivedataid"))
                            temp.append(mn.get("enabled"))
            if len(temp) > 0:
                Total_records.append(temp)
        return Total_records

    def insert_dove_db(self, records, foldarg):
        insert_query = """insert into user_active_resume_text_{0} (`resume_text`,`kiwi_profile_id`,`user_id`,`profile_id`,`user_active_data_id`,`status`) values(%s,%s,%s,%s,%s,%s);""".format(
            foldarg)
        conn, curr = self.dove_insert_connection()
        curr.executemany(insert_query, records)
        conn.commit()
        conn.close()

    def update_dove_db(self, records, foldarg):
        update_query = """update user_active_resume_text_{0} set `resume_text`=%s,`status`=%s,`user_active_data_id`=%s where`kiwi_profile_id`=%s and`user_id`=%s and`profile_id`=%s;""".format(
            foldarg)
        conn, curr = self.dove_insert_connection()
        curr.executemany(update_query, records)
        conn.commit()
        conn.close()

    def insert_dove_db_old(self, records, foldarg):
        insert_query = """insert into user_active_resume_text_{0} (`resume_text`,`kiwi_profile_id`,`user_id`,`profile_id`,`user_active_data_id`,`status`) values(%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE resume_text=%s;""".format(
            foldarg)
        conn, curr = self.dove_insert_connection()
        for rec in records:
            # print (insert_query,len(rec))
            # exc_query = insert_query%(rec)
            curr.execute(insert_query, rec)
        conn.commit()
        conn.close()

    def updatedata_for_update_query(self,records):
        tp_records=[]
        for rec in records:
            tp_records.append((rec[0],rec[5],rec[4],rec[1],rec[2],rec[3]))
        return tp_records

    def kiwi_profile_ids(self, datas, folder_arg,is_update=False):
        resume_ids = ','.join([str(j.get("kiwi_profile_id")) for j in datas if j.get("kiwi_profile_id", None)])
        mapdata=[]
        if resume_ids:
            query = "select resume_id as kiwi_profile_id,resume_url from image_resume_url where resume_url !='' and resume_url is not null and resume_id in (%s)" % (
                resume_ids)
            bvalue = self.execute_db2_query(query)
            # print(len(bvalue))
            mapdata = self.mappingdata(datas, bvalue)
            print("Map Resume length", len(mapdata))
        Total_records = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
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
        print("Total process insertion/updation resume text :", len(Total_records))
        # if len(Total_records) > 0:
        #     if is_update:
        #         update_records = self.updatedata_for_update_query(Total_records)
        #         print("Total updated resume text :", len(update_records))
        #         self.update_dove_db(update_records, folder_arg)
        #     else:
        #         print("Total inserted resume text :", len(Total_records))
        #         self.insert_dove_db(Total_records, folder_arg)

    def check_exist(self, alluaddata, tableindex):
        conn, curr = self.dove_insert_connection()
        profileid_list = [str(m.get("profile_id", "")) for m in alluaddata if m.get("profile_id", None)]
        profilesids = ','.join(profileid_list)
        query = "select profile_id from user_active_resume_text_{0} where profile_id in ({1});".format(tableindex,
                                                                                                       profilesids)
        # print(query)
        curr.execute(query)
        records = curr.fetchall()
        conn.close()
        dict = {}
        maindata = []
        matched_data=[]
        for j in records:
            dict[j.get("profile_id")] = j.get("profile_id")
        print("match_records", len(records), len(alluaddata))
        for da in alluaddata:
            if da.get("profile_id") != dict.get(da.get("profile_id")):
                maindata.append(da)
            else:
                matched_data.append(da)
        return maindata,matched_data

    def getLoginDetails(self):
        maindata = []
        for arg_val in range(4, 5):
            last_activeatinsert_id = main_directory + "/last_active_at_insert_time_" + str(arg_val)
            read_file = open(last_activeatinsert_id)
            last_active_at = read_file.read()
            read_file.close()
            print(last_active_at)
            lastactive_at_time = int(last_active_at)
            print("start time :", lastactive_at_time)
            is_next = True
            while lastactive_at_time <= self.maxdate and is_next:
                process_data = []
                try:
                    # query = "select uad.id as useractivedataid,uad.user_id,uad.profile_id,up.kiwi_profile_id,up.resume_exists as enabled,uad.active_at as activedate from user_active_data as uad inner join user_profiles as up on uad.profile_id=up.id where (mod(uad.profile_id,10) = {0}) and up.resume_exists=1 and uad.active_at>{1} order by uad.active_at asc LIMIT {2} OFFSET {3};".format(
                    # arg_val,last_active_at, batch_size, offset)
                    query = "select uad.id as useractivedataid,uad.user_id,uad.profile_id,up.kiwi_profile_id,up.resume_exists as enabled,uad.active_at as activedate from user_active_data as uad inner join user_profiles as up on uad.profile_id=up.id where (mod(uad.profile_id,10) = {0}) and up.resume_exists=1 and uad.active_at between {1} and {2} order by uad.active_at asc LIMIT {3};".format(
                        arg_val, lastactive_at_time, self.maxdate, self.chunk_size)
                    print(query)
                    useractivedata = self.execute_falcon_query(query)
                    print('data : ', len(useractivedata))
                    for j in useractivedata:
                        lastactive_at_time = j.get("activedate")
                    print("last time of chunck : ", lastactive_at_time)
                    if len(useractivedata) > 0:
                        # process_data=useractivedata[:]
                        process_data,process_for_update = self.check_exist(useractivedata, arg_val)
                        print("Process data length ::", len(process_data),len(process_for_update))
                        process_data=[{'useractivedataid': 62383646, 'user_id': 78319800, 'profile_id': 92175814, 'kiwi_profile_id': None, 'enabled': 1, 'activedate': 1615551902935}]
                        if len(process_data) > 0:
                            self.kiwi_profile_ids(process_data, arg_val)
                        # if len(process_for_update)>0:
                        #     self.kiwi_profile_ids(process_for_update, arg_val,is_update=True)
                    # last_writeid = open(last_activeatinsert_id, 'w')
                    # last_writeid.write(str(lastactive_at_time))
                    # last_writeid.close()
                    if len(process_data) == 0 or len(useractivedata)<2:
                        is_next = False
                except Exception as err:
                    logfilename = main_directory + "/error_log"
                    logtext = "Error in getLoginDetails : " + str(err) + " " + str(process_data)
                    logopenfile = open(logfilename, 'a')
                    logopenfile.write(str(logtext))
                    logopenfile.write("\n")
                    logopenfile.close()
                    print(logtext)
                    pass


if __name__ == "__main__":
    obj = ResumeDocstore()
    obj.getLoginDetails()



