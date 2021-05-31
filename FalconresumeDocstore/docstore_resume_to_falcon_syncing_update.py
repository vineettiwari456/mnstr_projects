import os
import mysql
import mysql.connector
import concurrent.futures
import requests, zlib
import psutil

main_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sync_ids")
resume_insertpid = main_directory + "/resume_update_pid"
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


class ResumeDocstoreUpdate:
    def __init__(self):
        self.chunk_size = 1000
        self.conf_falcon = {"user": "vtiwari", "password": "vtiwari@123##", "host": "10.216.247.108",
                            "database": "falcon"}
        # self.conf = {"user": "db", "password": "bazookadb", "host": "10.216.204.7", "database": "bazooka"}
        self.conf = {"user": "asteam", "password": "Asteam@123##", "host": "10.216.240.118", "database": "bazooka"}
        self.conf_dove = {"user": "vtiwari", "password": "GhR42wMRQ}", "host": "10.216.247.119", "database": "dolphin"}
        # self.conf_dove = {"user": "db", "password": "bazookadb", "host": "10.216.204.20", "database": "bazooka"}
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

    def insert_log_into_table(self, profile_id, error_text):
        conn, curr = self.dove_insert_connection()
        insert_log_query = "insert into resume_profile_text_logs (`profile_id`,`log_text`) values(%s,%s)"
        records = (profile_id, error_text)
        curr.execute(insert_log_query, records)
        conn.commit()
        conn.close()

    def download_doc(self, url_link, fprofile_id, status):
        temp = []
        resume_text = ""
        try:
            response = requests.get(url_link, timeout=180)
            resume_text1 = response.text.encode("utf8").decode("utf-8")
            resume_text = zlib.compress(bytes(resume_text1.encode("utf8"))).decode('latin1')
        except Exception as e:
            print(e, url_link)
            logfilename = main_directory + "/update_error_log"
            logtext = "Error in download_doc profile id: " + str(fprofile_id) + str(e) + " " + str(url_link)
            logopenfile = open(logfilename, 'a')
            logopenfile.write(str(logtext))
            logopenfile.write("\n")
            logopenfile.close()
            self.insert_log_into_table(fprofile_id, logtext)
            pass
        if len(resume_text) > 5:
            temp.append(resume_text)
            temp.append(status)
            temp.append(fprofile_id)
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
                            temp.append(mn.get("profile_id"))
                            temp.append(mn.get("enabled"))
            if len(temp) > 0:
                Total_records.append(temp)
        return Total_records

    def update_dolphin_db(self, update_records, table_index):
        for updat_data in update_records:
            try:
                conn, curr = self.dove_insert_connection()
                update_query = "update user_active_resume_text_{0} set resume_text={1},status={2} where profile_id={3};".format(
                    table_index, updat_data[0], updat_data[1], updat_data[2])
                curr.execute(update_query)
                conn.commit()
                conn.close()
            except Exception as exc:
                self.insert_log_into_table(updat_data[2], "DB Error: "+str(exc))

    def kiwi_profile_ids(self, datas, folder_arg, is_update=False):
        resume_ids = ','.join([str(j.get("kiwi_profile_id")) for j in datas if j.get("kiwi_profile_id", None)])
        query = "select resume_id as kiwi_profile_id,resume_url from image_resume_url where resume_id in (%s)" % (
            resume_ids)
        bvalue = self.execute_db2_query(query)
        # print(len(bvalue))
        mapdata = self.mappingdata(datas, bvalue)
        print("Map Resume length", len(mapdata))
        Total_records = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            future_to_url = {executor.submit(self.download_doc, url_jb[0], url_jb[1], url_jb[2]): url_jb
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
        print("Total updated resume text :", len(Total_records))
        if len(Total_records) > 0:
            if is_update:
                self.update_dolphin_db(Total_records, folder_arg)

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
        update_records = []
        for j in records:
            dict[j.get("profile_id")] = j.get("profile_id")
        print("match_records", len(records), len(alluaddata))
        for da in alluaddata:
            if da.get("profile_id") == dict.get(da.get("profile_id")):
                update_records.append(da)
        return update_records

    def getLoginDetails(self):
        maindata = []
        for arg_val in range(1, 10):
            last_activeatupdate_id = main_directory + "/last_active_at_update_time_" + str(arg_val)
            read_file = open(last_activeatupdate_id)
            last_active_at = read_file.read()
            read_file.close()
            print(last_active_at)
            lastactive_at_time = int(last_active_at)
            print("start time :", lastactive_at_time)
            is_next = True
            while lastactive_at_time and is_next:
                need_to_update = []
                try:
                    # query = "select uad.id as useractivedataid,uad.user_id,uad.profile_id,up.kiwi_profile_id,up.resume_exists as enabled,uad.active_at as activedate from user_active_data as uad inner join user_profiles as up on uad.profile_id=up.id where (mod(uad.profile_id,10) = {0}) and up.resume_exists=1 and uad.active_at>{1} order by uad.active_at asc LIMIT {2} OFFSET {3};".format(
                    # arg_val,last_active_at, batch_size, offset)
                    query = "select uad.profile_id,up.kiwi_profile_id,up.resume_exists as enabled,uad.active_at as activedate from user_active_data as uad inner join user_profiles as up on uad.profile_id=up.id where (mod(uad.profile_id,10) = {0}) and up.profile_updated_at>{1} order by uad.profile_updated_at asc LIMIT {2};".format(
                        arg_val, lastactive_at_time, self.chunk_size)
                    print(query)
                    useractivedata = self.execute_falcon_query(query)
                    print('data : ', len(useractivedata))
                    for j in useractivedata:
                        lastactive_at_time = j.get("activedate")
                    print("last time of chunck : ", lastactive_at_time)
                    if len(useractivedata) > 0:
                        need_to_update = self.check_exist(useractivedata, arg_val)
                        print("Process data length ::", len(need_to_update))
                        if len(need_to_update) > 0:
                            self.kiwi_profile_ids(need_to_update, arg_val)
                    last_writeid = open(last_activeatupdate_id, 'w')
                    last_writeid.write(str(lastactive_at_time))
                    last_writeid.close()
                    if len(useractivedata) == 0:
                        is_next = False
                except Exception as err:
                    logfilename = main_directory + "/update_error_log"
                    logtext = "Error in getLoginDetails : " + str(err) + " " + str(need_to_update)
                    logopenfile = open(logfilename, 'a')
                    logopenfile.write(str(logtext))
                    logopenfile.write("\n")
                    logopenfile.close()
                    print(logtext)
                    pass


if __name__ == "__main__":
    obj = ResumeDocstoreUpdate()
    obj.getLoginDetails()
