import os
import mysql
import mysql.connector
import concurrent.futures
import requests, zlib


class ResumeDocstore:
    def __init__(self, arg_value):
        self.arg_value = arg_value
        self.chunk_size = 1000
        self.mindate = 0
        self.maxdate = 0
        self.table_index = 0
        self.conf_falcon = {"user": "vtiwari", "password": "vtiwari@123##", "host": "10.216.247.108",
            "database": "falcon"}
        self.conf = {"user": "db", "password": "bazookadb", "host": "10.216.204.7", "database": "bazooka"}
        self.conf_dove = {"user": "vtiwari", "password": "GhR42wMRQ}", "host": "10.216.247.119", "database": "dove"}
        # self.conf_fal = {"user": "db", "password": "bazookadb", "host": "10.216.204.20", "database": "bazooka"}
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

    def download_doc(self, url_link, kiwi_profileid, fprofile_id, fuser_id, fuseractivedataid, status):
        temp = []
        resume_text = ""
        try:
            response = requests.get(url_link, timeout=180)
            resume_text1 = response.text.encode("utf8").decode("utf-8")
            resume_text = zlib.compress(bytes(resume_text1.encode("utf8"))).decode('latin1')
        except Exception as e:
            print(e, url_link)
        if len(resume_text) > 5:
            temp.append(resume_text)
            temp.append(kiwi_profileid)
            temp.append(fuser_id)
            temp.append(fprofile_id)
            temp.append(fuseractivedataid)
            temp.append(status)
        return temp

    def mappingdata(self, mainlistdict, blistdict):
        Total_records = []
        for mn in mainlistdict:
            temp = []
            for nm in blistdict:
                if mn.get("kiwi_profile_id") == nm.get("kiwi_profile_id"):
                    if nm.get("resume_url", None):
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

    def insert_falcon_db(self, records,foldarg):
        insert_query = """insert into user_active_resume_text_{0} (`resume_text`,`kiwi_profile_id`,`user_id`,`profile_id`,`user_active_data_id`,`status`) values(%s,%s,%s,%s,%s,%s);""".format(foldarg)
        conn, curr = self.dove_insert_connection()
        curr.executemany(insert_query, records)
        conn.commit()
        conn.close()

    def kiwi_profile_ids(self, datas,folder_arg):
        resume_ids = ','.join([str(j.get("kiwi_profile_id")) for j in datas if j.get("kiwi_profile_id", None)])
        query = "select resume_id as kiwi_profile_id,resume_url from image_resume_url where resume_url !='' and resume_url is not null and resume_id in (%s)" % (
        resume_ids)
        bvalue = self.execute_db2_query(query)
        # print(len(bvalue))
        mapdata = self.mappingdata(datas, bvalue)
        print(len(mapdata))
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
        print(len(Total_records))
        self.insert_falcon_db(Total_records,folder_arg)
        # return Total_records
    def check_exist(self, alluaddata,tableindex):
        conn,curr = self.dove_insert_connection()
        profileid_list = [str(m.get("profile_id", "")) for m in alluaddata if m.get("profile_id", None)]
        profilesids = ','.join(profileid_list)
        query = "select profile_id from user_active_resume_text_{0} where profile_id in ({1});".format(tableindex,profilesids)
        # print(query)
        curr.execute(query)
        records = curr.fetchall()
        conn.close()
        dict ={}
        maindata=[]
        for j in records:
            dict[j.get("profile_id")]=j.get("profile_id")
        print("match_records",len(records),len(alluaddata))
        for da in alluaddata:
            if da.get("profile_id")!=dict.get(da.get("profile_id")):
                maindata.append(da)
        return maindata

    def check_running_cron(self, is_last_update=False):
        connection, cursor = self.dove_insert_connection()
        if is_last_update:
            sql_upate = """Update replication_tables set descr='0',end_time=now() where code='PHY_CV_TEXT_SCRIPT_STATUS';"""
            cursor.execute(sql_upate)
            connection.commit()
            connection.close()
        else:
            sql = """select * from replication_tables where code='PHY_CV_TEXT_SCRIPT_STATUS';"""
            # print(sql)
            cursor.execute(sql)
            results = cursor.fetchone()
            is_running = False
            if results.get("descr", '0') == '1':
                is_running = True
            else:
                sql_upate = """Update replication_tables set descr='1',start_time=now() where code='PHY_CV_TEXT_SCRIPT_STATUS';"""
                # cursor.execute(sql_upate)
                # connection.commit()
        connection.close()
        return is_running

    def get_last_active_at(self, is_end=False):
        conn, curr = self.dove_insert_connection()
        if is_end:
            sql_upate = """Update replication_tables set end_time=now() where code='PHY_CV_TEXT_JOB_EXECUTING';"""
            curr.execute(sql_upate)
            conn.commit()
            conn.close()
        else:
            sql = """select * from replication_tables where code='PHY_CV_TEXT_JOB_EXECUTING';"""
            print(sql)
            curr.execute(sql)
            results = curr.fetchone()
            is_running = False
            if results.get("descr", None):
                index_with_active_at = results.get("descr", "").split("_")
                self.table_index = int(index_with_active_at[0])
                self.mindate = int(index_with_active_at[-1])
                self.maxdate = int(results.get("last_active_at", 0))

    def update_replication_table(self, lastactiveatid):
        conn, curr = self.dove_insert_connection()
        sql_upate = """Update replication_tables set descr='%s',start_time=now(),end_time=now() where code='LOGIN_DTL_SALESFORCE';""" % (str(lastactiveatid))
        # print sql_upate
        curr.execute(sql_upate)
        conn.commit()
        conn.close()


    def getLoginDetails(self):
        maindata = []
        for arg_val in range(self.table_index, 10):
            query_count = "select count(*) as count from user_active_data as uad inner join user_profiles as up on uad.profile_id=up.id where (mod(uad.profile_id,10) = {0}) and up.resume_exists=1 and uad.active_at between {1} and {2};".format(
            str(arg_val),self.mindate,self.maxdate)
            # query_count = "select count(*) as count from user_active_data as uad inner join user_profiles as up on uad.profile_id=up.id where (mod(uad.id,10) = %s) limit 100000" % (
            #     str(arg_val))
            print (query_count)
            fcount = self.execute_falcon_query(query_count)
            count = fcount[0].get("count", 0)
            print('Total count :', count)
            batch_size = self.chunk_size
            lastactive_at_time = 0
            for offset in range(0, count, batch_size):
                query = "select uad.id as useractivedataid,uad.user_id,uad.profile_id,up.kiwi_profile_id,up.resume_exists as enabled,uad.active_at as activedate from user_active_data as uad inner join user_profiles as up on uad.profile_id=up.id where (mod(uad.profile_id,10) = {0}) and up.resume_exists=1 and uad.active_at between {1} and {2} order by uad.active_at asc LIMIT {3} OFFSET {4};".format(
                arg_val,self.mindate,self.maxdate, batch_size, offset)
                # query = "select uad.id as useractivedataid,uad.user_id,uad.profile_id,up.kiwi_profile_id from user_active_data as uad inner join user_profiles as up on uad.profile_id=up.id where (mod(uad.id,10) = %s) order by uad.id asc LIMIT %s OFFSET %s;" % ( 1580495400000 and 1583389800000
                #     arg_val, batch_size, offset)
                print(query)
                useractivedata = self.execute_falcon_query(query)
                print('data : ', len(useractivedata),useractivedata)
                for j in useractivedata:
                    lastactive_at_time = j.get("activedate")
                    print(lastactive_at_time)
                    lastactiveat_with_tbindex = str(arg_val)+"_"+str(lastactive_at_time)
                    print(lastactiveat_with_tbindex)
                    self.update_replication_table(lastactiveat_with_tbindex)
                    if len(useractivedata) > 0:
                        process_data = self.check_exist(useractivedata, arg_val)
                        print("Process data length ::",len(process_data))
                        if len(process_data)>0:
                            self.kiwi_profile_ids(process_data, arg_val)


    def start_falcon_cv_text_data(self):
        is_run = self.check_running_cron()
        is_run = False
        print('Starting Script : ', is_run)
        if not is_run:
            self.get_last_active_at()
            # print('start', self.maxdate, self.maxdate)
            self.getLoginDetails()
            print("Successfully completed : ", str(datetime.now()))
        else:
            print("Cron already running")

if __name__ == "__main__":
    obj = ResumeDocstore(1)
    obj.start_falcon_cv_text_data()
