import os
import mysql
import mysql.connector
import concurrent.futures
import requests, zlib
import psutil

main_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "txtfiles")
resume_insertpid = main_directory+"/resume_insert_pid"
fd = open(resume_insertpid)
pidval = fd.read()
fd.close()
if pidval:
    if int(pidval) in [p.info["pid"] for p in psutil.process_iter(attrs=['pid'])]:
        print ('Process is already running------', pidval)
        exit(0)
pidfile = open(resume_insertpid,'w')
pidfile.write(str(os.getpid()))
pidfile.close()


class ResumeDocstore:
    def __init__(self):
        self.chunk_size = 1000
        self.maxdate = 1554057000000
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

    def insert_log_into_table(self,profile_id,error_text):
        conn,curr = self.dove_insert_connection()
        insert_log_query = "insert into resume_profile_text_logs (`profile_id`,`log_text`) values(%s,%s)"
        records = (profile_id,error_text)
        curr.execute(insert_log_query, records)
        conn.commit()
        conn.close()

    def download_doc(self, url_link, kiwi_profileid, fprofile_id, fuser_id, fuseractivedataid, status):
        temp = []
        resume_text = ""
        try:
            response = requests.get(url_link, timeout=180)
            resume_text1 = response.text.encode("utf8").decode("utf-8")
            resume_text = zlib.compress(bytes(resume_text1.encode("utf8"))).decode('latin1')
        except Exception as e:
            print(e, url_link)
            logfilename = main_directory + "/error_log"
            logtext = "Error in download_doc profile id: " +str(fprofile_id)+" kiwi_profileid: "+str(kiwi_profileid)+" "+ str(e)+" "+str(url_link)
            logopenfile = open(logfilename, 'a')
            logopenfile.write(str(logtext))
            logopenfile.write("\n")
            logopenfile.close()
            self.insert_log_into_table(fprofile_id,logtext)
            pass
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
                        if str(nm.get("resume_url", "")).strip().endswith("."):
                            self.insert_log_into_table(mn.get("profile_id"),"Invalid extension")
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

    def insert_dove_db(self, records,foldarg):
        insert_query = """insert into user_active_resume_text_{0} (`resume_text`,`kiwi_profile_id`,`user_id`,`profile_id`,`user_active_data_id`,`status`) values(%s,%s,%s,%s,%s,%s);""".format(foldarg)
        conn, curr = self.dove_insert_connection()
        curr.executemany(insert_query, records)
        conn.commit()
        conn.close()

    def kiwi_profile_ids(self, datas):
        resume_ids = ','.join([str(j.get("kiwi_profile_id")) for j in datas if j.get("kiwi_profile_id", None)])
        query = "select resume_id as kiwi_profile_id,resume_url from image_resume_url where resume_url !='' and resume_url is not null and resume_id in (%s)" % (
        resume_ids)
        bvalue = self.execute_db2_query(query)
        # print(len(bvalue))
        mapdata = self.mappingdata(datas, bvalue)
        print("Map Resume length",len(mapdata))
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
        print("Total inserted resume text :",len(Total_records))
        if len(Total_records)>0:
            for jd in Total_records:
                folder_arg=jd[3]%10
                print(folder_arg)
                # self.insert_dove_db([jd],folder_arg)


    def check_exist(self, alluaddata):

        profileid_list = [str(m.get("profile_id", "")) for m in alluaddata if m.get("profile_id", None)]
        # profilesids = ','.join(profileid_list)
        maindata = []
        for jd in alluaddata:
            tableindex = int(jd.get("profile_id", ""))%10
            profilesids = str(jd.get("profile_id", ""))
            query = "select profile_id from user_active_resume_text_{0} where profile_id in ({1});".format(tableindex,profilesids)
            # print(query)
            conn, curr = self.dove_insert_connection()
            curr.execute(query)
            records = curr.fetchall()
            conn.close()
            print('Rec===:',len(records))
            if len(records)==0:
                maindata.append(jd)
        return maindata


    def getLoginDetails(self):
        maindata = []
        process_data = []
        try:
            # query = "select uad.id as useractivedataid,uad.user_id,uad.profile_id,up.kiwi_profile_id,up.resume_exists as enabled,uad.active_at as activedate from user_active_data as uad inner join user_profiles as up on uad.profile_id=up.id where (mod(uad.profile_id,10) = {0}) and up.resume_exists=1 and uad.active_at>{1} order by uad.active_at asc LIMIT {2} OFFSET {3};".format(
            # arg_val,last_active_at, batch_size, offset)
            query = "select uad.id as useractivedataid,uad.user_id,uad.profile_id,up.kiwi_profile_id,up.resume_exists as enabled,uad.active_at as activedate from user_active_data as uad inner join user_profiles as up on uad.profile_id=up.id where up.resume_exists=1 and up.id in (89134120, 52919593, 89994254, 36687445, 12714766, 47081861, 6831161, 1177183, 87095274, 86351277, 31605367, 45273149, 23263181, 20387851, 37789541, 50230571, 10184903, 32107233, 29774363, 55545923, 13963163, 24146543, 28974424, 44596114, 6082275, 64319645, 17397885, 34768675, 48856106, 32001656, 57959796, 16773546, 54093556, 11638136, 41760996, 33441177, 32585918, 54765238, 48386048, 19575478, 25997589, 14934532, 86893, 18984033, 42531498, 21920568, 22335218, 13835792, 33523536, 56029705, 48779039, 83162309);"
            print(query)
            useractivedata = self.execute_falcon_query(query)
            print('data : ', len(useractivedata))

            if len(useractivedata) > 0:
                process_data = self.check_exist(useractivedata)
                print("Process data length ::",len(process_data))
                if len(process_data)>0:
                    self.kiwi_profile_ids(process_data)

        except Exception as err:
            logfilename = main_directory + "/error_log"
            logtext = "Error in getLoginDetails : "+ str(err)+" "+ str(process_data)
            logopenfile = open(logfilename,'a')
            logopenfile.write(str(logtext))
            logopenfile.write("\n")
            logopenfile.close()
            print (logtext)
            pass


if __name__ == "__main__":
    obj = ResumeDocstore()
    obj.getLoginDetails()
