import os
import mysql
import mysql.connector
import concurrent.futures
import requests


class ResumeDocstore:
    def __init__(self, arg_value):
        self.arg_value = arg_value
        self.chunk_size = 10000
        self.conf_falcon = {"user": "vtiwari", "password": "vtiwari@123##", "host": "10.216.247.108",
                            "database": "falcon"}
        self.conf = {"user": "db", "password": "bazookadb", "host": "10.216.204.7", "database": "bazooka"}
        # self.conf_fal = {"user": "db", "password": "bazookadb", "host": "10.216.204.20", "database": "bazooka"}
        self.conf_fal = {"user": "vtiwari", "password": "vtiwari@123#", "host": "10.216.204.150", "database": "falcon"}

    def falcon_connection(self):
        connection_falcon = mysql.connector.connect(user=self.conf_falcon['user'],
                                                    password=self.conf_falcon['password'],
                                                    host=self.conf_falcon['host'],
                                                    database=self.conf_falcon['database'])
        cursor_falcon = connection_falcon.cursor(dictionary=True)
        return connection_falcon, cursor_falcon

    def falcon_insert_connection(self):
        connection = mysql.connector.connect(user=self.conf_fal['user'],
                                                    password=self.conf_fal['password'],
                                                    host=self.conf_fal['host'],
                                                    database=self.conf_fal['database'])
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

    def download_doc(self, url_link, kiwi_profileid,fprofile_id,fuser_id,fuseractivedataid, status):
        temp=[]
        resume_text = ""
        try:
            response = requests.get(url_link,timeout=180)
            resume_text = response.text.encode("utf8").decode("utf-8")
        except Exception as e:
            print(e,url_link)
        if len(resume_text)>5:
            temp.append(resume_text)
            temp.append(kiwi_profileid)
            temp.append(fuser_id)
            temp.append(fprofile_id)
            temp.append(fuseractivedataid)
            temp.append(status)
        return temp

    def mappingdata(self,mainlistdict,blistdict):
        Total_records = []
        for mn in mainlistdict:
            temp = []
            for nm in blistdict:
                if mn.get("kiwi_profile_id")==nm.get("kiwi_profile_id"):
                    if nm.get("resume_url",None):
                        heads, tail = os.path.split(nm.get("resume_url", ""))
                        tails = tail.split(".")[0] + ".txt"
                        filepath = heads + "/" + tails
                        temp.append(filepath)
                        temp.append(nm.get("kiwi_profile_id"))
                        temp.append(mn.get("profile_id"))
                        temp.append(mn.get("user_id"))
                        temp.append(mn.get("useractivedataid"))
                        temp.append(mn.get("enabled"))
            if len(temp)>0:
                Total_records.append(temp)
        return Total_records

    def insert_falcon_db(self,records):
        insert_query = """insert into user_active_resume_text (`resume_text`,`kiwi_profile_id`,`user_id`,`profile_id`,`user_active_data_id`,`status`) values(%s,%s,%s,%s,%s,%s);"""
        conn, curr = self.falcon_insert_connection()
        curr.executemany(insert_query,records)
        conn.commit()
        conn.close()

    def kiwi_profile_ids(self,datas):
        resume_ids = ','.join([str(j.get("kiwi_profile_id")) for j in datas if j.get("kiwi_profile_id",None) ])
        query = "select resume_id as kiwi_profile_id,resume_url from image_resume_url where resume_url !='' and resume_url is not null and resume_id in (%s)"%(resume_ids)
        bvalue  = self.execute_db2_query(query)
        # print(len(bvalue))
        mapdata = self.mappingdata(datas,bvalue)
        # print(len(mapdata),mapdata)
        Total_records = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            future_to_url = {executor.submit(self.download_doc, url_jb[0], url_jb[1], url_jb[2],url_jb[3],url_jb[4],url_jb[5]): url_jb
                             for url_jb in
                             mapdata[:]}
            for future in concurrent.futures.as_completed(future_to_url):
                url2 = future_to_url[future]
                try:
                    dataout = future.result()
                    if len(dataout)>0:
                        Total_records.append(tuple(dataout))
                    # print("completed for record: ", len(Total_records))
                except Exception as exc:
                    # print(exc)
                    pass
        print(len(Total_records))
        self.insert_falcon_db(Total_records)
        # return Total_records

    def getLoginDetails(self):
        sum=0
        maindata = []
        for arg_val in range(0,10):
            query_count = "select count(*) as count from user_active_data as uad inner join user_profiles as up on uad.profile_id=up.id where (mod(uad.id,10) = %s) and  uad.created_at between 1580495400000 and 1583389800000;"%(str(arg_val))
            # query_count = "select count(*) as count from user_active_data as uad inner join user_profiles as up on uad.profile_id=up.id where (mod(uad.id,10) = %s) limit 100000" % (
            #     str(arg_val))
            fcount = self.execute_falcon_query(query_count)
            count = fcount[0].get("count",0)
            print('Total count :',count)
            # count = 100000
            batch_size = self.chunk_size
            for offset in range(0, count, batch_size):
                if offset!=0:
                    query = "select uad.id as useractivedataid,uad.user_id,uad.profile_id,up.kiwi_profile_id,up.resume_exists as enabled from user_active_data as uad inner join user_profiles as up on uad.profile_id=up.id where (mod(uad.id,10) = %s) and  uad.created_at between 1580495400000 and 1583389800000 order by uad.id asc LIMIT %s OFFSET %s;" % (
                    arg_val, batch_size, offset)
                    # query = "select uad.id as useractivedataid,uad.user_id,uad.profile_id,up.kiwi_profile_id from user_active_data as uad inner join user_profiles as up on uad.profile_id=up.id where (mod(uad.id,10) = %s) order by uad.id asc LIMIT %s OFFSET %s;" % (
                    #     arg_val, batch_size, offset)
                    print(query)
                    data = self.execute_falcon_query(query)
                    print('data : ', len(data))
                    sum+=len(data)
                    if len(data)>0:
                        textdata = self.kiwi_profile_ids(data)
                # maindata.extend(textdata)
        print(sum, len(maindata))


if __name__ == "__main__":
    obj = ResumeDocstore(1)
    obj.getLoginDetails()
