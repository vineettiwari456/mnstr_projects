# -*-coding:utf-8 -*-
import os
import mysql.connector
from datetime import datetime
import csv, ast, json
from base64 import b64decode
import sys, time
import requests
from datetime import datetime
import concurrent.futures

reload(sys)
sys.setdefaultencoding('utf8')


class SalesForceToPython:
    def __init__(self):
        self.count = 1800000
        self.conf_falcon = {"user": "vtiwari", "password": "vtiwari@123##", "host": "10.216.247.108",
                            "database": "falcon"}
        self.conf_rio = {"user": "vtiwari", "password": "vtiwari@123##", "host": "10.216.247.113",
                         "database": "rio"}
        self.conf_salesforce = {"user": "vtiwari", "password": "vtiwari@123##", "host": "10.216.247.113",
                                "database": "salesforce"}
        # self.conf_salesforce = {"user": "vtiwari", "password": "vtiwari@123##", "host": "10.216.204.151",
        #                         "database": "salesforce"}

        self.headers = {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; WOW64)\
                AppleWebKit/537.36 (KHTML'}
        self.data = {"grant_type": "refresh_token",
                     "refresh_token": "5Aep8613hy0tHCYdhy8kgAYukrhbCZdid46vVpFzUUj7zUytO7H6vsVphlwqA5rxCCYiw6J6cqdJGUopxu_._EQ",
                     "client_id": "3MVG9d8..z.hDcPI.oXp8sBWD5ucRoFEc9qhQrrR5Lb4RIb8TBbgnvUfBE5PmAo8Ncgyae_qL3GScJI4mMb0z",
                     "client_secret": "892594666719033861",
                     "format": "json"}
        self.url = 'https://login.salesforce.com/services/oauth2/token'
        self.res = requests.post(self.url, data=self.data, headers=self.headers)
        self.load_data()

    def falcon_connection(self):
        connection_falcon = mysql.connector.connect(user=self.conf_falcon['user'],
                                                    password=self.conf_falcon['password'],
                                                    host=self.conf_falcon['host'],
                                                    database=self.conf_falcon['database'])
        cursor_falcon = connection_falcon.cursor(dictionary=True)
        return connection_falcon, cursor_falcon

    def rio_connection(self):
        connection_rio = mysql.connector.connect(user=self.conf_rio['user'],
                                                 password=self.conf_rio['password'],
                                                 host=self.conf_rio['host'],
                                                 database=self.conf_rio['database'])
        cursor_rio = connection_rio.cursor(dictionary=True)
        return connection_rio, cursor_rio

    def salesforce_connection(self):
        connection_sales = mysql.connector.connect(user=self.conf_salesforce['user'],
                                                   password=self.conf_salesforce['password'],
                                                   host=self.conf_salesforce['host'],
                                                   database=self.conf_salesforce['database'])
        cursor_sales = connection_sales.cursor(dictionary=True)
        return connection_sales, cursor_sales

    def get_key_value_format_mapping(self, total_data):
        result = {}
        for da in total_data:
            da_list = da.values()
            result[str(da_list[0]).lower().strip()] = da_list[1]
        return result

    def get_key_value_format_mapping_currency(self, total_data):
        result = {}
        for da in total_data:
            result[str(da.get("currency_code")).lower()] = str(da.get("name")).strip()
        return result

    def load_data(self):
        conn_master, cursor_master = self.falcon_connection()
        query_master_job_location = 'select jl.uuid,jll.name from job_locations as jl inner join job_location_langs as jll on jl.id=jll.job_location_id;'
        cursor_master.execute(query_master_job_location)
        countries_locuuid_raw = cursor_master.fetchall()
        self.job_locationuuid_name_master = self.get_key_value_format_mapping(countries_locuuid_raw)
        query_master_job_country = 'select nal.uuid,nall.name from nationalities as nal inner join nationality_langs as nall on nal.id=nall.nationality_id;'
        cursor_master.execute(query_master_job_country)
        countries_raw = cursor_master.fetchall()
        self.job_country_name_master = self.get_key_value_format_mapping(countries_raw)

        query_master_currencyname = 'select currency_code,name from currencies;'
        cursor_master.execute(query_master_currencyname)
        currencyname_raw = cursor_master.fetchall()
        self.currencyname_name_master = self.get_key_value_format_mapping_currency(currencyname_raw)
        #
        query_master_indus = 'select ir.uuid,il.name from industries as ir inner join industry_langs as il on ir.id=il.industry_id;;'
        cursor_master.execute(query_master_indus)
        industry_raw = cursor_master.fetchall()
        self.industry_data_master = self.get_key_value_format_mapping(industry_raw)
        # # print industry_data_master
        query_master_category = 'select far.uuid,farl.name from function_and_roles as far inner join function_and_role_langs as farl on far.id=farl.function_and_role_id;'
        cursor_master.execute(query_master_category)
        category_raw = cursor_master.fetchall()
        self.category_data_master = self.get_key_value_format_mapping(category_raw)

        query_master_currency = 'select sm.uuid,sml.name from salary_modes as sm inner join salary_mode_langs as sml on sm.id=sml.salary_mode_id;'
        cursor_master.execute(query_master_currency)
        ds_salary = cursor_master.fetchall()
        self.salary_data_master = self.get_key_value_format_mapping(ds_salary)

        query_master_county = 'select con.iso_code,contl.name from countries as con inner join country_langs as contl on con.id=contl.country_id;'
        cursor_master.execute(query_master_county)
        ds_county = cursor_master.fetchall()
        self.country_data_master = self.get_key_value_format_mapping(ds_county)
        conn_master.close()

    def get_key_value_format_mapping_rio(self, datalist):
        dictrio = {}
        for usval in datalist:
            dictrio[usval.get("userid")] = usval
        return dictrio

    def get_key_value_format_mapping_rio_email(self, datalist):
        dictrio = {}
        for usval in datalist:
            if dictrio.get(usval.get("userid"), None):
                dictrio.get(usval.get("userid")).append(usval)
            else:
                dictrio[usval.get("userid")] = [usval]

        return dictrio

    def load_rio_data(self, uids):
        uidsl = ','.join([str(i) for i in uids])
        conn_master, cursor_master = self.rio_connection()
        query_master_email = 'select user_id as userid,email as main_data,primary_email as is_primary from user_emails where user_id in (%s);' % (
            uidsl)
        cursor_master.execute(query_master_email)
        ds_email = cursor_master.fetchall()
        email_data_master = self.get_key_value_format_mapping_rio_email(ds_email)
        query_master_phone = 'select user_id as userid,number as main_data,primary_contact as is_primary from user_contact_numbers where user_id in (%s);' % (
            uidsl)
        cursor_master.execute(query_master_phone)
        ds_phone = cursor_master.fetchall()
        phone_data_master = self.get_key_value_format_mapping_rio(ds_phone)
        conn_master.close()
        return email_data_master, phone_data_master

    def get_key_value_format_mapping_falcon(self, datalist):
        dict = {}
        for jk in datalist:
            if jk.get("job_pref_id", None):
                try:
                    dict[jk.get("job_pref_id")].append(jk)
                except:
                    dict[jk.get("job_pref_id")] = [jk]
        return dict

    def load_falcon_role_industry_function_data(self, uids):
        function_data_master, roles_data_master, industry_data_master = {}, {}, {}
        if len(uids) > 0:
            uidsl = ','.join([str(i) for i in uids])
            conn_master, cursor_master = self.falcon_connection()
            query_master_function = 'select job_preferences_id as job_pref_id,function_uuid as uuid,function_text as text from user_job_preferences_functions where (deleted!=1 or deleted is null) and job_preferences_id in (%s);' % (
                uidsl)
            cursor_master.execute(query_master_function)
            ds_function = cursor_master.fetchall()
            function_data_master = self.get_key_value_format_mapping_falcon(ds_function)

            query_master_role = 'select job_preferences_id as job_pref_id,role_uuid as uuid,role_text as text from user_job_preferences_roles where (deleted!=1 or deleted is null) and job_preferences_id in (%s);' % (
                uidsl)
            cursor_master.execute(query_master_role)
            ds_roles = cursor_master.fetchall()
            roles_data_master = self.get_key_value_format_mapping_falcon(ds_roles)

            query_master_industry = 'select job_preferences_id as job_pref_id,industry_uuid as uuid,industry_text as text from user_job_preferences_industries where (deleted!=1 or deleted is null) and job_preferences_id in (%s);' % (
                uidsl)
            cursor_master.execute(query_master_industry)
            ds_industries = cursor_master.fetchall()
            industry_data_master = self.get_key_value_format_mapping_falcon(ds_industries)
            conn_master.close()
        return function_data_master, roles_data_master, industry_data_master

    def check_running_cron(self, is_last_update=False):
        connection, cursor = self.salesforce_connection()
        if is_last_update:
            sql_upate = """Update replication_tables set descr='0',end_time=now() where code='LOGIN_DTL_SALESFORCE_CRON_EXECUTING';"""
            cursor.execute(sql_upate)
            connection.commit()
            connection.close()
        else:
            sql = """select * from replication_tables where code='LOGIN_DTL_SALESFORCE_CRON_EXECUTING';"""
            # print(sql)
            cursor.execute(sql)
            results = cursor.fetchone()
            is_running = False
            if results.get("descr", '0') == '1':
                is_running = True
            else:
                sql_upate = """Update replication_tables set descr='1',start_time=now() where code='LOGIN_DTL_SALESFORCE_CRON_EXECUTING';"""
                cursor.execute(sql_upate)
                connection.commit()
            connection.close()
            return is_running

    def get_lastUclmid(self, is_end=False):
        conn, curr = self.salesforce_connection()
        if is_end:
            sql_upate = """Update replication_tables set end_time=now() where code='LOGIN_DTL_SALESFORCE';"""
            curr.execute(sql_upate)
            conn.commit()
            conn.close()
        else:
            last_id = 0
            lastUclm_id_og = 2
            sql = """select * from replication_tables where code='LOGIN_DTL_SALESFORCE';"""
            print(sql)
            curr.execute(sql)
            results = curr.fetchone()
            is_running = False
            if results.get("descr", None):
                if int(results.get("descr", None)) >= 0:
                    last_id = int(results.get("descr", 0))
                    lastUclm_id_og = int(results.get("descr", 0)) + self.count
                else:
                    lastUclm_id_og = last_id + self.count
            else:
                lastUclm_id_og = last_id + self.count
            sql_upate = """Update replication_tables set start_time=now() where code='LOGIN_DTL_SALESFORCE';"""
            curr.execute(sql_upate)
            conn.commit()
            conn.close()
            return last_id, lastUclm_id_og

    def update_replication_table(self, lastuclmid):
        conn, curr = self.salesforce_connection()
        sql_upate = """Update replication_tables set descr=%s where code='LOGIN_DTL_SALESFORCE';""" % (str(lastuclmid))
        # print sql_upate
        curr.execute(sql_upate)
        conn.commit()
        conn.close()

    def update_salesforce_api_response(self, total_data):
        try:
            conn, curr = self.salesforce_connection()
            sql_upate = """insert into salesforce_api_response (`uid`,`input_json`,`output`) values(%s,"%s",%s);"""
            # print sql_upate
            curr.executemany(sql_upate, total_data)
            conn.commit()
            conn.close()
        except Exception as ex:
            print ('Error in salesforce_api_response insert query ::: ', ex, sql_upate)
            pass

    def falcon_query(self, query):
        falconn, falcurr = self.falcon_connection()
        falcurr.execute(query)
        results = falcurr.fetchall()
        falconn.close()
        return results

    def rio_query(self, query):
        rioconn, riocurr = self.rio_connection()
        riocurr.execute(query)
        results = riocurr.fetchall()
        rioconn.close()
        return results

    def get_experience(self, maxyear, minmonth):
        exp_str = []
        if maxyear:
            if int(maxyear) > 0:
                da1 = str(maxyear) + " years"
                exp_str.append(da1)
        if minmonth:
            if int(minmonth) > 0:
                da2 = str(minmonth) + " months"
                exp_str.append(da2)
        return ', '.join(exp_str)

    def get_fun_role_ind_text(self, uuidtext_dict, master_data):
        text = []
        # print uuidtext_dict
        for dictuuid in uuidtext_dict:
            if dictuuid.get("uuid", None):
                funname = master_data.get(str(dictuuid.get("uuid", None)), '')
                if funname:
                    text.append(funname)
            else:
                if dictuuid.get("text", None):
                    text.append(dictuuid.get("text", ''))

        # print ' | '.join(text), text
        return ' | '.join(text)

    def get_function_role_industry(self, dictlist, funmaster, rolemaster, industmaster):
        for dict_data in dictlist:
            cur_location = self.job_locationuuid_name_master.get(str(dict_data.get("cur_location", "")), "")
            functiondata = funmaster.get(dict_data.get("job_pre_id", "0"), [])
            category = self.get_fun_role_ind_text(functiondata, self.category_data_master)
            roledata = rolemaster.get(dict_data.get("job_pre_id", "0"), [])
            role = self.get_fun_role_ind_text(roledata, self.category_data_master)
            inddata = industmaster.get(dict_data.get("job_pre_id", "0"), [])
            industrys = self.get_fun_role_ind_text(inddata, self.industry_data_master)
            dict_data.update(
                {"categories": category, "roles": role, "industry": industrys, "cur_location": cur_location})
        return dictlist

    def get_email_mobile_text(self, namets, is_email=False, is_phone=False):
        dict = {}
        if namets:
            if is_email:
                dict["email"] = ""
                dict["alt_email"] = ""
                for namet in namets:
                    if namet.get("is_primary") == 1:
                        dict["email"] = namet.get("main_data", "")
                    else:
                        dict["alt_email"] = namet.get("main_data", "")
                if dict["email"] == "" and dict["alt_email"] != "":
                    dict["email"] = dict["alt_email"]
            if is_phone:
                dict["primary_phone"] = ""
                dict["mobile"] = ""
                # for namet in namets:
                if namets.get("is_primary") == 1:
                    dict["primary_phone"] = namets.get("main_data", "")
                else:
                    dict["mobile"] = namets.get("main_data", "")
        return dict

    def get_all_mobile_email(self, rio_dictlist, emaildatamaster, phonedatamaster):
        for riodata in rio_dictlist:
            rawdata1 = emaildatamaster.get(riodata.get("rioid", ''), None)
            if rawdata1:
                rawdata2 = self.get_email_mobile_text(rawdata1, is_email=True)
                riodata.update(rawdata2)
            rawdataph = phonedatamaster.get(riodata.get("rioid", ''), None)
            if rawdataph:
                rawdata3 = self.get_email_mobile_text(rawdataph, is_phone=True)
                riodata.update(rawdata3)
            # riodata.update({"nationality": self.job_country_name_master.get(riodata.get("nationality", ""), "")})
        return rio_dictlist

    def rio_data_main(self, dictdata_list):
        mj = time.time()
        uids = [str(mh.get("rio_uuid", "")) for mh in dictdata_list]
        # uIds = ','.join(uids)
        con_rio, cur_rio = self.rio_connection()
        rio_query = "select uuid as uuid, id as rioid,first_name,last_name,full_name,nationality as nation from users where uuid in ('" + "','".join(
            uids) + "');"
        # print ('Rio Query : : ', rio_query)
        cur_rio.execute(rio_query)
        rawrio_data_list = cur_rio.fetchall()
        con_rio.close()
        Userids = [m.get("rioid", None) for m in rawrio_data_list if m.get("rioid", None)]
        if len(Userids) > 0:
            emailmaster, phone_master = self.load_rio_data(Userids)
            for riodata in dictdata_list:
                nationality = ""
                for rawrio_data in rawrio_data_list:
                    nationality=self.job_country_name_master.get(rawrio_data.get("nation", ""), "")
                    if riodata.get("rio_uuid", '') == rawrio_data.get("uuid", ""):
                        alldatario = self.get_all_mobile_email([rawrio_data], emailmaster, phone_master)
                        if len(alldatario) == 1:
                            alldatario[0].update({"nationality": nationality})
                            riodata.update(alldatario[0])

        return dictdata_list

    def get_current_uids(self,uids_list):
        conn,curr = self.salesforce_connection()
        query = "select uid from salesforce_api_response where date(updated)=curdate();"
        curr.execute(query)
        datas = curr.fetchall()
        curr.close()
        conn.close()
        valdata = [str(j.get("uid", '')) for j in datas if j.get("uid", None)]
        uids =[]
        for ud in uids_list:
            if ud not in valdata:
                uids.append(ud)
        return uids


    def getSeekerDetails(self, UIDS):
        seekerHashDetails = {}
        uIds = [str(i) for i in list(UIDS.keys())]
        if "40290447" in uIds:
            try:
                uIds.remove("40290447")
            except Exception as ex:
                print (ex)
                pass
        uIds = self.get_current_uids(uIds)
        print ('UID List:::: ', len(list(set(uIds))))
        if len(uIds) > 0:
            uIdString = ','.join(uIds)
            getUserIdQuery = "select usr.uuid as rio_uuid,uprof_emp.salary_absolute_value as salary,uprof_emp.salary_currency_code as currency_id,uprof_emp.salary_mode as salary_mode, usr.id as id,CASE When usrpr.resume_file_path is not null Then '1' else '0' END as resume_flags,usrpr.country as country,usrpr.resume_filename as filename,usrpr.site_context as site,usrpr.experience_months,usrpr.experience_years,usj_pre.id as job_pre_id,usrpr.current_location_uuid as cur_location,FROM_UNIXTIME(usr.created_at/1000,'%Y-%m-%d %h:%i:%s') as regdate,FROM_UNIXTIME(usr.updated_at/1000,'%Y-%m-%d %h:%i:%s') as updated,usr.permanent_address as address, usrpr.id as profile_id from users as usr left join user_profiles as usrpr on usr.id = usrpr.user_id left join user_job_preferences as usj_pre on usrpr.id= usj_pre.profile_id left join user_profile_employments as uprof_emp on usrpr.id= uprof_emp.profile_id where usrpr.enabled=1 and usr.id in ({}) order by uprof_emp.start_date asc;".format(
                uIdString)  # usr.id,uprof_emp.company_text,,uprof_emp.updated_at #uprof_emp.deleted!=1 and
            # print ("Falcon query:: ", getUserIdQuery)
            userIdHashRef_raw = self.falcon_query(getUserIdQuery)
            print ('Falcon Length : ', len(userIdHashRef_raw))
            if len(userIdHashRef_raw) > 0:
                jobprefids = [str(mh.get("job_pre_id", None)) for mh in userIdHashRef_raw if mh.get("job_pre_id", None)]
                fundata, rolesdata, indusdata = self.load_falcon_role_industry_function_data(jobprefids)
                userIdHashRef_rio = self.get_function_role_industry(userIdHashRef_raw, fundata, rolesdata, indusdata)
                userIdHashRef = self.rio_data_main(userIdHashRef_rio)
                if len(userIdHashRef) > 0:
                    for seeker in userIdHashRef:
                        seekerHashDetails[seeker.get("id")] = seeker
                        seekerHashDetails[seeker.get("id")]["CUR_LOC_NAME"] = seeker.get("cur_location", "")
                        seekerHashDetails[seeker.get("id")]["INDUSTRY_NAME"] = seeker.get("industry", "")
                        seekerHashDetails[seeker.get("id")]["CATEGORY_NAME"] = seeker.get("categories", "")
                        seekerHashDetails[seeker.get("id")]["ROLE_NAME"] = seeker.get("roles", "")
                        seekerHashDetails[seeker.get("id")]["EXPERIENCE"] = self.get_experience(
                            seeker.get("experience_years", None), seeker.get("experience_months", None))
                        seekerHashDetails[seeker.get("id")]["COUNTRY_NAME"] = seeker.get("country", "")
                        sal_mode = ''
                        sal_val = ''
                        currenyid = ''
                        if seeker.get("currency_id", None):
                            currenyid = str(seeker.get("currency_id", ''))
                            if currenyid:
                                currenyid = self.currencyname_name_master.get(str(currenyid).lower().strip(), currenyid)
                        if seeker.get("salary_mode", None):
                            sal_mode = self.salary_data_master.get(str(seeker.get("salary_mode", '')), '')
                        if seeker.get("salary", None):
                            if int(seeker.get("salary", 0)) > 0:
                                sal_val = str(seeker.get("salary", ''))
                        seekerHashDetails[seeker.get("id")]["SAL_DTL"] = str(
                            currenyid + " " + str(sal_val) + " " + sal_mode).strip()
        return seekerHashDetails

    def request_output(self, url, data, header, uidseeker, seeker):
        temp1 = []
        try:
            response = requests.post(url, data=data, headers=header, timeout=180)
            # if response.status_code==200:
            output = str(response.json())
            temp1.append(uidseeker)
            temp1.append(seeker)
            temp1.append(output)
        except Exception as e:
            pass
        return temp1

    def sendDataToSalesforce(self, seekerHash, lastUclmId):
        print ('Length of seekerHash', len(list(seekerHash.keys())))
        # res = requests.post(self.url, data=self.data, headers=self.headers)
        res = self.res
        print 'status code :: ', res.status_code
        if res.status_code == 200:
            outputData = res.json()
            Totalrecords = []
            for seekerDetail in seekerHash:
                temp = []
                seekerdata = {}
                seekerdata["seekerid"] = seekerDetail
                jsondata = seekerHash.get(seekerDetail)
                seekerdata["fname"] = str(jsondata.get("first_name", '')).encode("utf8") if jsondata.get("first_name",
                                                                                                         '') else ''
                seekerdata["lname"] = str(jsondata.get("last_name", '')).encode("utf8") if jsondata.get("last_name",
                                                                                                        '') else ''
                if not seekerdata["fname"]:
                    seekerdata["fname"] = str(jsondata.get("full_name", '')).encode("utf8") if jsondata.get("full_name",
                                                                                                            '') else ''
                seekerdata["Email"] = str(jsondata.get("email", '')).encode("utf8") if jsondata.get("email", '') else ''
                seekerdata["alt_email"] = str(jsondata.get("alt_email", '')).encode("utf8") if jsondata.get("alt_email",
                                                                                                            '') else ''
                seekerdata["primary_phone"] = str(jsondata.get("primary_phone", '')) if jsondata.get("primary_phone",
                                                                                                     '') else ''
                seekerdata["mobile"] = str(jsondata.get("mobile", '')) if jsondata.get("mobile",
                                                                                                     '') else ''
                if jsondata.get("CUR_LOC_NAME", None):
                    seekerdata["CurrentLocation"] = jsondata.get("CUR_LOC_NAME", '')
                else:
                    seekerdata["CurrentLocation"] = ""

                if jsondata.get("EXPERIENCE", None):
                    seekerdata["Experience"] = jsondata.get("EXPERIENCE", '')
                else:
                    seekerdata["Experience"] = ""
                if jsondata.get("CATEGORY_NAME", None):
                    seekerdata["Categories"] = jsondata.get("CATEGORY_NAME", '')
                else:
                    seekerdata["Categories"] = ""
                if jsondata.get("INDUSTRY_NAME", None):
                    seekerdata["Industry"] = jsondata.get("INDUSTRY_NAME", '')
                else:
                    seekerdata["Industry"] = ""
                if jsondata.get("ROLE_NAME", None):
                    seekerdata["Roles"] = jsondata.get("ROLE_NAME", '')
                else:
                    seekerdata["Roles"] = ""
                seekerdata["ResId"] = jsondata.get("profile_id", "")
                if jsondata.get("SAL_DTL", None):
                    seekerdata["Salary"] = jsondata.get("SAL_DTL", "")
                else:
                    seekerdata["Salary"] = ""

                if jsondata.get("regdate", None):
                    seekerdata["Registration"] = str(jsondata.get("regdate", "")).replace(" ", "T") + "z"
                else:
                    seekerdata["Registration"] = ""
                if jsondata.get("updated", None):
                    seekerdata["Updatedate"] = str(jsondata.get("updated", "")).replace(" ", "T") + "z"
                else:
                    seekerdata["Updatedate"] = ""
                if jsondata.get("nationality", None):
                    seekerdata["Nationality"] = str(jsondata.get("nationality", ""))
                else:
                    seekerdata["Nationality"] = ""
                if jsondata.get("address", None):
                    seekerdata["Address"] = str(jsondata.get("address", "")).replace('"', "'").replace("'",
                                                                                                       "''").encode(
                        "utf8")
                else:
                    seekerdata["Address"] = ""
                if jsondata.get("COUNTRY_NAME", ""):
                    seekerdata["Country"] = self.country_data_master.get(jsondata.get("COUNTRY_NAME", "").lower(), '')
                else:
                    seekerdata["Country"] =""
                if jsondata.get("site", None):
                    seekerdata["Source_File"] = str(jsondata.get("site", ""))
                else:
                    seekerdata["Source_File"] = ""
                seekerdata["Resume"] = "NA"
                seekerdata["attchname"] = ""
                if jsondata.get("filename", None):
                    seekerdata["Resume"] = "A"
                    fname = str(jsondata.get("filename", ""))
                    if fname.startswith("'"):
                        fname = fname[1:]
                    if fname.endswith("'"):
                        fname = fname[:-1]
                    seekerdata["attchname"] = fname.encode("utf8").replace("'", "\'").encode("utf8")
                self.headers["Content-Type"] = "application/json"
                self.headers["Authorization"] = "Bearer " + outputData.get("access_token", "")
                url = outputData.get("instance_url", "") + "/services/apexrest/B2Cintegration/"
                jsonCandStr = json.dumps(seekerdata)
                # print '+++++++>', seekerdata
                temp.append(url)
                temp.append(jsonCandStr)
                temp.append(self.headers)
                uid_seeker = seekerdata.get("seekerid")
                temp.append(uid_seeker)
                temp.append(str(seekerdata))
                Totalrecords.append(temp)
            Total_data = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
                future_to_url = {
                    executor.submit(self.request_output, url_jb[0], url_jb[1], url_jb[2], url_jb[3], url_jb[4]): url_jb
                    for url_jb in
                    Totalrecords[:]}
                for future in concurrent.futures.as_completed(future_to_url):
                    url2 = future_to_url[future]
                    try:
                        data = future.result()
                        if len(data) > 0:
                            Total_data.append(tuple(data))
                    except Exception as exc:
                        print '=====>', exc, url2
                        pass
            print ('Instred Rec Final length:', len(Total_data))
            if len(Total_data) > 0:
                try:
                    self.update_salesforce_api_response(Total_data)
                except Exception as exc_api:
                    print ("Error in insert into salesforce_api_response table : " + str(exc_api))

    def getLastLoginDetails(self, lastUclmId, lastUclmId_org):
        print ('lastUclmId Start', lastUclmId)
        try:
            islastUclmId = lastUclmId
            is_continue = True
            mn = 0
            while lastUclmId <= lastUclmId_org and is_continue:
                ucl_conn, ucl_curr = self.falcon_connection()
                query = "select id,user_id,profile_id,active_at from user_active_data where active_at > %s and active_at < %s order by active_at asc limit 50" % (
                    lastUclmId, lastUclmId_org)
                # query = "select id,user_id,profile_id,active_at from user_active_data where user_id=41175810"
                print (query)
                ucl_curr.execute(query)
                uclmDetails = ucl_curr.fetchall()
                ucl_conn.close()
                print 'UCLMDeatails',len(uclmDetails)
                if len(uclmDetails) > 0:
                    uids = {}
                    for seeker in uclmDetails:
                        lastUclmId = seeker.get("active_at")
                        uids[seeker.get("user_id")] = 1
                    print ('Length of uids in Chuncks : ', len(uids))
                    seekerHashDetails = self.getSeekerDetails(uids)
                    print ('lastUclmId', str(lastUclmId),len(seekerHashDetails))
                    if len(seekerHashDetails)>0:
                       self.update_replication_table(lastUclmId)
                       self.sendDataToSalesforce(seekerHashDetails, lastUclmId)
                    else:
                        self.update_replication_table(lastUclmId)
                else:
                    is_continue = False
                if lastUclmId == islastUclmId:
                    lastUclmId_org += 900000
                    is_continue = True
                    mn += 1
                if mn == 2:
                    is_continue = False
        except Exception as e:
            print ("Error in getLastLoginDetails: ",str(e))
        self.check_running_cron(is_last_update=True)
        self.get_lastUclmid(is_end=True)

    def start_salesforce_data(self):
        is_run = self.check_running_cron()
        # is_run = False
        print ('Starting Script : ', is_run)
        if not is_run:
            lastUclmId, lastUclmId_org = self.get_lastUclmid()
            print ('start', lastUclmId, lastUclmId_org)
            self.getLastLoginDetails(lastUclmId, lastUclmId_org)
            print ("Successfully completed : ", str(datetime.now()))
        else:
            print ("Cron already running")


if __name__ == "__main__":
    obj = SalesForceToPython()
    obj.start_salesforce_data()
