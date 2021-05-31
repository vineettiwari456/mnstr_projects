# -*-coding:utf-8 -*-

import requests
import mysql.connector
from mysql.connector import pooling
import os
from urllib.parse import urljoin
import concurrent.futures
import urllib.request
import time
from urllib.parse import urljoin
import sys, subprocess, filetype
import docx2txt
from minio import Minio
from pdf_text import PdfParser
from striprtf.striprtf import rtf_to_text
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)


class ResumeDownload:
    def __init__(self, arg_value, high_limit):
        self.arg_value = arg_value
        self.highest_limit = high_limit
        self.obj_pdf = PdfParser()
        self.bucket_name = "vineettest-falcon"
        self.chunk = 10000
        if high_limit:
            if high_limit < self.chunk:
                self.chunk = high_limit
        self.conf_falcom = {"user": "vtiwari", "password": "vtiwari@123#", "host": "10.216.204.150",
                            "database": "falcon"}
        # self.conf = {"user": "db", "password": "bazookadb", "host": "10.216.204.7", "database": "bazooka"}
        self.conf_db2sl = {"user": "db", "password": "bazookadb", "host": "10.216.204.38", "database": "bazooka"}
        self.dirs = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resumes_download")
        filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Log")
        if not os.path.exists(filepath):
            os.mkdir(filepath)
        if not os.path.exists(self.dirs):
            os.mkdir(self.dirs)
        self.minioClient = Minio('minio.monsterlocal.com',
                                 access_key='minio',
                                 secret_key='minio123',
                                 secure=False)
        buckets = self.minioClient.list_buckets()
        if self.bucket_name.lower() not in [bucket.name for bucket in buckets]:
            self.minioClient.make_bucket(self.bucket_name)
        self.connection_falcon1 = mysql.connector.pooling.MySQLConnectionPool(pool_name="falcon",
                                                                              pool_size=32,
                                                                              pool_reset_session=True,
                                                                              host=self.conf_falcom['host'],
                                                                              database=self.conf_falcom['database'],
                                                                              user=self.conf_falcom['user'],
                                                                              password=self.conf_falcom['password'], )
        self.connection_db2sl2 = mysql.connector.pooling.MySQLConnectionPool(pool_name="db2sl",
                                                                             pool_size=32,
                                                                             pool_reset_session=True,
                                                                             host=self.conf_db2sl['host'],
                                                                             database=self.conf_db2sl['database'],
                                                                             user=self.conf_db2sl['user'],
                                                                             password=self.conf_db2sl['password'], )
        # if not os.path.exists(self.dirs):
        #     os.mkdir(self.dirs)
        filename = os.path.join(filepath, str(
            self.arg_value) + '_log_file.txt')
        # filename = "log_file.txt"
        self.f = open(filename, "w")

    def falcon_connection(self):

        connection_db2sl = mysql.connector.connect(user=self.conf_falcom['user'], password=self.conf_falcom['password'],
                                                   host=self.conf_falcom['host'],
                                                   database=self.conf_falcom['database'])
        cursor_db2sl = connection_db2sl.cursor(dictionary=True)
        return connection_db2sl, cursor_db2sl

    def download_resume(self, url_link, file_name, id):
        try:
            urllib.request.urlretrieve(url_link, os.path.join(self.dirs, file_name))
        except Exception as e:
            self.f.write("Error in Resume download : " + str(str(e) + "  " + str(url_link) + "  " + str(id)))
            self.f.write("\n")
    def get_cmd_output(self, *args, **kwargs):
        """Returns text output of a command."""
        encoding = kwargs.get("encoding", "utf-8")
        # logger.debug("get_cmd_output(): args = {}".format(repr(args)))
        p = subprocess.Popen(args, stdout=subprocess.PIPE)
        stdout, stderr = p.communicate()
        return stdout.decode(encoding, errors='ignore')

    def convert_doc_to_text(self, filename=None, blob=None):
        if filename:
            try:
                return self.get_cmd_output(
                    'antiword',  # IN CASE OF FAILURE: sudo apt-get install antiword
                    filename)
            except:
                return ''
    def convert_pdf_to_txt(self, filename=None, blob=None):
        """Pass either a filename or a binary object."""
        # External command method:
        if filename:
            return self.get_cmd_output(
                'pdftotext',  # Core part of Linux?
                filename,
                '-')
    def convert_rtf_to_text(self, filepath):
        try:
            f = open(filepath, 'r')
            rtfdata = rtf_to_text(f.read())
            f.close()
            return rtfdata
        except:
            return ''

    def get_docx_doc_pdf_text(self, filepath, destintaion_path):
        file_name = os.path.basename(filepath)
        filename, file_extension = os.path.splitext(file_name)
        fdata = ""
        is_text = False
        if file_extension.lower() == ".docx":
            try:
                fdata = docx2txt.process(filepath)
            except:
                pass
        if file_extension.lower() == ".doc":
            try:
                kind = filetype.guess(filepath)
                if kind:
                    if 'rtf' in str(kind.mime).lower():
                        fdata = self.convert_rtf_to_text(filepath)
                    else:
                        fdata = self.convert_doc_to_text(filepath)
                else:
                    fdata = self.convert_doc_to_text(filepath)
            except:
                fdata = self.convert_doc_to_text(filepath)

        if file_extension.lower() == ".rtf":
            fdata = self.convert_rtf_to_text(filepath)
        if file_extension.lower() == ".pdf":
            fdata = self.obj_pdf.pdfparser(filepath)
            # fdata = self.convert_pdf_to_txt(filepath)
        if fdata:
            fd = open(destintaion_path,'w',encoding="utf-8")
            fd.write(fdata)
            fd.close()
            is_text= True
        return fdata,is_text

    def upload_resume_to_minio(self, sourcepath, destinationpath):
        is_success = False
        try:
            self.minioClient.fput_object(self.bucket_name, destinationpath, sourcepath)
            is_success = True
        except Exception as minio_e:
            self.f.write("Error in Minio Insert : " + str(
                str(minio_e) + "  " + str(destinationpath)))
            self.f.write("\n")
            pass
        return is_success

    def update_in_falcon(self, id, minio_url):
        is_insert = False
        try:
            connection_write_f = self.connection_falcon1.get_connection()
            if connection_write_f.is_connected():
                cursor_write_f = connection_write_f.cursor(dictionary=True)
                ssourcesql = 'UPDATE user_profiles SET resume_file_path=%s where id=%s'
                ssourceval = tuple([minio_url, id])
                cursor_write_f.execute(ssourcesql, ssourceval)
                connection_write_f.commit()
                is_insert = True
                if (connection_write_f.is_connected()):
                    cursor_write_f.close()
                    connection_write_f.close()
            self.f.write("Updated falcon id : " + str(id))
            self.f.write("\n")

        except Exception as db_e:
            self.f.write("Error in Update resume path in falcon database " + str(
                str(db_e) + "  " + str(id) + "  " + str(minio_url)))
            self.f.write("\n")
            pass
        return is_insert

    def update_in_bazooka(self, id, minio_url):
        is_update = False
        try:
            connection_write_b = self.connection_db2sl2.get_connection()
            if connection_write_b.is_connected():
                cursor_write_b = connection_write_b.cursor(dictionary=True)
                ssourcesql = 'UPDATE image_resume_url SET resume_url=%s where resume_id=%s'
                ssourceval = tuple([minio_url, id])
                cursor_write_b.execute(ssourcesql, ssourceval)
                connection_write_b.commit()
                is_update = True
                if (connection_write_b.is_connected()):
                    cursor_write_b.close()
                    connection_write_b.close()
                self.f.write("Updated in image_resume_path Bazooka resume id : " + str(id))
                self.f.write("\n")
        except Exception as db_e:
            self.f.write("Error in Update resume path in bazooka database " + str(
                str(db_e) + "  " + str(id) + "  " + str(minio_url)))
            self.f.write("\n")
            pass

    def download_doc(self, url_link, uuid, falconuserid, falconprofileid, file_namedb):
        try:
            unixtime = int(time.time() * 1000)
            filename, file_extension = os.path.splitext(file_namedb)
            # print('falconprofileid', falconprofileid, filename, file_extension, url_link)
            file_name = str(filename) + "_" + str(unixtime) + str(file_extension)
            storepath = os.path.join(self.dirs, file_name).replace("\\", "/")
            self.download_resume(url_link, storepath, falconprofileid)
            try:

                if os.path.exists(storepath):
                    if '.txt' not in str(file_extension).lower():
                        file_name_txt = str(filename) + "_" + str(unixtime) + ".txt"
                        storepath_text = os.path.join(self.dirs, file_name_txt).replace("\\", "/")
                        try:
                            filetext, istext= self.get_docx_doc_pdf_text(storepath,storepath_text)
                            if istext:
                                try:
                                    minio_path_text = os.path.join("resume", str(uuid), str(falconuserid), str(falconprofileid),
                                                              file_name_txt).replace(
                                        "\\", "/")
                                    print(minio_path_text)
                                    sucess_text = self.upload_resume_to_minio(storepath_text, minio_path_text)
                                except Exception as minio_text:
                                    self.f.write("Error in Upload resume text to  minio : "+str(str(minio_text) + "  " + str(falconprofileid) + "  " + str(url_link)))
                                    self.f.write("\n")
                        except Exception as err_text:
                            self.f.write("Error in Extract text from resume : " + str(
                                str(err_text) + "  " + str(falconprofileid)))
                            self.f.write("\n")
                            pass
                    minio_path = os.path.join("resume", str(uuid), str(falconuserid), str(falconprofileid),
                                              file_name).replace(
                        "\\", "/")
                    print(minio_path)
                    try:
                        sucess = self.upload_resume_to_minio(storepath, minio_path)
                        print(sucess)
                        # if sucess:
                        #     is_update = self.update_in_falcon(falconprofileid,"/"+minio_path)
                        #     if is_update:
                        #         resume_id = str(url_link.split("/")[-2]).strip()
                        #         self.update_in_bazooka(resume_id,"/"+minio_path)
                    except Exception as err:
                        self.f.write("Error in Upload resume to  minio : "+str(str(err) + "  " + str(falconprofileid) + "  " + str(url_link)))
                        self.f.write("\n")
                        pass
                    # try:
                    #     os.remove(storepath)
                    # except:
                    #     pass
            except Exception as e:
                self.f.write("Error in After Resume download : " + str(
                    str(e) + "  " + str(url_link) + "  " + str(falconprofileid)))
                self.f.write("\n")
        except Exception as exc_file:
            self.f.write("Error in Last : " + str(str(exc_file) + "  " + str(url_link) + "  " + str(falconprofileid)))
            self.f.write("\n")

    def download(self):
        connection, cur = self.falcon_connection()
        if self.highest_limit:
            count = self.highest_limit
        else:
            query_count = "select count(up.id) from user_profiles as up inner join users as usr on up.user_id = usr.id where (mod(up.id,10) = %s) and up.resume_file_path like '%%docstore.monsterindia%%' order by up.id asc;" % (
                self.arg_value)
            print(query_count)
            cur.execute(query_count)
            rawcount = cur.fetchone()
            count = rawcount.get("count")
            connection.close()
        print("Total records : ", count)
        batch_size = self.chunk  # whatever
        for offset in range(0, count, batch_size):
            All_urls = []
            connection1, cursor = self.falcon_connection()
            # print(self.arg_value, batch_size, offset)
            sql = """select up.id as userprofileid,up.resume_file_path as resumepath,up.resume_filename as filename,up.user_id as userid,usr.uuid as uuid from user_profiles as up inner join users as usr on up.user_id = usr.id where  (mod(up.id,10) = %s) and up.resume_file_path like '%%docstore.monsterindia%%' order by up.id desc LIMIT %s OFFSET %s""" % (
                self.arg_value, batch_size, offset)
            # sql = """select up.id as userprofileid,up.resume_file_path as resumepath,up.resume_filename as filename,up.user_id as userid,usr.uuid as uuid from user_profiles as up inner join users as usr on up.user_id = usr.id where (mod(up.id,10) = 1) and up.resume_file_path like '%%docstore.monsterindia%%' order by up.id asc limit 5000"""
            # sql = """select up.id as userprofileid,up.resume_file_path as resumepath,up.resume_filename as filename,up.user_id as userid,usr.uuid as uuid from user_profiles as up inner join users as usr on up.user_id = usr.id where up.resume_file_path like '%%18865403%%' limit 1"""
            print(sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            connection1.close()
            for j_temp in results:
                temp = []
                temp.append(j_temp.get("resumepath", None))
                temp.append(j_temp.get("uuid", None))
                temp.append(j_temp.get("userid", None))
                temp.append(j_temp.get("userprofileid", None))
                temp.append(j_temp.get("filename", None))
                All_urls.append(temp)
            print("length of urls : ", len(All_urls),All_urls)
            #
            with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
                future_to_url = {
                    executor.submit(self.download_doc, url_jb[0], url_jb[1], url_jb[2], url_jb[3], url_jb[4]): url_jb
                    for url_jb in All_urls[:]}
                try:
                    for future in concurrent.futures.as_completed(future_to_url):
                        url2 = future_to_url[future]
                        data = future.result()
                        # print("completed for record: ", url2[2])
                except Exception as exc:
                    self.f.write(str(exc))
                    pass
        self.f.close()


if __name__ == "__main__":
    st = time.time()
    obj = ResumeDownload(0, 2)
    obj.download()
    # value_index = sys.argv[1:]
    # if len(value_index) == 0:
    #     print("Please enter arg with script with range 0-9")
    #     sys.exit()
    # else:
    #     highlimit = None
    #     if len(value_index) == 2:
    #         highlimit = int(value_index[1])
    #     int_val = int(value_index[0])
    #     # print("started for modulo operator : ", int_val)
    #     obj = ResumeDownload(int_val, highlimit)
    #     obj.download()
