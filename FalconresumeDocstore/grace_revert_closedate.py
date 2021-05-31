# -*-coding:utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

import requests
import time
import mysql.connector
import concurrent.futures
import smtplib,os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from datetime import timedelta
from base64 import b64decode

class ExtractGraceableJobs:
    def __init__(self):
        # self.db1sl_conf = {"user": "db", "password": "bazookadb", "host": "10.216.204.20",
        #                    "database": "bazooka"}
        self.db1sl_conf = {"user": "db", "password": "YWkzaEVnZkF0OWFvamRl", "host": "db1m",
                           "database": "bazooka"}


    def db1sl_connection(self):
        connection_db1sl = mysql.connector.connect(user=self.db1sl_conf['user'],
                                                   password=b64decode(self.db1sl_conf['password']),
                                                   host=self.db1sl_conf['host'],
                                                   database=self.db1sl_conf['database'])
        cursor_db1sl = connection_db1sl.cursor(dictionary=True)
        return connection_db1sl, cursor_db1sl
    # def db1sl_connection(self):
    #     connection_db1sl = mysql.connector.connect(user=self.db1sl_conf['user'],
    #                                                password=self.db1sl_conf['password'],
    #                                                host=self.db1sl_conf['host'],
    #                                                database=self.db1sl_conf['database'])
    #     cursor_db1sl = connection_db1sl.cursor(dictionary=True)
    #     return connection_db1sl, cursor_db1sl


    def get_graceable_jobs(self):
        conn,curr = self.db1sl_connection()
        # query = "select gp.folderid,gp.close,gp.gracedate, date(DATE_SUB(now(), INTERVAL (datediff(now(),gp.close))-120 DAY)) as closedate,mod(aj.company,20)+1 as jobfolder  from gracePosting as gp inner join activejobs as aj on gp.folderid=aj.folderid where (date(date) between '2020-04-27' and '2020-04-28') and (datediff(gp.gracedate,gp.close)) > 121;"
        query = "select gp.folderid,gp.close,gp.gracedate, date(DATE_SUB(now(), INTERVAL (datediff(now(),gp.close))-120 DAY)) as closedate,mod(aj.company,20)+1 as jobfolder  from gracePosting as gp inner join activejobs as aj on gp.folderid=aj.folderid where (date(date) between '2020-04-27' and '2020-04-28');"
        print query
        curr.execute(query)
        records = curr.fetchall()
        conn.close()
        print len(records)
        if len(records)>0:
            Total_data = []
            for md in records:
                try:
                    print (md)
                    folderid=md.get("folderid")
                    close = md.get("closedate")
                    mainclosedate = md.get("close")
                    jobfolderindex = md.get("jobfolder")
                    connection, cursor = self.db1sl_connection()
                    update_query="update jobs_%s set close='%s' where folderid=%s;"%(jobfolderindex,close, folderid)
                    cursor.execute(update_query)
                    update_activejobs = "update activejobs set close='%s' where folderid=%s;"%(close, folderid)
                    cursor.execute(update_activejobs)
                    update_map_activejobs = "update map_activejobs set close_date='%s' where folderid=%s;"%(close, folderid)
                    cursor.execute(update_map_activejobs)
                    update_gracePosting = "update gracePosting set gracedate='%s' where folderid=%s and close='%s';"%(close, folderid,mainclosedate)
                    cursor.execute(update_gracePosting)
                    connection.commit()
                    connection.close()
                except Exception as e:
                    print ("Error in insert",str(e))
                    pass
        #         break


if __name__=="__main__":
    obj = ExtractGraceableJobs()
    obj.get_graceable_jobs()