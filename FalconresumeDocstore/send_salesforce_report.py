# -*-coding:utf-8 -*-
import os
import mysql.connector
from datetime import datetime
import csv, ast, json
from base64 import b64decode
import sys, time
import requests
from datetime import datetime
from datetime import timedelta
import concurrent.futures
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

reload(sys)
sys.setdefaultencoding('utf8')


class SendSalesForceReport:
    def __init__(self):
        self.conf_salesforce = {"user": "vtiwari", "password": "vtiwari@123##", "host": "10.216.247.113",
                                "database": "salesforce"}
        self.path = os.path.dirname(os.path.abspath(__file__))
        self.filename = os.path.join(self.path, "Daily_salesforce_report.xls")
        self.mainfile = "Daily_salesforce_report.xls"
        self.yesterdays = (datetime.now() - timedelta(days=1)).strftime('%d-%m-%y')
        self.header = ['Hourly Time','Insert Successful','Error','Invalid Country','Unqualified Lead','Update Successful','Product already sold/Seeker not interested']

    def salesforce_connection(self):
        connection_sales = mysql.connector.connect(user=self.conf_salesforce['user'],
                                                   password=self.conf_salesforce['password'],
                                                   host=self.conf_salesforce['host'],
                                                   database=self.conf_salesforce['database'])
        cursor_sales = connection_sales.cursor(dictionary=True)
        return connection_sales, cursor_sales

    def send_mail(self, dictdata):
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%d-%m-%Y')

        fromaddr = "webmaster@monster.co.in"
        toaddr = ["vineetkumar.tiwari@monsterindia.com", "amit.saini@monsterindia.com", "vipul.kumar@monsterindia.com"]
        # toaddr = ["vineetkumar.tiwari@monsterindia.com"]
        msg = MIMEMultipart()
        msg['From'] = fromaddr
        msg['To'] = ", ".join(toaddr)
        # storing the subject
        msg['Subject'] = "Salesforce Log Report"
        signat = """<br>Regards</br>
                            Vineet Kumar Tiwari<br>"""
        # string to store the body of the mail
        body = "<strong> Hi All,<br><br>Please find the Salesforce log Report  for {0} on hourly basis.<strong><br><br>{1}".format(
            yesterday,signat)
        # print body
        # attach the body with the msg instance
        msg.attach(MIMEText(body, 'html'))
        filename = self.filename
        attachment = open(filename, "rb")
        # instance of MIMEBase and named as p
        p = MIMEBase('application', 'octet-stream')
        # To change the payload into encoded form
        p.set_payload((attachment).read())
        # encode into base64
        encoders.encode_base64(p)
        p.add_header('Content-Disposition', "attachment; filename= %s" % self.mainfile)
        # attach the instance 'p' to instance 'msg'
        msg.attach(p)
        # creates SMTP session
        # s = smtplib.SMTP('smtp.gmail.com', 587)
        s = smtplib.SMTP('10.216.240.89')
        # start TLS for security
        s.starttls()
        # s.login(fromaddr, "Password_of_the_sender")
        s.login('smtp-auth@monsterindia.com', 'vS!wA7*z?_wsD=hx')
        # Converts the Multipart msg into a string
        text = msg.as_string()
        # sending the mail
        s.sendmail(fromaddr, toaddr, text)
        # terminating the session
        s.quit()


    def start_salesforce_data(self):
        conn, cursor = self.salesforce_connection()
        # query = "select CASE WHEN output like '%error%' THEN 'Error' ELSE output END AS output_text, extract( hour from updated) as hour ,count(*) as count from salesforce_api_response where date(updated)=curdate() group by extract( HOUR from updated),output_text;"
        query = "select CASE WHEN output like '%error%' THEN 'Error' ELSE output END AS output_text, extract( hour from updated) as hour ,count(*) as count from salesforce_api_response where date(updated)=date(DATE_ADD(NOW(), INTERVAL -1 DAY)) group by extract( HOUR from updated),output_text;"
        cursor.execute(query)
        records = cursor.fetchall()
        conn.close()
        dict = {}
        count = 0
        for j in records:
            count += j.get("count")
            try:
                dict[j.get("hour")].update({j.get("output_text"): j.get("count", 0)})
            except:
                tmp = {}
                dict[j.get("hour")] = {j.get("output_text"): j.get("count", 0)}
        if len(dict)>0:
            with open(self.filename, "wb") as ofs:
                for v in self.header:
                    ofs.write(str(v).encode("utf8"))
                    ofs.write('\t')
                ofs.write('\n')
                ins_count = 0
                err_count = 0
                inv_country_count = 0
                unquali_count = 0
                upscucc_count = 0
                passnint_cout = 0
                for da in dict:
                    temp=[]
                    if len(str(da)) == 1:
                        datf = str(self.yesterdays) + " 0" + str(da).strip()+":00:00"
                    else:
                        datf = str(self.yesterdays) + " " + str(da).strip()+":00:00"
                    rawdict = dict.get(da)
                    ins_count += rawdict.get("Insert Successful", 0)
                    err_count += rawdict.get("Error", 0)
                    inv_country_count += rawdict.get("Invalid Country", 0)
                    unquali_count += rawdict.get("Unqualified Lead", 0)
                    upscucc_count += rawdict.get("Update Successful", 0)
                    passnint_cout += rawdict.get("Product already sold/Seeker not interested", 0)
                    for vd in (datf,str(rawdict.get("Insert Successful", "0")),str(rawdict.get("Error", "0")),str(rawdict.get("Invalid Country", "0")),str(rawdict.get("Unqualified Lead", "0")),str(rawdict.get("Update Successful", "0")),str(rawdict.get("Product already sold/Seeker not interested", "0"))):
                        ofs.write(str(vd).encode("utf8"))
                        ofs.write('\t')
                    ofs.write('\n')
                for vd in ("Total",ins_count,err_count,inv_country_count,unquali_count,upscucc_count,passnint_cout):
                    ofs.write(str(vd).encode("utf8"))
                    ofs.write('\t')
                ofs.write('\n')

            self.send_mail(dict)
            print ("Email send Successfully : ", str(datetime.now()))


if __name__ == "__main__":
    obj = SendSalesForceReport()
    obj.start_salesforce_data()
