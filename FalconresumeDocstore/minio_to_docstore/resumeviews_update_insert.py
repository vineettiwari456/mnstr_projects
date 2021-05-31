import os, json
import mysql.connector
import requests
from decouple import config
import ast


class UpdateResumeView:
    def __init__(self):
        self.conf_db3 = {"user": config("DB3_USER"), "password": config("DB3_PASS"), "host": config("DB3_HOST"),
                         "database": config("DB3_DATABASE")}

    def db3_connection(self):
        connection_db2 = mysql.connector.connect(user=self.conf_db3['user'],
                                                 password=self.conf_db3['password'],
                                                 host=self.conf_db3['host'],
                                                 database=self.conf_db3['database'])
        cursor_db2 = connection_db2.cursor(dictionary=True)
        return connection_db2, cursor_db2

    def insert_db3_db(self, records):
        insert_query = """insert into resumeviewed (`resid`,`subuid`,`viewdate`,`tobedeleted`,`channel_id`) values(%s,%s,%s,%s,%s);"""
        conn, curr = self.db3_connection()
        curr.executemany(insert_query, records)
        conn.commit()
        conn.close()

    def chunks(self,lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def get_resume_id_with_channel(self):
        Total_data = self.get_resume_records()
        chunk_data = self.chunks(Total_data, 1000)
        for record in chunk_data:
            print(len(record))
            # self.insert_db3_db(record)


    def get_resume_records(self):
        f = open("resumeviews_insert_records", 'r')
        unique_profiles = ast.literal_eval(f.read())
        f.close()
        return unique_profiles


if __name__ == "__main__":
    obj = UpdateResumeView()
    obj.get_resume_id_with_channel()