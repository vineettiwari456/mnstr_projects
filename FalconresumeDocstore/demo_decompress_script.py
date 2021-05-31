import mysql.connector
import zlib

conf_dolphin = {"user": "xxxxxxx", "password": "xxxxxxxx", "host": "xxxxxxxx", "database": "dolphin"}

def dolphin_fetch_connection():
    connection = mysql.connector.connect(user=conf_dolphin['user'],
                                         password=conf_dolphin['password'],
                                         host=conf_dolphin['host'],
                                         database=conf_dolphin['database'])
    cursor = connection.cursor(dictionary=True)
    return connection, cursor

fetch_query = """select resume_text from user_active_resume_text_2 limit 1;"""
conn, curr = dolphin_fetch_connection()
curr.execute(fetch_query)
dolphin_records = curr.fetchall()
for data in dolphin_records:
    outdata = zlib.decompress(data.get("resume_text").encode("latin1")).decode('utf-8')
    print(outdata)
    f_open=open("resume.txt",'w')
    f_open.write(str(outdata))
    f_open.close()