# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import requests
import time, os, re, sys, csv
from urllib.parse import urljoin
# from urlparse import urljoin
import concurrent.futures
import codecs
from datetime import datetime
# import urllib.request
from get_pdf_text import MyParser
from get_image_pdf_text import ExtractImageText
from get_ner_spacy import NerSpacy
from parse import Parse
import mysql.connector
import boto3
import warnings

warnings.filterwarnings("ignore")


class PdfCrawler:
    def __init__(self):
        self.headers = {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; WOW64)\
                AppleWebKit/537.36 (KHTML'}
        self.filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads")
        if not os.path.exists(self.filepath):
            os.mkdir(self.filepath)
        self.obj = ExtractImageText()
        self.obj_parse = Parse()
        self.ner_obj = NerSpacy()
        self.urls = []
        self.conf = {"user": "root", "password": "Password@123", "host": "127.0.0.1", "database": "bazooka"}
        self.depth = 2
        self.bucket_name = "lunge-data-pdfs"
        AWS_ACCESS_KEY_ID = 'AKIAQDTI4BOM45WFPVP4'
        AWS_SECRET_ACCESS_KEY = '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'
        self.s3_connection = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID,
                                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        self.load_csv()
        self.conceptdict = {}
        self.load_master_data()

    def load_csv(self):
        csv_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "urls_list.csv")
        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            line_count = 0
            for row in csv_reader:
                if "Urls" not in row[0]:
                    self.urls.append(str(row[0]).strip())

    def get_key_value_format_mapping(self, total_data):
        result = {}
        for da in total_data:
            result[da.get("name").strip()] = da.get("concept_id")
        return result

    def load_master_data(self):
        conn_read, cursor_read = self.database_connection()
        query = "select concept_id,name from conceptdictionaries;"
        cursor_read.execute(query)
        records = cursor_read.fetchall()
        conn_read.close()
        self.conceptdict = self.get_key_value_format_mapping(records)

    def upload_pdf_into_s3(self, pdf_path, file_name):
        is_upload = False
        try:
            # s3_connection = self.session.resource('s3')
            # s3_connection.meta.client.upload_file(pdf_path, self.bucket_name, file_name)
            # s3_connection = self.session.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID,
            #                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
            self.s3_connection.upload_file(Bucket=self.bucket_name, Filename=pdf_path, Key=file_name)
            is_upload = True
        except Exception as e:
            print(e)
            pass
        return is_upload

    def database_connection(self):
        connection = mysql.connector.connect(user=self.conf['user'], password=self.conf['password'],
                                             host=self.conf['host'],
                                             database=self.conf['database'])
        cursor = connection.cursor(dictionary=True)
        return connection, cursor

    def check_pdf_link(self, link):
        pdf_link = None
        other_link = None
        if 'pdf' in link.lower():
            try:
                cc = 0
                while cc < 2:
                    cc += 1
                    try:
                        session = requests.session()
                        response = session.get(str(link), headers=self.headers, timeout=60)
                        # print('+++++++', response.status_code, link)
                        if response.status_code == 200:
                            break
                    except Exception as e:
                        time.sleep(1)
                        pass
                if response.status_code == 200:
                    if response.headers.get("Content-Type") == "application/pdf" or response.headers.get(
                            "Content-Type") == "file/download":
                        # all_pdf.append(link)
                        pdf_link = link
                        # download_pdf(foldername, pd, job_type)
                    # if "text/html" in response.headers.get("Content-Type"):
                    #     other_link = link
            except Exception as e:
                # print ('inside******>', link, e)
                pass
        else:
            other_link = link
        return pdf_link, other_link

    def next_page_link(self, next_ul):
        all_link = []
        try:
            cc = 0
            while cc < 2:
                cc += 1
                try:
                    session = requests.session()
                    response = session.get(str(next_ul), headers=self.headers, timeout=60)
                    # print('+++++++', response.status_code, next_ul)
                    if response.status_code == 200:
                        break
                except Exception as e:
                    time.sleep(1)
                    pass
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'lxml')
                raw_link = [li.get("href") for li in soup.find_all('a') if li.get("href")]
                all_link = self.remove_invalid_url(raw_link, next_ul)
        except:
            pass
        return all_link

    def get_pdf_links(self, alllinks, depth):
        All_pdf = []
        other_links = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            future_to_url = {
                executor.submit(self.check_pdf_link, urllink): urllink for urllink in list(set(alllinks))[:]}
            for future in concurrent.futures.as_completed(future_to_url):
                future_to_url[future]
                try:
                    pdf, nextlink = future.result()
                    if pdf:
                        All_pdf.append(pdf)
                    if nextlink:
                        if self.depth - depth == 1:
                            other_links.append(nextlink)
                        else:
                            nextlink_urls = self.next_page_link(nextlink)
                            other_links.extend(nextlink_urls)
                        # other_links.append(nextlink)
                except Exception as exc:
                    print('esddsfdsdfdf>>>>', exc)
                    pass
        return list(set(All_pdf)), list(set(other_links))

    def remove_invalid_url(self, all_raw_urls, url):
        all_link = []
        for link in all_raw_urls:
            if "mailto" not in link.lower() and "javascript" not in link.lower() and len(link) > 6:
                if str(link).lower().startswith("http"):
                    all_link.append(link)
                else:
                    new_link = urljoin(url, link)
                    all_link.append(new_link)
        return list(set(all_link))

    def download_pdf(self, pdf_url, sourceurl):
        temp = []
        try:
            lastname = str(pdf_url).split("/")[-1]
            pdf_name = ''.join(re.sub(r'[^\x00-\x7F]+', '', lastname).split())
            pdf_path = os.path.join(self.filepath, pdf_name)
            cc = 0
            while cc < 5:
                cc += 1
                try:
                    session = requests.session()
                    response = session.get(str(pdf_url), verify=False, headers=self.headers, timeout=400, stream=True)
                    # print('Download+++++++', response.status_code)
                    if response.status_code == 200:
                        break
                except Exception as e:
                    time.sleep(1)
                    pass
            if response.status_code == 200:
                with open(pdf_path, 'wb') as fd:
                    fd.write(response.content)
                time.sleep(2)
                p_doc = MyParser(pdf_path)
                doc_text = ' '.join([k.strip() for k in p_doc.records if k])
                # if not doc_text:
                #     # if doc_text == "" or doc_text == "":
                #     #     doc_text = self.extract_pdf_image_text(pdf_path)
                #     if doc_text.startswith("") or doc_text == "":
                #         doc_text = self.obj.next_image_pdf_data(pdf_path)

                if doc_text != "":
                    is_upload = self.upload_pdf_into_s3(pdf_path, pdf_name)
                    if is_upload:
                        doc_texts = self.obj_parse.parse(doc_text)
                        nerkeywods = self.ner_obj.extract_ner(doc_texts)
                        nerkeywods_weights = self.ner_obj.extract_weight(doc_texts)
                        s3_file_path = "https://s3-us-west-2.amazonaws.com/" + self.bucket_name + pdf_name
                        temp.append(pdf_url)
                        temp.append(s3_file_path)
                        temp.append(pdf_name)
                        temp.append(nerkeywods)
                        temp.append(nerkeywods_weights)
                        temp.append(pdf_path)
                        temp.append(sourceurl)

                else:
                    print("Blank=====>", pdf_url, doc_text)
        except Exception as e:
            print("+++++++++error ::: ", pdf_url, e)
        return temp

    def check_exist(self, sourceurl):
        conn_read, cursor_read = self.database_connection()
        query = "select source_id, pdf_count from dataSources where source_url='%s';" % (sourceurl.strip())
        cursor_read.execute(query)
        pdf_records = cursor_read.fetchall()
        conn_read.close()
        return pdf_records

    def insert_into_database(self, records, s_url, source_ids, pdf_count):
        now = datetime.now()
        formatted_date = now.strftime('%Y-%m-%d')
        conn_write, cursor_write = self.database_connection()
        if source_ids == 0:
            datasources_query = "insert into dataSources (source_url,source_name,lastCheckedDate,pdf_count) values (%s,%s,%s,%s)"
            pdfcount = len(records)
            datasources_value = (s_url, s_url, formatted_date, pdfcount)
            cursor_write.execute(datasources_query, datasources_value)
            cursor_write.execute("select LAST_INSERT_ID() as id")
            soure_id = cursor_write.fetchone()['id']
        else:
            datasources_update_query = "update dataSources set pdf_count=%s,lastCheckedDate=%s where source_id=%s;"
            pdfcount = pdf_count + len(records)
            datasources_update_value = (pdfcount, formatted_date, source_ids)
            cursor_write.execute(datasources_update_query, datasources_update_value)
            soure_id = source_ids
        conn_write.commit()
        conn_write.close()
        return soure_id

    def check_pdf_links(self, allpdflinks, source_id):
        Unparsed_pdfs = []
        for pdflink in allpdflinks:
            conn_read, cursor_read = self.database_connection()
            query_pdf = "select * from dataPDFs where source_id=%s and original_pdf_url='%s'" % (
                source_id, str(pdflink).strip())
            cursor_read.execute(query_pdf)
            pdf_records = cursor_read.fetchall()
            conn_read.close()
            if len(pdf_records) == 0:
                Unparsed_pdfs.append(pdflink)
        return Unparsed_pdfs

    def insert_into_dataPDFs(self, souce_id, all_records):
        now = datetime.now()
        formatted_date = now.strftime('%Y-%m-%d')
        conn_write, cursor_write = self.database_connection()
        for datapdf in all_records:
            datapdfs_query = "insert into dataPDFs (source_id,original_pdf_url,scrap_pdf_location,downloadDate,creationDate,file_name,extracted_abstract) values (%s,%s,%s,%s,%s,%s,%s)"
            datapdfs_value = (souce_id, datapdf[0], datapdf[1], formatted_date, formatted_date, datapdf[2], "Dummy")
            cursor_write.execute(datapdfs_query, datapdfs_value)
            cursor_write.execute("select LAST_INSERT_ID() as id")
            pdf_id = cursor_write.fetchone()['id']
            ner_dict = datapdf[3]
            if ner_dict:
                for nerkey in ner_dict:
                    if self.conceptdict.get(nerkey,None):
                        concept_id= self.conceptdict.get(nerkey)
                        nerval = ', '.join(ner_dict.get(nerkey))
                        query_dictionaryentries_insert = "insert into dictionaryEntries (concept_id,entry,entryLanguage,creationDate,createdBy,isApproved) values (%s,%s,%s,%s,%s,%s);"
                        query_dictionaryentries_value =(concept_id,nerval,"en",formatted_date,"model","yes")
                        cursor_write.execute(query_dictionaryentries_insert,query_dictionaryentries_value)
                        cursor_write.execute("select LAST_INSERT_ID() as id")
                        entry_id = cursor_write.fetchone()['id']
        conn_write.commit()
        conn_write.close()


    def crawl(self):
        for url in self.urls:
            cc = 0
            while cc < 3:
                cc += 1
                try:
                    session = requests.session()
                    response = session.get(str(url), verify=False, headers=self.headers, timeout=40, stream=True)
                    print('+++++++', response.status_code)
                    if response.status_code == 200:
                        break
                except Exception as e:
                    time.sleep(1)
                    pass
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'lxml')
                raw_link = [li.get("href") for li in soup.find_all('a') if li.get("href")]
                # print(raw_link)
                all_link = self.remove_invalid_url(raw_link, url)
                print(len(all_link))
                All_pdf_links = []
                # depth = 0
                # while depth < self.depth:
                #     print("depth::", depth)
                #     All_pdf, other_links = self.get_pdf_links(list(set(all_link)), depth)
                #     depth += 1
                #     print(len(All_pdf), len(other_links))
                #     if len(All_pdf) > 0:
                #         # print(All_pdf)
                #         All_pdf_links.extend(All_pdf)
                #     if len(other_links) > 0:
                #         all_link = other_links
                print('PDF+++++++++', len(list(set(All_pdf_links))))
                print(list(set(All_pdf_links)))
                All_pdf_links = ['https://www.mckinsey.com/~/media/McKinsey/Careers REDESIGN/Interviewing/Main/Problem Solving Test PDFs/FAQs-for-candidates.pdf']
                # All_pdf_links = [
                #     "https://www.mckinsey.com/~/media/McKinsey/About Us/Social responsibility/Social-Responsibility-Report-2018.pdf",
                #     "https://www.mckinsey.com/~/media/mckinsey/industries/private equity and principal investors/our insights/mckinseys private markets annual review/mckinsey-global-private-markets-review-2020-v4.pdf"]
                souce_ur_db = self.check_exist(url)
                print(souce_ur_db)
                source_id = 0
                pdf_count = 0
                if len(souce_ur_db) > 0:
                    source_id = souce_ur_db[0].get("source_id")
                    pdf_count = souce_ur_db[0].get("pdf_count")
                if source_id > 0:
                    All_pdf_links = self.check_pdf_links(All_pdf_links, source_id)
                if len(All_pdf_links) > 0:
                    Final_data = []
                    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor2:
                        future_to_url = {
                            executor2.submit(self.download_pdf, str(pdf_url).strip(), url): pdf_url for
                            pdf_url in list(set(All_pdf_links))[:1]}
                        for future in concurrent.futures.as_completed(future_to_url):
                            future_to_url[future]
                            try:
                                data4 = future.result()
                                if len(data4) > 0:
                                    Final_data.append(data4)
                            except Exception as exc:
                                print('pdf_download>>>>', exc)
                                pass
                    print(len(Final_data), Final_data)
                    if len(Final_data)>0:
                        source_id_db = self.insert_into_database(Final_data, url, source_id, pdf_count)
                        self.insert_into_dataPDFs(source_id_db, Final_data)


if __name__ == "__main__":
    obj = PdfCrawler()
    # urls = ["http://agricoop.nic.in/recruitment"]
    obj.crawl()
