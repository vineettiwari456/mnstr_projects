# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import requests
import time, os, re, sys, csv, shutil
from urllib.parse import urljoin
import concurrent.futures
import codecs
from decouple import config
from datetime import datetime
from get_pdf_text import MyParser
from get_ner_spacy import NerSpacy
from parse import Parse
import mysql.connector
from azure.storage.blob import BlockBlobService, PublicAccess
from dateutil.parser import parse as parsedate
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
        main_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Log")
        if not os.path.exists(main_directory):
            os.mkdir(main_directory)
        self.logfilename = main_directory + "/error_log"
        self.script_logfilename = main_directory + "/script_log"
        self.obj_parse = Parse()
        self.ner_obj = NerSpacy()
        self.urls = []
        self.depth = int(config("DEPTH"))
        self.abstract_length = int(config("ABSTRACTED_LENGTH"))
        # Create the BlockBlockService that is used to call the Blob service for the storage account
        self.account_name = str(config("ACCOUNT_NAME")).strip()
        self.block_blob_service = BlockBlobService(account_name=self.account_name,
                                                   account_key=str(config("ACCOUNT_KEY")).strip(), socket_timeout=600)
        # Create a container called 'quickstartblobs'.
        self.container_name = str(config("CONTAINER_NAME")).lower().strip()
        self.block_blob_service.create_container(self.container_name)
        self.block_blob_service.set_container_acl(self.container_name, public_access=PublicAccess.Container)
        self.conf = {"user": config("DB_USER"), "password": config("DB_PASSWORD"), "host": config("DB_HOST"),
                     "database": config("DB_NAME")}

        self.conceptdict = {}
        self.load_csv()
        self.load_master_data()

    def load_csv(self):
        csv_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "urls_list.csv")
        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            line_count = 0
            for row in csv_reader:
                if "Urls" not in row[0]:
                    self.urls.append(str(row[0]).strip())

    def database_connection(self):
        connection = mysql.connector.connect(user=self.conf['user'], password=self.conf['password'],
                                             host=self.conf['host'],
                                             database=self.conf['database'])
        cursor = connection.cursor(dictionary=True)
        return connection, cursor

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

    def write_log(self, logfilename, logtext):
        logopenfile = open(logfilename, 'a')
        logopenfile.write(str(logtext))
        logopenfile.write("\n")
        logopenfile.close()

    def upload_pdf_into_azure(self, pdf_path, file_name, s_url):
        is_upload = False
        azure_file_path = None
        cc = 0
        while cc < 2 and not is_upload:
            try:
                cc += 1
                s_url = ''.join(re.sub(r'[^\w\s]', '', s_url).split())
                azure_file_path = str(s_url) + "/" + str(file_name)
                # Upload the created file, use local_file_name for the blob name
                self.block_blob_service.create_blob_from_path(self.container_name, azure_file_path, pdf_path)
                is_upload = True
            except Exception as e:
                if cc == 2:
                    logtext = str(datetime.now()) + " : Error in Upload Azure blob :" + file_name + str(e)
                    self.write_log(self.logfilename, logtext)
                pass
        return is_upload, azure_file_path

    def check_pdf_link(self, link):
        pdf_link = []
        other_link = None
        if 'pdf' in link.lower() or "ashx" in link.lower():
            try:
                cc = 0
                while cc < 2:
                    cc += 1
                    try:
                        session = requests.session()
                        response = session.get(str(link), stream=True, headers=self.headers, timeout=180)
                        if response.status_code == 200:
                            break
                    except Exception as e:
                        time.sleep(1)
                        pass
                if response.status_code == 200:
                    response_header = response.headers
                    if response_header.get("Content-Type") == "application/pdf" or response_header.get(
                            "Content-Type") == "file/download":
                        last_modified = None
                        last_modified = response_header.get('last-modified', None)
                        if last_modified:
                            last_modified = str(parsedate(last_modified).strftime('%Y-%m-%d')).strip()
                        if last_modified:
                            lastname = str(link).split("/")[-1]
                            pdf_name = ''.join(re.sub(r'[^\x00-\x7F]+', '', lastname).split())
                            pdf_path = os.path.join(self.filepath, pdf_name)
                            with open(pdf_path, 'wb') as fd:
                                fd.write(response.content)
                            time.sleep(2)
                            pdf_link.append(pdf_name)
                            pdf_link.append(pdf_path)
                            pdf_link.append(last_modified)
                            pdf_link.append(str(link).strip())
            except Exception as e:
                pass
        else:
            other_link = link
        return pdf_link, other_link

    def next_page_link(self, next_ul):
        all_link = []
        try:
            cc = 0
            while cc < 1:
                cc += 1
                try:
                    session = requests.session()
                    response = session.get(str(next_ul), headers=self.headers, timeout=60)
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

    def remove_exists_db_pdflinks(self, links, source_id):
        all_links = []
        for link in links:
            try:
                if 'pdf' in link.lower():
                    unparse, iszero = self.check_pdf_links([link], source_id)
                    if len(unparse) > 0:
                        all_links.append(link)
                else:
                    all_links.append(link)
            except Exception as e:
                pass
        return all_links

    def get_pdf_links(self, alllinks, depth, is_json=False):
        All_pdf = []
        other_links = []
        # if source_id > 0:
        #     alllinks = self.remove_exists_db_pdflinks(alllinks, source_id)
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            future_to_url = {
                executor.submit(self.check_pdf_link, urllink): urllink for urllink in list(set(alllinks))}
            for future in concurrent.futures.as_completed(future_to_url):
                future_to_url[future]
                try:
                    pdf, nextlink = future.result()
                    if len(pdf) > 0:
                        All_pdf.append(pdf)
                    if not is_json:
                        if nextlink:
                            if self.depth - depth == 1:
                                other_links.append(nextlink)
                            else:
                                nextlink_urls = self.next_page_link(nextlink)
                                other_links.extend(nextlink_urls)
                except Exception as exc:
                    pass

        return All_pdf, list(set(other_links))

    def remove_invalid_url(self, all_raw_urls, url):
        all_link = []
        for link in all_raw_urls:
            if "mailto" not in link.lower() and "javascript" not in link.lower() and len(link) > 6:
                if str(link).lower().startswith("http"):
                    all_link.append(str(link).strip())
                else:
                    new_link = urljoin(url, link)
                    all_link.append(str(new_link))
        return list(set(all_link))

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
            pdfcount = pdf_count + len([mk for mk in records if mk[-1] == "is_new"])
            datasources_update_value = (pdfcount, formatted_date, source_ids)
            cursor_write.execute(datasources_update_query, datasources_update_value)
            soure_id = source_ids
        conn_write.commit()
        conn_write.close()
        return soure_id

    def checkdictionaries_entity(self, entities, concept_id, pdf_id):
        insert_entities = []
        conn, curr = self.database_connection()
        for ent in entities:
            try:
                query = "select * from dictionaryEntries where concept_id=%s and pdf_id=%s and entry=%s;"
                value = (concept_id, pdf_id, str(ent).strip())
                curr.execute(query, value)
                records = curr.fetchall()
                if len(records) == 0:
                    insert_entities.append(ent)
            except Exception as e:
                pass
        conn.close()
        return insert_entities

    def checkpdf_extracted_entity(self, keywords, pdf_id):
        insert_keywords = []
        conn, curr = self.database_connection()
        for ent in keywords:
            try:
                query = "select * from pdfextractedentities where pdf_id=%s and name=%s"
                value = (pdf_id, str(ent[0]).strip())
                curr.execute(query, value)
                records = curr.fetchall()
                if len(records) == 0:
                    insert_keywords.append(ent)
            except Exception as ex:
                pass
        conn.close()
        return insert_keywords

    def insert_into_dataPDFs(self, souce_id, all_records):
        now = datetime.now()
        formatted_date = now.strftime('%Y-%m-%d')
        for datapdf in all_records:
            try:
                conn_write, cursor_write = self.database_connection()
                if datapdf[-1].strip() == "is_new":
                    datapdfs_query = "insert into dataPDFs (source_id,original_pdf_url,scrap_pdf_location,downloadDate,creationDate,file_name,extracted_abstract) values (%s,%s,%s,%s,%s,%s,%s)"
                    datapdfs_value = (souce_id, datapdf[0], datapdf[1], formatted_date, datapdf[7], datapdf[2],
                                      str(datapdf[8])[:self.abstract_length])
                    cursor_write.execute(datapdfs_query, datapdfs_value)
                    cursor_write.execute("select LAST_INSERT_ID() as id")
                    pdf_id = cursor_write.fetchone()['id']
                    pdfContents_insert_query = "insert into pdfContents(pdf_id,pdfText) values (%s,%s)"
                    pdfContents_insert_value = (pdf_id, str(datapdf[8]))
                    cursor_write.execute(pdfContents_insert_query, pdfContents_insert_value)
                    ner_dict = datapdf[3]
                    if ner_dict:
                        for nerkey in ner_dict:
                            if self.conceptdict.get(nerkey, None):
                                concept_id = self.conceptdict.get(nerkey)
                                if len(ner_dict.get(nerkey)) > 0:
                                    for nerval in ner_dict.get(nerkey):
                                        # nerval = ', '.join(ner_dict.get(nerkey))
                                        query_dictionaryentries_insert = "insert into dictionaryEntries (concept_id,pdf_id,entry,entryLanguage,creationDate,createdBy,isApproved) values (%s,%s,%s,%s,%s,%s,%s);"
                                        query_dictionaryentries_value = (
                                            concept_id, pdf_id, nerval, "en", formatted_date, "model", "yes")
                                        cursor_write.execute(query_dictionaryentries_insert, query_dictionaryentries_value)
                    keyword_weights = datapdf[4]
                    if len(keyword_weights) > 0:
                        for kw in keyword_weights:
                            query_pdfExtractedEntities = "insert into pdfExtractedEntities (pdf_id,name,extractionModel,modelScore,creationDate) values (%s,%s,%s,%s,%s);"
                            query_pdfExtractedEntities_value = (pdf_id, kw[0], "nltk", kw[1], formatted_date)
                            cursor_write.execute(query_pdfExtractedEntities, query_pdfExtractedEntities_value)
                elif datapdf[-1].strip() == "is_update":
                    update_pdfid = datapdf[9]
                    datapdfs_query_update = "update dataPDFs set creationDate=%s,extracted_abstract=%s,scrap_pdf_location=%s,downloadDate=%s where original_pdf_url=%s and pdf_id=%s;"
                    datapdfs_query_update_value = (
                        datapdf[7], datapdf[8][:self.abstract_length], datapdf[1], formatted_date, datapdf[0], update_pdfid)
                    cursor_write.execute(datapdfs_query_update, datapdfs_query_update_value)
                    pdfContents_update_query = "update pdfContents set pdfText=%s where pdf_id=%s;"
                    pdfContents_update_value = (str(datapdf[8]), update_pdfid)
                    cursor_write.execute(pdfContents_update_query, pdfContents_update_value)
                    ner_dict = datapdf[3]
                    if ner_dict:
                        for nerkey in ner_dict:
                            if self.conceptdict.get(nerkey, None):
                                concept_id = self.conceptdict.get(nerkey)
                                if len(ner_dict.get(nerkey)) > 0:
                                    nerlist = self.checkdictionaries_entity(ner_dict.get(nerkey), concept_id, update_pdfid)
                                    for nerval in nerlist:
                                        query_dictionaryentries_insert = "insert into dictionaryEntries (concept_id,pdf_id,entry,entryLanguage,creationDate,createdBy,isApproved) values (%s,%s,%s,%s,%s,%s,%s);"
                                        query_dictionaryentries_value = (
                                            concept_id, pdf_id, nerval, "en", formatted_date, "model", "yes")
                                        cursor_write.execute(query_dictionaryentries_insert, query_dictionaryentries_value)
                    keyword_weights = datapdf[4]
                    if len(keyword_weights) > 0:
                        keyword_weights = self.checkpdf_extracted_entity(keyword_weights, update_pdfid)
                        for kw in keyword_weights:
                            query_pdfExtractedEntities = "insert into pdfExtractedEntities (pdf_id,name,extractionModel,modelScore,creationDate) values (%s,%s,%s,%s,%s);"
                            query_pdfExtractedEntities_value = (update_pdfid, kw[0], "nltk", kw[1], formatted_date)
                            cursor_write.execute(query_pdfExtractedEntities, query_pdfExtractedEntities_value)
                conn_write.commit()
                conn_write.close()
            except Exception as edb:
                error_insertiondb_text = str(datetime.now()) + " Error in insert_into_dataPDFs :: " + str(edb) + str(datapdf[:3])
                self.write_log(self.logfilename, error_insertiondb_text)
                pass



    def download_pdf(self, pdf_name, pdf_path, last_modified, pdf_url, is_new_update, pdf_id, sourceurl):
        temp = []
        try:
            doc_texts = ""
            try:
                p_doc = MyParser(pdf_path)
                doc_text = ' '.join([k.strip() for k in p_doc.records if k])
                doc_texts = self.obj_parse.parse(doc_text)
            except Exception as err_resume_text:
                res_text_error = str(datetime.now()) + " :Error in convert pdf to text: " + str(
                    pdf_path) + " " + str(err_resume_text)
                self.write_log(self.logfilename, res_text_error)
                pass
            if len(doc_texts) > 50:
                is_upload, azure_file_dir = self.upload_pdf_into_azure(pdf_path, pdf_name, sourceurl)
                if is_upload:
                    if 'cid129' in doc_text:
                        doc_text = doc_text.replace('cid129','ff')
                    doc_texts = self.obj_parse.parse(doc_text)
                    nerkeywods = self.ner_obj.extract_ner(doc_texts)
                    nerkeywods_weights = self.ner_obj.extract_weight(doc_texts)
                    azure_file_path = "https://" + str(
                        self.account_name).strip() + ".blob.core.windows.net/" + self.container_name + "/" + str(
                        azure_file_dir).strip()
                    temp.append(pdf_url)
                    temp.append(azure_file_path)
                    temp.append(pdf_name)
                    temp.append(nerkeywods)
                    temp.append(nerkeywods_weights)
                    temp.append(pdf_path)
                    temp.append(sourceurl)
                    temp.append(last_modified)
                    temp.append(doc_texts)
                    temp.append(pdf_id)
                    temp.append(is_new_update)
            else:
                empty_pdf = str(datetime.now()) + " Empty text in PDF : " + pdf_url
                self.write_log(self.logfilename, empty_pdf)
        except Exception as e:
            parse_error = str(datetime.now()) + " Error in parse text : " + pdf_url + " :: " + str(e)
            self.write_log(self.logfilename, parse_error)
            pass
        return temp

    def check_pdf_links(self, allpdflinks, source_id):
        Unparsed_pdfs = []
        pdf_counts = []
        # is_zero = False
        count_data = 0
        for pdflink in allpdflinks:
            conn_read, cursor_read = self.database_connection()
            query_pdf = "select * from dataPDFs where source_id=%s and original_pdf_url='%s'" % (
                source_id, str(pdflink[-1]).strip())
            cursor_read.execute(query_pdf)
            pdf_records = cursor_read.fetchall()
            pdfdata_query = "select count(*) as count from dataPDFs where source_id=%s" % (source_id)
            cursor_read.execute(pdfdata_query)
            pdf_counts = cursor_read.fetchall()
            conn_read.close()
            if len(pdf_records) == 0:
                pdflink.append("is_new")
                pdflink.append("None")
                Unparsed_pdfs.append(pdflink)
            elif len(pdf_records) > 0:
                # pdf_counts.append(pdflink)
                if str(pdf_records[0].get("creationDate")).strip() != str(pdflink[-2]).strip():
                    pdflink.append("is_update")
                    pdflink.append(pdf_records[0].get("pdf_id"))
                    Unparsed_pdfs.append(pdflink)
        if len(pdf_counts) > 0:
            count_data = pdf_counts[0].get("count", 0)
            conn_write, cursor_write = self.database_connection()
            query_update = "update dataSources set pdf_count=%s where source_id=%s;" % (count_data, source_id)
            cursor_write.execute(query_update)
            conn_write.commit()
            conn_write.close()
            # is_zero = True
        return Unparsed_pdfs, count_data

    def remove_duplicate_urls(self, urls):
        Links = []
        temp = []
        for ul in urls:
            url = str(ul[-1]).lower()
            if url not in temp:
                temp.append(url)
                Links.append(ul)
        return Links

    def iterate_all(self, iterable, returned="key"):
        """Returns an iterator that returns all keys or values
           of a (nested) iterable.

           Arguments:
               - iterable: <list> or <dictionary>
               - returned: <string> "key" or "value"

           Returns:
               - <iterator>
        """

        if isinstance(iterable, dict):
            for key, value in iterable.items():
                if returned == "key":
                    yield key
                elif returned == "value":
                    if not (isinstance(value, dict) or isinstance(value, list)):
                        yield value
                else:
                    raise ValueError("'returned' keyword only accepts 'key' or 'value'.")
                for ret in self.iterate_all(value, returned=returned):
                    yield ret
        elif isinstance(iterable, list):
            for el in iterable:
                for ret in self.iterate_all(el, returned=returned):
                    yield ret

    def crawl(self):
        len_urls = str(datetime.now()) + "  Number of URLs Found = " + str(len(self.urls))
        print(len_urls)
        self.write_log(self.script_logfilename, len_urls)
        for url in self.urls:
            start_url = str(datetime.now()) + " Started URL: " + str(url)
            print(start_url)
            self.write_log(self.script_logfilename, start_url)
            try:
                cc = 0
                while cc < 3:
                    cc += 1
                    try:
                        session = requests.session()
                        response = session.get(str(url), verify=False, headers=self.headers, timeout=40, stream=True)
                        # print('+++++++', response.status_code)
                        if response.status_code == 200:
                            break
                    except Exception as e:
                        time.sleep(1)
                        pass
                if response.status_code == 200:
                    json_data = {}
                    try:
                        json_data = response.json()
                    except Exception as e:
                        pass
                    souce_ur_db = self.check_exist(url)
                    source_id = 0
                    pdf_count = 0
                    if len(souce_ur_db) > 0:
                        source_id = souce_ur_db[0].get("source_id")
                        pdf_count = souce_ur_db[0].get("pdf_count")
                    All_pdf_links = []
                    if len(json_data) > 1:
                        all_link = list(self.iterate_all(json_data, returned="value"))
                        All_pdf_json, other_links = self.get_pdf_links(list(set(all_link)), 0, is_json=True)
                        logtext_json = str(datetime.now()) + " :: Completed json contains URL. Total PDFs found=" + str(
                            len(All_pdf_json))
                        print(logtext_json)
                        self.write_log(self.script_logfilename, logtext_json)
                        if len(All_pdf_json) > 0:
                            All_pdf_links.extend(All_pdf_json)
                    else:
                        soup = BeautifulSoup(response.text, 'lxml')
                        raw_link = [li.get("href") for li in soup.find_all('a') if li.get("href")]
                        all_link = self.remove_invalid_url(raw_link, url)
                        # print(len(all_link))
                        # All_pdf_links = []
                        depth = 0
                        while depth < self.depth:
                            print(str(datetime.now()), " Running depth:: ", depth + 1)
                            All_pdf, other_links = self.get_pdf_links(list(set(all_link)), depth)
                            depth += 1
                            logtext = str(datetime.now()) + " :: Completed depth " + str(
                                depth) + ". Total PDFs found=" + str(len(All_pdf))
                            print(logtext)
                            self.write_log(self.script_logfilename, logtext)
                            if len(All_pdf) > 0:
                                All_pdf_links.extend(All_pdf)
                            if len(other_links) > 0:
                                all_link = other_links
                    All_pdf_links = self.remove_duplicate_urls(All_pdf_links)
                    logpdf = str(datetime.now()) + ' All unique PDF count :: ' + str(len(All_pdf_links))
                    print(logpdf)
                    self.write_log(self.script_logfilename, logpdf)
                    if source_id > 0:
                        All_pdf_links, pdfcnt = self.check_pdf_links(All_pdf_links, source_id)
                        pdf_count = pdfcnt
                    else:
                        All_pdf_links = [pdf_li + ["is_new", None] for pdf_li in All_pdf_links]
                    dbcheck_log = str(datetime.now()) + " After checking PDF into DB length : " + str(
                        len(All_pdf_links))
                    # print(dbcheck_log)
                    self.write_log(self.script_logfilename, dbcheck_log)
                    if len(All_pdf_links) > 0:
                        Final_data = []
                        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor2:
                            future_to_url = {
                                executor2.submit(self.download_pdf, pdf_url[0], pdf_url[1], pdf_url[2], pdf_url[3],
                                                 pdf_url[4], pdf_url[5], url): pdf_url for
                                pdf_url in All_pdf_links}
                            for future in concurrent.futures.as_completed(future_to_url):
                                future_to_url[future]
                                try:
                                    data4 = future.result()
                                    if len(data4) > 0:
                                        Final_data.append(data4)
                                except Exception as exc:
                                    pass
                        insert_pdf_log = str(datetime.now()) + " Inserted New PDFs= " + str(len(Final_data))
                        print(insert_pdf_log)
                        self.write_log(self.script_logfilename, insert_pdf_log)
                        if len(Final_data) > 0:
                            source_id_db = self.insert_into_database(Final_data, url, source_id, pdf_count)
                            self.insert_into_dataPDFs(source_id_db, Final_data)
            except Exception as exc:
                error_text = str(datetime.now()) + " Error in crawl :: " + str(exc) + str(url)
                self.write_log(self.logfilename, error_text)
            # Remove directory from local
            try:
                folder = self.filepath
                for filename in os.listdir(folder):
                    file_path = os.path.join(folder, filename)
                    try:
                        if os.path.isfile(file_path) or os.path.islink(file_path):
                            os.unlink(file_path)
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)
                    except Exception as e:
                        # print('Failed to delete %s. Reason: %s' % (file_path, e))
                        pass
            except:
                pass


if __name__ == "__main__":
    obj = PdfCrawler()
    obj.crawl()
