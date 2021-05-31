# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import requests
import time, os, re, sys, csv
from urllib.parse import urljoin
import concurrent.futures
import codecs
from datetime import datetime
from get_pdf_text import MyParser
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
        self.obj_parse = Parse()
        self.urls = []
        self.depth = 2
        self.load_csv()

    def load_csv(self):
        csv_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "urls_list.csv")
        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            line_count = 0
            for row in csv_reader:
                if "Urls" not in row[0]:
                    self.urls.append(str(row[0]).strip())

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
                        pdf_link = link
            except Exception as e:
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
                if doc_text != "":
                    doc_texts = self.obj_parse.parse(doc_text)
                    temp.append(pdf_url)
                    temp.append(pdf_name)
                    temp.append(pdf_path)
                    temp.append(sourceurl)
        except Exception as e:
            pass
        return temp

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
                depth = 0
                while depth < self.depth:
                    print("depth::", depth)
                    All_pdf, other_links = self.get_pdf_links(list(set(all_link)), depth)
                    depth += 1
                    print("Length of PDF:",len(All_pdf), len(other_links))
                    if len(All_pdf) > 0:
                        # print(All_pdf)
                        All_pdf_links.extend(All_pdf)
                    if len(other_links) > 0:
                        all_link = other_links
                print('PDF+++++++++', len(list(set(All_pdf_links))))
                if len(All_pdf_links) > 0:
                    Final_data = []
                    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor2:
                        future_to_url = {
                            executor2.submit(self.download_pdf, str(pdf_url).strip(), url): pdf_url for
                            pdf_url in list(set(All_pdf_links))}
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


if __name__ == "__main__":
    obj = PdfCrawler()
    obj.crawl()
