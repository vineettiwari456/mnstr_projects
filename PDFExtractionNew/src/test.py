# import requests
# links = ['http://agricoop.gov.in/sites/default/files/Time-Series-1st-Adv-Estimate-2019-20-Final-Press.pdf', 'http://agricoop.gov.in/sites/default/files/2ndADVEST201819_E.pdf', 'http://agricoop.gov.in/sites/default/files/Time-Series-English.pdf', 'http://agricoop.gov.in/sites/default/files/Time-Series-1st-Adv-Estimate-2019-20-Final-Hindi.pdf', 'http://agricoop.nic.in/sites/default/files/adc_vac_cancel.pdf', 'http://agricoop.nic.in/sites/default/files/scan117.pdf', 'http://agricoop.gov.in/sites/default/files/Time-Series-Hindi.pdf', 'http://agricoop.gov.in/sites/default/files/3rdADVEST201819_H.pdf', 'http://agricoop.nic.in/sites/default/files/OM%20on%20YP.pdf', 'http://agricoop.nic.in/sites/default/files/scan116.pdf', 'http://agricoop.gov.in/sites/default/files/agristatglance2018.pdf', 'http://agricoop.nic.in/sites/default/files/DG%20MANAGE.pdf', 'http://agricoop.nic.in/sites/default/files/Vacancy%20Circular%2C%20DD%28Admn.%292020.pdf', 'http://agricoop.nic.in/sites/default/files/Vacancies%20%20circular%20of%20OS.pdf', 'http://agricoop.gov.in/sites/default/files/handbookworkallo_2018.pdf', 'http://agricoop.nic.in/sites/default/files/ADV_WITHDRWL.pdf']
# print(len(links))
# res = requests.get("http://cashlessindia.gov.in/",stream=True)
# print(res)
# print(res.headers)
# from PIL import Image, ImageEnhance, ImageFilter
# import pytesseract
# import PyPDF2, struct, time
# import codecs, uuid, os, sys, warnings
#
# warnings.filterwarnings("ignore")
# reload(sys)
# sys.setdefaultencoding('utf8')
# sys.stdout = codecs.getwriter("iso-8859-1")(sys.stdout, 'xmlcharrefreplace')
#
#
# def tiff_header_for_CCITT(width, height, img_size, CCITT_group=4):
#     tiff_header_struct = '<' + '2s' + 'h' + 'l' + 'h' + 'hhll' * 8 + 'h'
#     return struct.pack(tiff_header_struct,
#                        b'II',  # Byte order indication: Little indian
#                        42,  # Version number (always 42)
#                        8,  # Offset to first IFD
#                        8,  # Number of tags in IFD
#                        256, 4, 1, width,  # ImageWidth, LONG, 1, width
#                        257, 4, 1, height,  # ImageLength, LONG, 1, lenght
#                        258, 3, 1, 1,  # BitsPerSample, SHORT, 1, 1
#                        259, 3, 1, CCITT_group,  # Compression, SHORT, 1, 4 = CCITT Group 4 fax encoding
#                        262, 3, 1, 0,  # Threshholding, SHORT, 1, 0 = WhiteIsZero
#                        273, 4, 1, struct.calcsize(tiff_header_struct),  # StripOffsets, LONG, 1, len of header
#                        278, 4, 1, height,  # RowsPerStrip, LONG, 1, lenght
#                        279, 4, 1, img_size,  # StripByteCounts, LONG, 1, size of image
#                        0  # last IFD
#                        )
#
#
# def extract_pdf_image_text(pdf_path):
#     pdf_file = open(pdf_path, 'rb')
#     cond_scan_reader = PyPDF2.PdfFileReader(pdf_file)
#     text_data = ""
#     try:
#         for i in range(0, cond_scan_reader.getNumPages()):
#             page = cond_scan_reader.getPage(i)
#             # print '----', page
#             xObject = []
#             if page.get('/Resources', '').get('/XObject', ''):
#                 xObject = page.get('/Resources', '').get('/XObject', '').getObject()
#             for obj in xObject:
#                 if xObject[obj]['/Subtype'] == '/Image':
#                     """
#                     The  CCITTFaxDecode filter decodes image data that has been encoded using
#                     either Group 3 or Group 4 CCITT facsimile (fax) encoding. CCITT encoding is
#                     designed to achieve efficient compression of monochrome (1 bit per pixel) image
#                     data at relatively low resolutions, and so is useful only for bitmap image data, not
#                     for color images, grayscale images, or general data.
#
#                     K < 0 --- Pure two-dimensional encoding (Group 4)
#                     K = 0 --- Pure one-dimensional encoding (Group 3, 1-D)
#                     K > 0 --- Mixed one- and two-dimensional encoding (Group 3, 2-D)
#                     """
#                     if xObject[obj]['/Filter'] == '/CCITTFaxDecode':
#                         if xObject[obj]['/DecodeParms']['/K'] == -1:
#                             CCITT_group = 4
#                         else:
#                             CCITT_group = 3
#                         width = xObject[obj]['/Width']
#                         height = xObject[obj]['/Height']
#                         data = xObject[obj]._data  # sorry, getData() does not work for CCITTFaxDecode
#                         img_size = len(data)
#                         tiff_header = tiff_header_for_CCITT(width, height, img_size, CCITT_group)
#                         img_name = obj[1:] + str(uuid.uuid1())[2:8] + '.png'
#                         temp_pdf_name = create_directory("PDF", '') + img_name
#                         with open(temp_pdf_name, 'wb') as img_file:
#                             img_file.write(tiff_header + data)
#                         temp_pdf_name.close()
#                         pytesseract.pytesseract.tesseract_cmd = 'C:/Program Files (x86)/Tesseract-OCR/tesseract'
#                         data = pytesseract.image_to_string(Image.open(temp_pdf_name))
#                         main_data = ' '.join(data.split())
#                         text_data = (str(text_data) + str(main_data)).encode("utf8")
#                         time.sleep(5)
#                         os.remove(temp_pdf_name)
#         pdf_file.close()
#     except Exception as e:
#         print(e)
#         pass
#     return str(text_data)
#
#
# def next_image_pdf_data(pdf_pa):
#     pdf = file(pdf_pa, "rb").read()
#     startmark = "\xff\xd8"
#     startfix = 0
#     endmark = "\xff\xd9"
#     endfix = 2
#     i = 0
#     njpg = 0
#     text_data = ""
#     while True:
#         try:
#             istream = pdf.find("stream", i)
#             if istream < 0:
#                 break
#             istart = pdf.find(startmark, istream, istream + 20)
#             if istart < 0:
#                 i = istream + 20
#                 continue
#             iend = pdf.find("endstream", istart)
#             if iend < 0:
#                 raise Exception("Didn't find end of stream!")
#             iend = pdf.find(endmark, iend - 20)
#             if iend < 0:
#                 raise Exception("Didn't find end of JPG!")
#
#             istart += startfix
#             iend += endfix
#             # print "JPG %d from %d to %d" % (njpg, istart, iend)
#             jpg = pdf[istart:iend]
#             fi = str(uuid.uuid1())[2:8] + "jpg%d.png" % njpg
#             # temp_pdf_path = create_directory("PDF", '') + fi
#             jpgfile = open(fi, "wb")
#             jpgfile.write(jpg)
#
#             jpgfile.close()
#             # path_fi = os.path.join(os.getcwd(), fi).replace("\\","/")
#             pytesseract.pytesseract.tesseract_cmd = 'C:/Program Files/Tesseract-OCR/tesseract'
#             data = pytesseract.image_to_string(Image.open(fi))
#             main_data = ' '.join(data.split())
#             text_data = (str(text_data) + str(main_data)).encode("utf8")
#             njpg += 1
#             i = iend
#             # time.sleep(1)
#             os.remove(fi)
#         except Exception as e:
#             print ('===------->>>>', e)
#             break
#             pass
#     return str(text_data)
#
#
# # pdf_path="E:\\PycharmProjects\\pdfkeywordextraction\\venv\\src\\downloads\\agristatglance2018.pdf"
# # doc_text = extract_pdf_image_text(pdf_path)
# # # doc_text = next_image_pdf_data(pdf_path)
# #
# # print(doc_text)
# # import csv
# # csv_file_path =os.path.join(os.path.dirname(os.path.abspath(__file__)), "urls_list.csv")
# # with open(csv_file_path,'rb') as csv_file:
# #     csv_reader = csv.reader(csv_file)
# #     line_count = 0
# #     for row in csv_reader:
# #         print row
#
# # import nltk
# #
# # sentence = "John bought a Toyota camry 2019 model in Toronto in January 2020 at a cost of $38000"
# # import nltk
# # for sent in nltk.sent_tokenize(sentence):
# #     namedEnt  = nltk.ne_chunk(nltk.pos_tag(nltk.word_tokenize(sent)),binary=True)
# #     namedEnt.draw()
#    # for chunk in nltk.ne_chunk(nltk.pos_tag(nltk.word_tokenize(sent))):
#    #    if hasattr(chunk, 'label'):
#    #       print(chunk.label(), ' '.join(c[0] for c in chunk))
# # import nltk
# # my_sent = "2010 WASHINGTON -- In the wake of a string of abuses by New York police officers in the 1990s, Loretta E. Lynch, the top federal prosecutor in Brooklyn, spoke forcefully about the pain of a broken trust that African-Americans felt and said the responsibility for repairing generations of miscommunication and mistrust fell to law enforcement."
# # # my_sent="John bought a Toyota camry 2019 model in Toronto in January 2020 at a cost of $38000"
# # word = nltk.word_tokenize(my_sent)
# # pos_tag = nltk.pos_tag(word)
# # chunk = nltk.ne_chunk(pos_tag)
# # NE = [ " ".join(w for w, t in ele) for ele in chunk if isinstance(ele, nltk.Tree)]
# # print (NE)
#
#
# # -*- coding:utf-8 -*-
# import nltk
# # from nltk.tag.stanford import StanfordNERTagger
# import nltk.tag.stanford as st
# import os
# from itertools import groupby
# from parse import Parse
# import time,sys
# reload(sys)
# sys.setdefaultencoding('utf8')
#
# class NltkNerExtractor:
#     def __init__(self):
#         self.obj = Parse()
#         java_path = "C:/Program Files/Java/jdk-13/bin/java.exe"
#         os.environ['JAVAHOME'] = java_path
#         claas_file = os.path.join(os.getcwd(),'stanford-ner/english.all.3class.distsim.crf.ser.gz')
#         ner_file = os.path.join(os.getcwd(),'stanford-ner/stanford-ner.jar')
#         self.st = st.NERTagger(claas_file, ner_file)
#
#     def get_continuous_chunks(self, string):
#         string = string
#         continuous_chunk = []
#         current_chunk = []
#
#         for token, tag in self.st.tag(string.split()):
#             if tag != "O":
#                 current_chunk.append((token, tag))
#             else:
#                 if current_chunk:  # if the current chunk is not empty
#                     continuous_chunk.append(current_chunk)
#                     current_chunk = []
#         # Flush the final current_chunk into the continuous_chunk, if any.
#         if current_chunk:
#             continuous_chunk.append(current_chunk)
#         named_entities = continuous_chunk
#         named_entities_str = [" ".join([token for token, tag in ne]) for ne in named_entities]
#         named_entities_str_tag = [(" ".join([token for token, tag in ne]), ne[0][1]) for ne in named_entities]
#         persons = []
#         for l in [l.split(",") for l, m in named_entities_str_tag if m == "PERSON"]:
#             for m in l:
#                 for n in m.strip().split(","):
#                     if len(n) > 0:
#                         persons.append(n.strip("*"))
#         organizations = []
#         for l in [l.split(",") for l, m in named_entities_str_tag if m == "ORGANIZATION"]:
#             for m in l:
#                 for n in m.strip().split(","):
#                     n.strip("*")
#                     if len(n) > 0:
#                         organizations.append(n.strip("*"))
#         locations = []
#         for l in [l.split(",") for l, m in named_entities_str_tag if m == "LOCATION"]:
#             for m in l:
#                 for n in m.strip().split(","):
#                     if len(n) > 0:
#                         locations.append(n.strip("*"))
#         dates = []
#         for l in [l.split(",") for l, m in named_entities_str_tag if m == "DATE"]:
#             for m in l:
#                 for n in m.strip().split(","):
#                     if len(n) > 0:
#                         dates.append(n.strip("*"))
#         money = []
#         for l in [l.split(",") for l, m in named_entities_str_tag if m == "MONEY"]:
#             for m in l:
#                 for n in m.strip().split(","):
#                     if len(n) > 0:
#                         money.append(n.strip("*"))
#         time = []
#         for l in [l.split(",") for l, m in named_entities_str_tag if m == "TIME"]:
#             for m in l:
#                 for n in m.strip().split(","):
#                     if len(n) > 0:
#                         money.append(n.strip("*"))
#
#         percent = []
#         for l in [l.split(",") for l, m in named_entities_str_tag if m == "PERCENT"]:
#             for m in l:
#                 for n in m.strip().split(","):
#                     if len(n) > 0:
#                         money.append(n.strip("*"))
#
#         entities = {}
#         entities['persons'] = persons
#         entities['organizations'] = organizations
#         entities['locations'] = locations
#         entities['dates']= dates
#         entities['money']= money
#         entities['time']= time
#         entities['percent']= percent
#
#         return entities
# if __name__=="__main__":
#     obj = NltkNerExtractor()
#     std = time.time()
#     text = "John tiwari bought a Toyota camry 2019 model in Toronto in January 2020 at a cost of $38000"
#     print obj.get_continuous_chunks(text)
#     print(time.time()-std)
# import mysql.connector
# def database_connection():
#     conf = {"user": "root", "password": "Password@123", "host": "127.0.0.1", "database": "bazooka"}
#     connection = mysql.connector.connect(user=conf['user'], password=conf['password'],
#                                          host=conf['host'],
#                                          database=conf['database'])
#     cursor = connection.cursor(dictionary=True)
#     return connection,cursor
# conn,curr = database_connection()
# print(curr)
import boto3
# import logging
# import boto3
# from botocore.exceptions import ClientError
#
# bucket_name = "lunge-data-pdfs"
# import boto3,os
# session = boto3.Session(
#     aws_access_key_id=AWS_ACCESS_KEY_ID,
#     aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
# )
# s3_connection = session.resource('s3')
# bucket_location = s3_connection.get_bucket_location(Bucket=bucket_name)
# print(bucket_location)
# s3_connection.meta.client.upload_file("E:\\PycharmProjects\\PDFExtractionNew\\venv\\src\\downloads\\FAQs-for-candidates.pdf",bucket_name,"FAQs-for-candidates.pdf")
# s3_connection.create_bucket(Bucket=bucket_name)
# print(s3_connection.buckets.all())
# for bucket in s3_connection.buckets.all():
#         print (bucket.name)
# my_bucket = s3_connection.Bucket(bucket_name)
# for s3_object in my_bucket.objects.all():
#     # Need to split s3_object.key into path and file name, else it will give error file not found.
#     path, filename = os.path.split(s3_object.key)
#     print(path)
#     print(filename)
# ls = ['https://www.mckinsey.com/~/media/McKinsey/Business Functions/Risk/Our Insights/COVID 19 Implications for business/COVID 19 June 18/COVID-19-Briefing-note-June-18.pdf','https://www.mckinsey.com/~/media/mckinsey/business functions/risk/our insights/covid 19 implications for business/covid 19 june 18/covid-19-briefing-note-june-18.pdf']
# print(list(set(ls)))
#
# print(list(dict.fromkeys(ls)))
# def Remove(duplicate):
#     final_list = []
#     for num in duplicate:
#         if str(num).strip() not in final_list:
#             final_list.append(str(num).strip())
#     return final_list
# print(Remove(ls))
# if d:
#     print('eee')
# else:
    # print('nnn')

# covid 19 june 18/covid-19-briefing-note-june-18.pdf
# https://www.mckinsey.com/~/media/mckinsey/business functions/risk/our insights/covid 19 implications for business/covid 19 june 18/covid-19-briefing-note-june-18.pdf
import os, uuid, sys
from azure.storage.blob import BlockBlobService, PublicAccess
# def run_sample():
#     try:
#         # Create the BlockBlockService that is used to call the Blob service for the storage account
#         block_blob_service = BlockBlobService(account_name='lungedatapdfs', account_key='wh/eHMDdfU6qt7WLOmbnfeTxNRH+iPAtNhhM0c0LwRpuAiX6+Yq1dsrwa+jeYoYMTKrh05u6x/N8hx/jLOIr9g==')
#
#         # Create a container called 'quickstartblobs'.
#         container_name ='pdfdata'
#         # block_blob_service.create_container(container_name)
#
#         # Set the permission so the blobs are public.
#         block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)
#
#         # Create a file in Documents to test the upload and download.
#         # local_path=os.path.abspath(os.path.curdir)
#         # local_file_name =input("Enter file name to upload : ")
#         # full_path_to_file =os.path.join(local_path, local_file_name)
#         full_path_to_file = "E:\\PycharmProjects\\PDFExtractionNew\\venv\\src\\downloads1\\coaching-guide-top-tips.pdf"
#         # Write text to the file.
#         #file = open(full_path_to_file,  'w')
#         #file.write("Hello, World!")
#         #file.close()
#         # "covid-19-facts-and-insights-june-1-vf.pdf"
#         local_file_name = "data/coaching-guide-top-tips.pdf"
#         print("Temp file = " + full_path_to_file)
#         print("\nUploading to Blob storage as blob" + local_file_name)
#
#         # Upload the created file, use local_file_name for the blob name
#         # block_blob_service.create_blob_from_path(container_name, local_file_name, full_path_to_file)
#         # block_blob_service.put_block(container_name, local_file_name, full_path_to_file)
#
#         # List the blobs in the container
#         print("\nList blobs in the container")
#         generator = block_blob_service.list_blobs(container_name,prefix='httpswwwmckinseycomfeaturedinsights/')
#         for blob in generator:
#             print("\t Blob name: " + blob.name)
#             # local_file_name = str(blob.name)
#             # full_path_to_file2 = "E:\\PycharmProjects\\PDFExtractionNew\\venv\\src\\downloads1\\test1.pdf"
#             # block_blob_service.get_blob_to_path(container_name, local_file_name, full_path_to_file2)
#
#         # Download the blob(s).
#         # Add '_DOWNLOADED' as prefix to '.txt' so you can see both files in Documents.
#         # full_path_to_file2 = os.path.join(local_path, str.replace(local_file_name ,'.txt', '_DOWNLOADED.txt'))
#         # print("\nDownloading blob to " + full_path_to_file2)
#         # local_file_name ="test.pdf"
#         # full_path_to_file2 = "E:\\PycharmProjects\\PDFExtractionNew\\venv\\src\\downloads1\\test.pdf"
#         # block_blob_service.get_blob_to_path(container_name, local_file_name, full_path_to_file2)
#         #
#         # sys.stdout.write("Sample finished running. When you hit <any key>, the sample will be deleted and the sample "
#         #                  "application will exit.")
#         # sys.stdout.flush()
#         # input()
#         #
#         # # Clean up resources. This includes the container and the temp files
#         # block_blob_service.delete_container(container_name)
#         # os.remove(full_path_to_file)
#         # os.remove(full_path_to_file2)
#     except Exception as e:
#         print(e)
#
#
# # Main method.
# if __name__ == '__main__':
#     run_sample()

# import spacy,re
# from nltk.stem.wordnet import WordNetLemmatizer
# obj_word_lemma = WordNetLemmatizer()
# print (obj_word_lemma.lemmatize("few","v"))
# # stringtext = "Intricate supplier networks that span the globe can deliver with great efficiency, but they may contain hidden vulnerabilities. Even before the COVID‑19 pandemic, a multitude of events in recent years temporarily disrupted production at many companies. Focusing on value chains that produce manufactured goods, this research explores their exposure to shocks, their vulnerabilities, and their expected financial losses. We also assess prospects for value chains to change their physical footprint in response to risk and evaluate strategies to minimize the growing cost of disruptions. Shocks that affect global production are growing more frequent and more severe. Companies face a range of hazards, from natural disasters to geopolitical uncertainties and cyberattacks on their digital systems. Global flows and networks offer more “surface area” for shocks to penetrate and damage to spread. Disruptions lasting a month or longer now occur every 3.7 years on average, and the financial toll associated with the most extreme events has been climbing. Shocks can be distinguished by whether they can be anticipated, how frequently they occur, the breadth of impact across industries and geographies, and the magnitude of impact on supply and demand. Value chains are exposed to different types of shocks based on their geographic footprint, factors of production, and other variables. Those with the highest trade intensity and export concentration in a few countries are more exposed than others. They include some of the highest-value and most soughtafter industries, such as communication equipment, computers and electronics, and semiconductors and components. Many labor-intensive value chains, such as apparel, are highly exposed to pandemics, heat stress, and flood risk. In contrast, food and beverage and fabricated metals have lower average exposure to shocks because they are among the least traded and most regionally oriented value chains. Operational choices can heighten or lessen vulnerability to shocks. Practices such as just-in-time production, sourcing from a single supplier, and relying on customized inputs with few substitutes amplify the disruption of external shocks and lengthen companies’ recovery times. Geographic concentration in supply networks can also be a vulnerability. Globally, we find 180 traded products (worth $134 billion in 2018) for which a single country accounts for the vast majority of exports. Value chain disruptions cause substantial financial losses. Adjusted for the probability and frequency of disruptions, companies can expect to lose more than 40 percent of a year’s profits every decade on average. But a single severe event that disrupts production for 100 days—something that happens every five to seven years on average—could erase almost a year’s earnings in some industries. Disruptions are costly to societies, too: after disasters claim lives and damage communities, production shutdowns can cause job losses and goods shortages. Resilience measures could more than pay off for companies, workers, and broader societies over the long term. The interconnected nature of value chains limits the economic case for making large-scale changes in their physical location. Value chains often span thousands of companies, and their configurations reflect specialization, access to consumer markets around the world, long-standing relationships, and economies of scale. Primarily labor-intensive value chains (such as apparel and furniture) have a strong economic rationale for shifting to new locations. Noneconomic pressures may prompt movement in others, such as pharmaceuticals. Considering both industry economics and national policy priorities, we estimate that 16 to 26 percent of global goods exports, worth $2.9 trillion to $4.6 trillion, could conceivably move to new countries over the next five years if companies restructure their supplier networks. Building supply chain resilience can take many forms beyond relocating production. This includes strengthening risk management capabilities and improving transparency; building redundancy in supplier and transportation networks; holding more inventory; reducing product complexity; creating the capacity to flex production across sites; and improving the financial and operational capacity to respond to shocks and recover quickly from them. "
# stringtext = "GRI Index 2018 Social Responsibility Report General Disclosures Disclosure 102-1 Description Name of the organization Reference or additional information McKinsey & Company 102-2 102-3 102-4 102-5 102-6 Activities, brands, products, and services Overview of McKinsey & Company Location of headquarters McKinsey Fact Sheet Location of operations Locations Ownership and legal form McKinsey Fact Sheet Markets served Locations: 130+ cities, 65+ countries Functions: 12 Business Functions Industries: 21 Industries 2018 GRI Index > General Disclosures McKinsey & Company 2 2018 GRI Index > General Disclosures General Disclosures Disclosure 102-7 Description Scale of the organization Reference or additional information McKinsey Fact Sheet In 2018, McKinsey had revenues in excess of $10 Billion. Change that Matters See also our Locations Functions Industries 102-8 Information on employees and other workers As noted in our McKinsey Fact Sheet, we have approximately 30,000 colleagues, our people speak more than 130 languages and represent over 130 citizenships. McKinsey offers colleagues multiple opportunities for flexibility. Nearly 8% of our colleagues (11% of consulting colleagues) worked part-time at least some period in 2018. In addition, we are supported by a pool of external workers globally, who provide specialized expertise or capacity on an as-needed basis. More information about women at McKinsey is also available on our website. 102-9 102-10 Supply Chain See “Engaging Stakeholders” Significant changes to the organization and its supply chain No significant changes in 2018 102-11 Precautionary Principle or approach We manage our business to reduce, avoid, or mitigate risks. 2018 Social Responsibility Report, Page 34 McKinsey & Company 3 General Disclosures Disclosure 102-12 Description External initiatives 102-13 External Memberships Reference or additional information 2018 Social Responsibility Report. About Us > Diversity About Us > Sustainability See also Functions Industries 2018 Social Responsibility Report. About Us > Diversity About Us > Sustainability 102-14 Statement from decision maker 2018 Social Responsibility Report, page 2 102-16 102-17 Values, principles, standards, and norms of behavior Mechanisms for advice and concerns about ethics (Statement from Kevin Sneader) Our mission and values Code of Professional Conduct Participant Page, UN Global Compact Code of Professional Conduct 2018 Social Responsibility Report Supplier Code of Conduct 102-18 Governance structure McKinsey Fact Sheet 2018 GRI Index > General Disclosures McKinsey & Company 4 2018 GRI Index > General Disclosures General Disclosures Disclosure 102-40 Description Stakeholder groups Reference or additional information See “Stakeholder Engagement” 102-41 Collective bargaining agreements About Us: Responsible Professional Practice (Human Rights) We support the protection of internationally proclaimed human rights and employees’ rights of freedom of association and to take part in collective-bargaining processes. This information is not tracked globally, as most colleagues are not covered by collective bargaining agreements. 102-42 102-43 102-44 102-45 102-46 Identifying and selecting stakeholders See “Stakeholder Engagement” Approach to stakeholder engagement See “Stakeholder Engagement” Key topics and concerns raised See “Stakeholder Engagement” Entities included in the consolidated financial statements Locations: 130+ cities, 65+ countries Our social responsibility reporting covers all locations of our firm globally. Defining report content and topic Boundaries See “Materiality” 102-47 List of material topics See “Materiality” McKinsey & Company 5 2018 GRI Index > General Disclosures General Disclosures Disclosure 102-48 Description Restatements of information Reference or additional information No restatements of information 102-49 Changes in reporting No significant changes 102-50 Date of Reporting January 1 – December 31, 2018 Financial Year 2018 102-51 102-52 102-53 102-54 102-55 102-56 Date of most recent report June 2019 Reporting cycle Annually Contact point for questions regarding the report McKinsey welcomes your comments and questions regarding this report. Please contact us at social_responsibility@mckinsey.com Claims of reporting in accordance with the GRI Standards This report has been prepared in accordance with the GRI Standards: Core option GRI Index This document. 2018 GRI Index. External assurance We conduct external assurance of our GHG emissions. 2018 Social Responsibility Report, page 36 McKinsey & Company 6 Engaging Stakeholders (102-40, 42, 43, 44) 2018 GRI Index > General Disclosures The way in which we run our firm and the decisions that we make affect a range of stakeholders, including our clients, our people (current, alumni and future colleagues ), and our suppliers, as well as our communities. We regularly use information from various touchpoints with these stakeholders to inform our goals, shape our practices, and refine our reporting. For example: Our clients. We communicate regularly with clients through formal and informal feedback, and analyze client requests for proposals (RFPs) and other inquiries (e.g., via third-party platforms and assessments) to identify trends in the issues that are most important to them. Our people. We draw on multiple formal and informal touchpoints to incorporate the insights of current, future and previous colleagues, including surveys and town halls. We also have dedicated teams for recruiting and alumni relations who incorporate feedback from both groups. Our suppliers. We have dedicated supplier management teams who work with and hear directly from suppliers, including which procurement and other practices matter to them.  Our Supplier Code of Conduct sets out our expectations of suppliers, and via the supporting Environmental, Social, and Governance (ESG) questionnaire we gain insights into their practices, which in turn informs our activities related to procurement and supplier management. Our communities.  McKinsey has offices in over 130 cities. These communities are home to our clients and their stakeholders, and to our own people and their families. We want them to be inclusive, sustainable, and prosperous places to live and to work. We support 600+ nonprofits per year, and our interactions with them provide valuable insights into the needs of our communities from multiple perspectives. Civil society, multi-sector organizations. Our practices and reporting are informed and shaped by our participation in initiatives such as the UN Global Compact and RE100, a coalition of businesses dedicated to purchasing 100% renewable electricity by 2025. McKinsey & Company 7 Materiality (102-46, 102-47) 2018 GRI Index > General Disclosures McKinsey has a long-standing commitment to social responsibility. We recognize we have an opportunity, and a responsibility, to use our knowledge and our capabilities to help address the world’s most pressing issues, including environmental and social issues. In 2018, McKinsey became a participant in the United Nations Global Compact (UNGC), a set of ten principles covering the areas of human rights, labor, environment, and anti-corruption. Our firm’s values, the social impact we seek to achieve, and voices of our diverse stakeholders inform the material topics upon which our social responsibility strategy and reporting are based. To identify our material topics, we held working sessions with internal stakeholders, performed benchmarking across peers, referenced the Sustainability Accounting Standards Board (SASB) for the professional services sector, and incorporated external perspectives through a variety of touchpoints (e.g., client requests regarding our social responsibility practices;  requirements of third party social responsibility and sustainability assessments). The priority topics we identified include ▪ Ethics (anti-corruption) ▪ Community engagement ▪ Environmental sustainability ▪ Human rights ▪ Employee development ▪ Diversity and inclusion ▪ Supplier sustainability and diversity ▪ Data privacy and security McKinsey & Company 8 2018 GRI Index > Material Topics Ethics (anti-corruption) Disclosure Description Reference or additional information 103-1 103-2 103-3 205-2 Explanation of the material topic and its Boundary See “Materiality” Participant Page, UN Global Compact About Us: Responsible Professional Practice (anti-corruption) The management approach and its components Code of Professional Conduct, page 6 (Corruption, bribery, gifts, and entertainment), 8 (Seeking advice and raising concerns) Supplier Code of Conduct, page 3 (Anti-corruption, Gifts) 2018 Social Responsibility Report, page 52, 54, 55 Evaluation of the management approach 2018 Social Responsibility Report, page 52, 54, 55 Communication and training about anticorruption policies and procedures 2018 Social Responsibility Report, page 52, 54 Our anti-corruption policy is made available to all colleagues on our intranet. In addition we communicate important elements of our policy to our stakeholders publicly, via our Social Responsibility Report and our Code of Professional Conduct, which are available for download on McKinsey.com. Code of Professional Conduct, page 6 (Corruption, bribery, gifts, and entertainment), 8 (Seeking advice and raising concerns) McKinsey & Company 9 2018 GRI Index > Material Topics Environmental sustainability (Energy) Disclosure Description Reference or additional information 103-1 103-2 103-3 302-1 Explanation of the material topic and its Boundary See “Materiality” Participant Page, UN Global Compact Environmental Sustainability at McKinsey The management approach and its components 2018 Social Responsibility Report, pages 32-40; 34, 37 (GHG emissions, electricity) Code of Professional Conduct, page 5 (Environment) Supplier Code of Conduct, page 4 (Environment) Evaluation of the management approach 2018 Social Responsibility Report, pages 32-40 (footprint and outcomes) Energy consumption within the organization 2018 Social Responsibility Report, page 37 (transitioning to renewable electricity) Consumption of fuel: 56,717 MWh Consumption of purchased or acquired electricity: 53,180 MWh Consumption of purchased or acquired heat: 9,188 MWh Total: 119,085 MWh McKinsey & Company 10 2018 GRI Index > Material Topics Environmental sustainability (GHG Emissions) Disclosure 103-1 Description Explanation of the material topic and its Boundary Reference or additional information See “Materiality” Participant Page, UN Global Compact Environmental Sustainability at McKinsey 103-2 103-3 305-1 305-2 305-3 305-4 Since 2018, we have been carbon neutral. We are committed to reducing our greenhouse gas (GHG) emissions while offsetting all remaining emissions that we have not yet been able to eliminate. The management approach and its components 2018 Social Responsibility Report, pages 35-36, 38, 56 (GHG methodology) Code of Professional Conduct, page 5 (Environment) Supplier Code of Conduct, page 4 (Environment) Evaluation of the management approach 2018 Social Responsibility Report, pages 35-36, 38, 56-57 (Details on our Footprint) GHG Emissions (Scope 1) 2018 Social Responsibility Report, page 36 GHG Emissions (Scope 2) 2018 Social Responsibility Report, page 36 GHG Emissions (Scope 3) 2018 Social Responsibility Report, page 36 GHG emissions intensity 2018 Social Responsibility Report, page 37 Per capita GHG 305-5 GHG reduction 2018 Social Responsibility Report, page 37 Per capita reduction from 2017-18 was 11% McKinsey & Company 11 2018 GRI Index > Material Topics Employee development (1 of 2) Disclosure 103-1 Description Explanation of the material topic and its Boundary Reference or additional information See “Materiality” Participant Page, UN Global Compact About Us > Our Values (People) 103-2 103-3 The management approach and its components 2018 Social Responsibility Report, pages 6, 28 (Supporting our People) Code of Professional Conduct, page 4 (Creating a working environment that inspires and motivates our people) Evaluation of the management approach 2018 Social Responsibility Report, page 28, (Supporting our People) We’ve received several Awards and Recognitions related to employee development 401-2 Benefits 2018 Social Responsibility Report, page 26, 28 (Example parental leave, “other” programs) As noted in our Social Responsibility Report, our people are central to everything we do. We are committed to building a diverse firm, fostering an inclusive culture, and supporting our people in an unrivaled environment. This extends to the benefits we provide. Benefits provided to full-time employees include, among others i, life insurance; ii. health care; iii. disability and invalidity coverage; iv. parental leave; v. retirement provision; Others (e.g., Mind Matters, to support mental health and well-being). All (100%) of FTEs have access to relevant benefits according to their location, regional regulations, role and other factors. McKinsey & Company 12 2018 GRI Index > Material Topics Employee development (2 of 2) Disclosure Description Reference or additional information 404-1 Average hours of training per year per employee About Us > Overview As noted on our website, we invest more than $600 million of our firm’s resources annually in knowledge development, learning and capability building. A core learning journey exists for each role at McKinsey, comprised of in-person, virtual, and digital solutions. McKinsey colleagues also have access to elective, on-demand learning and participate in formal and informal learning events. In 2018, colleagues in client-facing roles participated in an average of 47 hours of firm-led training per year, while colleagues in internal roles participated in an average of 14 hours of training per year, in addition to training opportunities offered by individual locations and practices. Training participation does not vary materially by gender. 404-3 Percentage of employees receiving regular performance and career development reviews Feedback and learning is an important part of people development at McKinsey. With few exceptions (e.g., for Medical or Educational Leave), 100% of FTEs receive a regular performance and career development review, both ad hoc and formally (at least annually). McKinsey & Company 13 2018 GRI Index > Material Topics Diversity and inclusion Disclosure 103-1 Description Explanation of the material topic and its Boundary Reference or additional information See “Materiality” Participant Page, UN Global Compact. 103-2 103-3 405-1 Code of Professional Conduct, page 4 (Inclusion and Diversity) The management approach and its components 2018 Social Responsibility Report, pages 6, 23-28 (Fostering an Inclusive Environment) Code of Professional Conduct, page 4 (Creating a working environment that inspires and motivates our people) Supplier Code of Conduct, page 3 (Inclusion and Diversity) Evaluation of the management approach 2018 Social Responsibility Report, page 28, (Supporting our People) We’ve received several Awards and Recognitions related to employees and diversity. Diversity of governance bodies and employees About Us > Diversity and Inclusion McKinsey & Company 14 2018 GRI Index > Material Topics Human rights Disclosure 103-1 Description Explanation of the material topic and its Boundary Reference or additional information See “Materiality” Participant Page, UN Global Compact 103-2 103-3 412-12 The management approach and its components 2018 Social Responsibility Report, page 54 (Human Rights Policy) Code of Professional Conduct, page 5 (Human Rights), 8 (Seeking advice and raising concerns) Evaluation of the management approach 2018 Social Responsibility Report, page 54, (Human Rights) Supplier Code of Conduct, page 3 (Human Rights) Employee training on human rights policies or procedures 2018 Social Responsibility Report, page 52-54 All new hires are required to read McKinsey’s Code of Professional Conduct and participate in an in- person onboarding session where they learn about our values, which address key human rights issues such as inclusion, discrimination, and harassment. In addition, all McKinsey colleagues are required to review our core policies annually – including those related to human rights – and certify that they will comply with those policies. All McKinsey colleagues take a program of mandatory Professional Standards & Risk learning modules annually. And, each colleague’s core learning curriculum includes training on topics related to diversity and inclusion and personal conduct – such as unconscious bias, inclusive leadership, psychological safety, sponsoring diverse colleagues, mental health, sexual harassment and bullying. We do not have a breakdown of hours focused specifically on Human Rights training. McKinsey & Company 15 2018 GRI Index > Material Topics Community engagement Disclosure 103-1 Description Explanation of the material topic and its Boundary Reference or additional information See “Materiality” 2018 Social Responsibility Report, pages 40-50 (Partnerships) 103-2 103-3 413-1 The management approach and its components About Us: Social Responsibility 2018 Social Responsibility Report, pages 40-50 (Partnerships) Code of Professional Conduct, page 5 (Societal impact of our activities) Evaluation of the management approach Operations with local community engagement, impact assessments, and development programs 2018 Social Responsibility Report, page 40 (Partnerships) 2018 Social Responsibility Report, page 40-50, especially 42 (Supporting Nonprofits) McKinsey & Company 16 2018 GRI Index > Material Topics Supplier sustainability and diversity Disclosure 103-1 Description Explanation of the material topic and its Boundary Reference or additional information See “Materiality” Participant Page, UN Global Compact Supplier Code of Conduct, page 3 103-2 103-3 414-1 308-1 The management approach and its components 2018 Social Responsibility Report, page 57 (Working with Suppliers) Evaluation of the management approach 2018 Social Responsibility Report, page 57, (Working with Suppliers) Percentage of new suppliers that were screened using social criteria Percentage of new suppliers that were screened using environmental criteria 2018 Social Responsibility Report, page 57, (Working with Suppliers) As described in our 2018 Social Responsibility Report, all (100%) suppliers are expected to adhere to our Supplier Code of Conduct. We screen new suppliers for social criteria, including upholding human rights and operating workplaces free of discrimination. Also see our Modern Slavery Statement. 2018 Social Responsibility Report, page 57, (Working with Suppliers) As described in our 2018 Social Responsibility Report, all (100%) suppliers are expected to adhere to our Supplier Code of Conduct, which includes environmental criteria, including managing the environmental footprint. McKinsey & Company 17 2018 GRI Index > Material Topics Data privacy and security Disclosure Description Reference or additional information 103-1 103-2 103-3 418-1 Explanation of the material topic and its Boundary See “Materiality” 2018 Social Responsibility Report, page 55-56 The management approach and its components 2018 Social Responsibility Report, page 55-56 Code of Professional Conduct, page 3 (Client confidential information), page 4 (Our colleagues’ personal information), page 6 (Data security and protection) Supplier Code of Conduct, page 3 (Data Privacy and Security) Evaluation of the management approach 2018 Social Responsibility Report, page 55-56 Substantiated complaints concerning breaches of customer privacy and losses of customer data 2018 Social Responsibility Report, page 55-56 As described in our report, we have extensive practices in place to ensure data privacy. For example, systems and controls are designed to meet ISO/IEC 27001 standards, in combination with industry best practices. Our security operations center ensures a high level of information security for McKinsey’s applications (internal and external), systems, and all related data by providing best-in-class security- incident detection, analysis, containment, and mitigation. Complaints, if any, are considered confidential to our firm. McKinsey & Company 18"
# import string
# # print(stringtext.split())
# # print(string.punctuation)
# stringtext = ' '.join([st for st in stringtext.split() if st not in  string.punctuation])
# stringtext = re.sub(r'[^\w\s]','',stringtext)
# nlp = spacy.load("en_core_web_sm")
# doc = nlp(stringtext)
# All_rawData=[]
# Measures=[]
# Actions=[]
# Goals=[]
# for mj in nlp("s"):
#     print(mj.head.pos_)
# for token in doc:
#     # textdata = token.text+"=="+token.dep_+"=="+token.head.pos_
#     # All_rawData.append(textdata)
#     print(token.text, token.dep_, token.head.text, token.head.pos_,
#             [child for child in token.children])
#     if (token.dep_=="amod" and token.head.pos_=="NOUN") or (token.dep_=="compound" and token.head.pos_=="NOUN"):
#         for tok_data in nlp(str(token.text).lower()):
#             if tok_data.head.pos_=="ADJ":
#                 s=10
#                 # print(str(token.text).lower(),token.head.text,tok_data.head.pos_)
#         measure_kw = str(token.text).lower()+" "+str(token.head.text).lower()
#         print("++++++",measure_kw,token.dep_,token.head.pos_)
#         Measures.append(measure_kw)
#     elif token.dep_=="dobj" and token.head.pos_=="VERB":
#         action_kw =  obj_word_lemma.lemmatize(str(token.head.text).lower(),"v") +" "+str(token.text).lower()
#         Actions.append(action_kw)
#         chtok = [child for child in token.children]
#         if len(chtok)>=2:
#             for tok_goal in nlp(str(str(chtok[1]).strip().lower())):
#                 # print(tok_goal.head.pos_,chtok)
#                 if tok_goal.head.pos_ == "NOUN" or tok_goal.head.pos_ == "ADJ" or tok_goal.head.pos_ == "VERB":
#                     goas_text = str(token.head.text).lower()+" "+str(chtok[1]).strip().lower()+" "+str(token.text).lower()
#                     Goals.append(goas_text)
# #
# print(list(set(Measures)))
# print(list(set(Actions)))
# print(len(Goals),Goals)
#
#
# # for i in doc:
# #     if i.pos_ in ["NOUN", "PROPN"]:
# #         # comps = [j for j in i.children if j.dep_ == "compound" or j.dep_ =="amod"]
# #         comps = [j for j in i.children if j.pos_ in ["ADJ", "NOUN", "PROPN"]]
# #         if comps:
# #             print(comps, i)
#
# # print(get_compound_nn_adj(doc))
# import re
# s='▪ rights'
# import re
# # s = "string. With. Punctuation?"
# s = re.sub(r'[^\w\s]','',s)
# print(s)
#
#
#
#
#
#
#
#
#
#
#
# # doc = nlp("few")
# # for token in doc:
# #     print(token.text, token.dep_, token.head.text, token.head.pos_,
# #             [child for child in token.children])
#
# # print(All_rawData)
# # Measures=[]
# # Actions=[]
# # for i,checkner in enumerate(All_rawData):
# #     if "amod==NOUN" in checkner:
# #         st_text = str(checkner.split("==")[0]).strip().encode("utf8").decode("utf8")
# #         if "compound==NOUN" in All_rawData[i+1]:
# #             end_text = str(All_rawData[i+1].split("==")[0]).strip().encode("utf8").decode("utf8")
# #             print(checkner,All_rawData[i+1])
# #             print(st_text,end_text)
# #             full_text = st_text+" "+end_text
# #             Measures.append(full_text)
# #         if "dobj==VERB" in All_rawData[i+1]:
# #             print("----------",checkner,All_rawData[i+1])
# #             end_text = str(All_rawData[i + 1].split("==")[0]).strip().encode("utf8").decode("utf8")
# #             full_text = st_text + " " + end_text
# #             Actions.append(full_text)
# #
# # print(Measures,Actions)
#
# print([m.lower() for m in []])








# import spacy,random
# spacy.load('en_core_web_sm')
# from spacy.lang.en import English
# parser = English()
# def tokenize(text):
#     lda_tokens = []
#     tokens = parser(text)
#     for token in tokens:
#         if token.orth_.isspace():
#             continue
#         elif token.like_url:
#             lda_tokens.append('URL')
#         elif token.orth_.startswith('@'):
#             lda_tokens.append('SCREEN_NAME')
#         else:
#             lda_tokens.append(token.lower_)
#     return lda_tokens
#
#
# import nltk
#
# nltk.download('wordnet')
# from nltk.corpus import wordnet as wn
#
#
# def get_lemma(word):
#     lemma = wn.morphy(word)
#     if lemma is None:
#         return word
#     else:
#         return lemma
#
#
# from nltk.stem.wordnet import WordNetLemmatizer
#
#
# def get_lemma2(word):
#     return WordNetLemmatizer().lemmatize(word)
#
# nltk.download('stopwords')
# en_stop = set(nltk.corpus.stopwords.words('english'))
# def prepare_text_for_lda(text):
#     tokens = tokenize(text)
#     tokens = [token for token in tokens if len(token) > 4]
#     tokens = [token for token in tokens if token not in en_stop]
#     tokens = [get_lemma(token) for token in tokens]
#     return tokens
# text = "GRI Index 2018 Social Responsibility Report General Disclosures Disclosure 102-1 Description Name of the organization Reference or additional information McKinsey & Company 102-2 102-3 102-4 102-5 102-6 Activities, brands, products, and services Overview of McKinsey & Company Location of headquarters McKinsey Fact Sheet Location of operations Locations Ownership and legal form McKinsey Fact Sheet Markets served Locations: 130+ cities, 65+ countries Functions: 12 Business Functions Industries: 21 Industries 2018 GRI Index > General Disclosures McKinsey & Company 2 2018 GRI Index > General Disclosures General Disclosures Disclosure 102-7 Description Scale of the organization Reference or additional information McKinsey Fact Sheet In 2018, McKinsey had revenues in excess of $10 Billion. Change that Matters See also our Locations Functions Industries 102-8 Information on employees and other workers As noted in our McKinsey Fact Sheet, we have approximately 30,000 colleagues, our people speak more than 130 languages and represent over 130 citizenships. McKinsey offers colleagues multiple opportunities for flexibility. Nearly 8% of our colleagues (11% of consulting colleagues) worked part-time at least some period in 2018. In addition, we are supported by a pool of external workers globally, who provide specialized expertise or capacity on an as-needed basis. More information about women at McKinsey is also available on our website. 102-9 102-10 Supply Chain See “Engaging Stakeholders” Significant changes to the organization and its supply chain No significant changes in 2018 102-11 Precautionary Principle or approach We manage our business to reduce, avoid, or mitigate risks. 2018 Social Responsibility Report, Page 34 McKinsey & Company 3 General Disclosures Disclosure 102-12 Description External initiatives 102-13 External Memberships Reference or additional information 2018 Social Responsibility Report. About Us > Diversity About Us > Sustainability See also Functions Industries 2018 Social Responsibility Report. About Us > Diversity About Us > Sustainability 102-14 Statement from decision maker 2018 Social Responsibility Report, page 2 102-16 102-17 Values, principles, standards, and norms of behavior Mechanisms for advice and concerns about ethics (Statement from Kevin Sneader) Our mission and values Code of Professional Conduct Participant Page, UN Global Compact Code of Professional Conduct 2018 Social Responsibility Report Supplier Code of Conduct 102-18 Governance structure McKinsey Fact Sheet 2018 GRI Index > General Disclosures McKinsey & Company 4 2018 GRI Index > General Disclosures General Disclosures Disclosure 102-40 Description Stakeholder groups Reference or additional information See “Stakeholder Engagement” 102-41 Collective bargaining agreements About Us: Responsible Professional Practice (Human Rights) We support the protection of internationally proclaimed human rights and employees’ rights of freedom of association and to take part in collective-bargaining processes. This information is not tracked globally, as most colleagues are not covered by collective bargaining agreements. 102-42 102-43 102-44 102-45 102-46 Identifying and selecting stakeholders See “Stakeholder Engagement” Approach to stakeholder engagement See “Stakeholder Engagement” Key topics and concerns raised See “Stakeholder Engagement” Entities included in the consolidated financial statements Locations: 130+ cities, 65+ countries Our social responsibility reporting covers all locations of our firm globally. Defining report content and topic Boundaries See “Materiality” 102-47 List of material topics See “Materiality” McKinsey & Company 5 2018 GRI Index > General Disclosures General Disclosures Disclosure 102-48 Description Restatements of information Reference or additional information No restatements of information 102-49 Changes in reporting No significant changes 102-50 Date of Reporting January 1 – December 31, 2018 Financial Year 2018 102-51 102-52 102-53 102-54 102-55 102-56 Date of most recent report June 2019 Reporting cycle Annually Contact point for questions regarding the report McKinsey welcomes your comments and questions regarding this report. Please contact us at social_responsibility@mckinsey.com Claims of reporting in accordance with the GRI Standards This report has been prepared in accordance with the GRI Standards: Core option GRI Index This document. 2018 GRI Index. External assurance We conduct external assurance of our GHG emissions. 2018 Social Responsibility Report, page 36 McKinsey & Company 6 Engaging Stakeholders (102-40, 42, 43, 44) 2018 GRI Index > General Disclosures The way in which we run our firm and the decisions that we make affect a range of stakeholders, including our clients, our people (current, alumni and future colleagues ), and our suppliers, as well as our communities. We regularly use information from various touchpoints with these stakeholders to inform our goals, shape our practices, and refine our reporting. For example: Our clients. We communicate regularly with clients through formal and informal feedback, and analyze client requests for proposals (RFPs) and other inquiries (e.g., via third-party platforms and assessments) to identify trends in the issues that are most important to them. Our people. We draw on multiple formal and informal touchpoints to incorporate the insights of current, future and previous colleagues, including surveys and town halls. We also have dedicated teams for recruiting and alumni relations who incorporate feedback from both groups. Our suppliers. We have dedicated supplier management teams who work with and hear directly from suppliers, including which procurement and other practices matter to them.  Our Supplier Code of Conduct sets out our expectations of suppliers, and via the supporting Environmental, Social, and Governance (ESG) questionnaire we gain insights into their practices, which in turn informs our activities related to procurement and supplier management. Our communities.  McKinsey has offices in over 130 cities. These communities are home to our clients and their stakeholders, and to our own people and their families. We want them to be inclusive, sustainable, and prosperous places to live and to work. We support 600+ nonprofits per year, and our interactions with them provide valuable insights into the needs of our communities from multiple perspectives. Civil society, multi-sector organizations. Our practices and reporting are informed and shaped by our participation in initiatives such as the UN Global Compact and RE100, a coalition of businesses dedicated to purchasing 100% renewable electricity by 2025. McKinsey & Company 7 Materiality (102-46, 102-47) 2018 GRI Index > General Disclosures McKinsey has a long-standing commitment to social responsibility. We recognize we have an opportunity, and a responsibility, to use our knowledge and our capabilities to help address the world’s most pressing issues, including environmental and social issues. In 2018, McKinsey became a participant in the United Nations Global Compact (UNGC), a set of ten principles covering the areas of human rights, labor, environment, and anti-corruption. Our firm’s values, the social impact we seek to achieve, and voices of our diverse stakeholders inform the material topics upon which our social responsibility strategy and reporting are based. To identify our material topics, we held working sessions with internal stakeholders, performed benchmarking across peers, referenced the Sustainability Accounting Standards Board (SASB) for the professional services sector, and incorporated external perspectives through a variety of touchpoints (e.g., client requests regarding our social responsibility practices;  requirements of third party social responsibility and sustainability assessments). The priority topics we identified include ▪ Ethics (anti-corruption) ▪ Community engagement ▪ Environmental sustainability ▪ Human rights ▪ Employee development ▪ Diversity and inclusion ▪ Supplier sustainability and diversity ▪ Data privacy and security McKinsey & Company 8 2018 GRI Index > Material Topics Ethics (anti-corruption) Disclosure Description Reference or additional information 103-1 103-2 103-3 205-2 Explanation of the material topic and its Boundary See “Materiality” Participant Page, UN Global Compact About Us: Responsible Professional Practice (anti-corruption) The management approach and its components Code of Professional Conduct, page 6 (Corruption, bribery, gifts, and entertainment), 8 (Seeking advice and raising concerns) Supplier Code of Conduct, page 3 (Anti-corruption, Gifts) 2018 Social Responsibility Report, page 52, 54, 55 Evaluation of the management approach 2018 Social Responsibility Report, page 52, 54, 55 Communication and training about anticorruption policies and procedures 2018 Social Responsibility Report, page 52, 54 Our anti-corruption policy is made available to all colleagues on our intranet. In addition we communicate important elements of our policy to our stakeholders publicly, via our Social Responsibility Report and our Code of Professional Conduct, which are available for download on McKinsey.com. Code of Professional Conduct, page 6 (Corruption, bribery, gifts, and entertainment), 8 (Seeking advice and raising concerns) McKinsey & Company 9 2018 GRI Index > Material Topics Environmental sustainability (Energy) Disclosure Description Reference or additional information 103-1 103-2 103-3 302-1 Explanation of the material topic and its Boundary See “Materiality” Participant Page, UN Global Compact Environmental Sustainability at McKinsey The management approach and its components 2018 Social Responsibility Report, pages 32-40; 34, 37 (GHG emissions, electricity) Code of Professional Conduct, page 5 (Environment) Supplier Code of Conduct, page 4 (Environment) Evaluation of the management approach 2018 Social Responsibility Report, pages 32-40 (footprint and outcomes) Energy consumption within the organization 2018 Social Responsibility Report, page 37 (transitioning to renewable electricity) Consumption of fuel: 56,717 MWh Consumption of purchased or acquired electricity: 53,180 MWh Consumption of purchased or acquired heat: 9,188 MWh Total: 119,085 MWh McKinsey & Company 10 2018 GRI Index > Material Topics Environmental sustainability (GHG Emissions) Disclosure 103-1 Description Explanation of the material topic and its Boundary Reference or additional information See “Materiality” Participant Page, UN Global Compact Environmental Sustainability at McKinsey 103-2 103-3 305-1 305-2 305-3 305-4 Since 2018, we have been carbon neutral. We are committed to reducing our greenhouse gas (GHG) emissions while offsetting all remaining emissions that we have not yet been able to eliminate. The management approach and its components 2018 Social Responsibility Report, pages 35-36, 38, 56 (GHG methodology) Code of Professional Conduct, page 5 (Environment) Supplier Code of Conduct, page 4 (Environment) Evaluation of the management approach 2018 Social Responsibility Report, pages 35-36, 38, 56-57 (Details on our Footprint) GHG Emissions (Scope 1) 2018 Social Responsibility Report, page 36 GHG Emissions (Scope 2) 2018 Social Responsibility Report, page 36 GHG Emissions (Scope 3) 2018 Social Responsibility Report, page 36 GHG emissions intensity 2018 Social Responsibility Report, page 37 Per capita GHG 305-5 GHG reduction 2018 Social Responsibility Report, page 37 Per capita reduction from 2017-18 was 11% McKinsey & Company 11 2018 GRI Index > Material Topics Employee development (1 of 2) Disclosure 103-1 Description Explanation of the material topic and its Boundary Reference or additional information See “Materiality” Participant Page, UN Global Compact About Us > Our Values (People) 103-2 103-3 The management approach and its components 2018 Social Responsibility Report, pages 6, 28 (Supporting our People) Code of Professional Conduct, page 4 (Creating a working environment that inspires and motivates our people) Evaluation of the management approach 2018 Social Responsibility Report, page 28, (Supporting our People) We’ve received several Awards and Recognitions related to employee development 401-2 Benefits 2018 Social Responsibility Report, page 26, 28 (Example parental leave, “other” programs) As noted in our Social Responsibility Report, our people are central to everything we do. We are committed to building a diverse firm, fostering an inclusive culture, and supporting our people in an unrivaled environment. This extends to the benefits we provide. Benefits provided to full-time employees include, among others i, life insurance; ii. health care; iii. disability and invalidity coverage; iv. parental leave; v. retirement provision; Others (e.g., Mind Matters, to support mental health and well-being). All (100%) of FTEs have access to relevant benefits according to their location, regional regulations, role and other factors. McKinsey & Company 12 2018 GRI Index > Material Topics Employee development (2 of 2) Disclosure Description Reference or additional information 404-1 Average hours of training per year per employee About Us > Overview As noted on our website, we invest more than $600 million of our firm’s resources annually in knowledge development, learning and capability building. A core learning journey exists for each role at McKinsey, comprised of in-person, virtual, and digital solutions. McKinsey colleagues also have access to elective, on-demand learning and participate in formal and informal learning events. In 2018, colleagues in client-facing roles participated in an average of 47 hours of firm-led training per year, while colleagues in internal roles participated in an average of 14 hours of training per year, in addition to training opportunities offered by individual locations and practices. Training participation does not vary materially by gender. 404-3 Percentage of employees receiving regular performance and career development reviews Feedback and learning is an important part of people development at McKinsey. With few exceptions (e.g., for Medical or Educational Leave), 100% of FTEs receive a regular performance and career development review, both ad hoc and formally (at least annually). McKinsey & Company 13 2018 GRI Index > Material Topics Diversity and inclusion Disclosure 103-1 Description Explanation of the material topic and its Boundary Reference or additional information See “Materiality” Participant Page, UN Global Compact. 103-2 103-3 405-1 Code of Professional Conduct, page 4 (Inclusion and Diversity) The management approach and its components 2018 Social Responsibility Report, pages 6, 23-28 (Fostering an Inclusive Environment) Code of Professional Conduct, page 4 (Creating a working environment that inspires and motivates our people) Supplier Code of Conduct, page 3 (Inclusion and Diversity) Evaluation of the management approach 2018 Social Responsibility Report, page 28, (Supporting our People) We’ve received several Awards and Recognitions related to employees and diversity. Diversity of governance bodies and employees About Us > Diversity and Inclusion McKinsey & Company 14 2018 GRI Index > Material Topics Human rights Disclosure 103-1 Description Explanation of the material topic and its Boundary Reference or additional information See “Materiality” Participant Page, UN Global Compact 103-2 103-3 412-12 The management approach and its components 2018 Social Responsibility Report, page 54 (Human Rights Policy) Code of Professional Conduct, page 5 (Human Rights), 8 (Seeking advice and raising concerns) Evaluation of the management approach 2018 Social Responsibility Report, page 54, (Human Rights) Supplier Code of Conduct, page 3 (Human Rights) Employee training on human rights policies or procedures 2018 Social Responsibility Report, page 52-54 All new hires are required to read McKinsey’s Code of Professional Conduct and participate in an in- person onboarding session where they learn about our values, which address key human rights issues such as inclusion, discrimination, and harassment. In addition, all McKinsey colleagues are required to review our core policies annually – including those related to human rights – and certify that they will comply with those policies. All McKinsey colleagues take a program of mandatory Professional Standards & Risk learning modules annually. And, each colleague’s core learning curriculum includes training on topics related to diversity and inclusion and personal conduct – such as unconscious bias, inclusive leadership, psychological safety, sponsoring diverse colleagues, mental health, sexual harassment and bullying. We do not have a breakdown of hours focused specifically on Human Rights training. McKinsey & Company 15 2018 GRI Index > Material Topics Community engagement Disclosure 103-1 Description Explanation of the material topic and its Boundary Reference or additional information See “Materiality” 2018 Social Responsibility Report, pages 40-50 (Partnerships) 103-2 103-3 413-1 The management approach and its components About Us: Social Responsibility 2018 Social Responsibility Report, pages 40-50 (Partnerships) Code of Professional Conduct, page 5 (Societal impact of our activities) Evaluation of the management approach Operations with local community engagement, impact assessments, and development programs 2018 Social Responsibility Report, page 40 (Partnerships) 2018 Social Responsibility Report, page 40-50, especially 42 (Supporting Nonprofits) McKinsey & Company 16 2018 GRI Index > Material Topics Supplier sustainability and diversity Disclosure 103-1 Description Explanation of the material topic and its Boundary Reference or additional information See “Materiality” Participant Page, UN Global Compact Supplier Code of Conduct, page 3 103-2 103-3 414-1 308-1 The management approach and its components 2018 Social Responsibility Report, page 57 (Working with Suppliers) Evaluation of the management approach 2018 Social Responsibility Report, page 57, (Working with Suppliers) Percentage of new suppliers that were screened using social criteria Percentage of new suppliers that were screened using environmental criteria 2018 Social Responsibility Report, page 57, (Working with Suppliers) As described in our 2018 Social Responsibility Report, all (100%) suppliers are expected to adhere to our Supplier Code of Conduct. We screen new suppliers for social criteria, including upholding human rights and operating workplaces free of discrimination. Also see our Modern Slavery Statement. 2018 Social Responsibility Report, page 57, (Working with Suppliers) As described in our 2018 Social Responsibility Report, all (100%) suppliers are expected to adhere to our Supplier Code of Conduct, which includes environmental criteria, including managing the environmental footprint. McKinsey & Company 17 2018 GRI Index > Material Topics Data privacy and security Disclosure Description Reference or additional information 103-1 103-2 103-3 418-1 Explanation of the material topic and its Boundary See “Materiality” 2018 Social Responsibility Report, page 55-56 The management approach and its components 2018 Social Responsibility Report, page 55-56 Code of Professional Conduct, page 3 (Client confidential information), page 4 (Our colleagues’ personal information), page 6 (Data security and protection) Supplier Code of Conduct, page 3 (Data Privacy and Security) Evaluation of the management approach 2018 Social Responsibility Report, page 55-56 Substantiated complaints concerning breaches of customer privacy and losses of customer data 2018 Social Responsibility Report, page 55-56 As described in our report, we have extensive practices in place to ensure data privacy. For example, systems and controls are designed to meet ISO/IEC 27001 standards, in combination with industry best practices. Our security operations center ensures a high level of information security for McKinsey’s applications (internal and external), systems, and all related data by providing best-in-class security- incident detection, analysis, containment, and mitigation. Complaints, if any, are considered confidential to our firm. McKinsey & Company 18"
# text=nltk.sent_tokenize(text)
# print(len(text))
# text_data = []
# import random
# text_data = []
# for line in text:
#     tokens = prepare_text_for_lda(line)
#     # print(tokens)
#     text_data.append(tokens)
# print(text_data)
#
# from gensim import corpora
# dictionary = corpora.Dictionary(text_data)
# corpus = [dictionary.doc2bow(text) for text in text_data]
# import pickle
# pickle.dump(corpus, open('corpus.pkl', 'wb'))
# dictionary.save('dictionary.gensim')
#
# import gensim
# # NUM_TOPICS = 5
# # ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics = NUM_TOPICS, id2word=dictionary, passes=15)
# # ldamodel.save('model5.gensim')
# # topics = ldamodel.print_topics(num_words=4)
# # for topic in topics:
# #     print(topic)
#
# ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics = 10, id2word=dictionary, passes=15)
# ldamodel.save('model10.gensim')
# topics = ldamodel.print_topics(num_words=5)
# for topic in topics:
#     print(topic)
# # dictionary = gensim.corpora.Dictionary.load('dictionary.gensim')
# # corpus = pickle.load(open('corpus.pkl', 'rb'))
# # lda = gensim.models.ldamodel.LdaModel.load('model5.gensim')
# # import pyLDAvis.gensim
# # lda_display = pyLDAvis.gensim.prepare(lda, corpus, dictionary, sort_topics=False)
# # pyLDAvis.display(lda_display)
#

# -*-coding:utf-8 -*-
import os
from datetime import datetime
import ast, json
import sys, time
import requests
from datetime import datetime

# reload(sys)
# sys.setdefaultencoding('utf8')

class RecruiterLogin:
    def __init__(self):
        self.headers = {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; WOW64)\
                AppleWebKit/537.36 (KHTML'}
        self.url = "https://recruiter.monsterindia.com/v2/login.html?src=https://recruiter.monsterindia.com/v2/employer_pg.html&msgflag=0&rand=3980"

    def login(self):
        payload = 'srcUrl=https%3A%2F%2Frecruiter.monsterindia.com%2Fv2%2Femployer_pg.html&submit1=1&referrer=&rn=16119105171611910517161191051716119105171611910517161191051716119105171611910517&login=monsterinternaldtest&passwd=7522RmmidZ&x=34&y=13'
        session = requests.session()
        response = session.request("POST", self.url, data=payload)
        st_time = time.time()
        resp = session.get("https://recruiter.monsterindia.com/v2/resumedatabase/searchresult.html",headers=self.headers)
        # loadtime = session.get("https://recruiter.monsterindia.com/v2/resumedatabase/searchresult.html",headers=self.headers).elapsed.total_seconds()
        # print loadtime
        end_time = time.time()
        loadtime=end_time-st_time
        print ("loadtime :: ",loadtime)
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),"loadtime.txt")
        f=open(file_path,"w")
        f.write(str(loadtime))
        f.close()

if __name__=="__main__":
    obj=RecruiterLogin()
    obj.login()


