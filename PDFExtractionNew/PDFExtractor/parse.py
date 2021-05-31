# -*- coding:utf-8 -*-

import re
import string
import time,sys
# reload(sys)
# sys.setdefaultencoding('utf8')


class Parse():
    def __init__(self):
        self.source=None
        self.printable = set(string.printable)

    def parse(self,source):
        '''
        '''
        self.source=source
        ########remove unicodes and non printable characters
        self.source = re.sub(r'[^\x00-\x7F]',' ',self.source)
        ######## removing all non printable characters
        self.source = ''.join(filter(lambda x:x in self.printable, self.source))
        ####### remove white space from the beginning of the line
        self.source= re.sub(r'^( +)',r' ',self.source,flags=re.IGNORECASE|re.MULTILINE)
        ####### remove lines containing number followed by dot(.) that appearing in the text
        self.source = re.sub(r'(^\s*\d+.\r*\n)|^( +)',r' ',self.source,flags=re.IGNORECASE|re.MULTILINE)
        ###### removing one or more whitespaces from the begining and end of each line
        self.source = re.sub(r'(^( +)|( +)$)',r' ',self.source,flags=re.IGNORECASE|re.MULTILINE)
        ###### removing all jpeg embedded in document
        self.source = re.sub(r'((\!\[\]\(data:image\/).*\r*\n)',r' ',self.source,flags=re.IGNORECASE|re.MULTILINE)
        ###### remove any blank images embedded in document
        self.source = re.sub(r'((\!\[\]\(data:\)))',r' ',self.source,flags=re.IGNORECASE|re.MULTILINE)
        ###### remove starting digits and '#' symbols
        # self.source = re.sub(r'(^\s*[^a-zA-Z]+\s*)',r' ',self.source,flags=re.IGNORECASE|re.MULTILINE)
        ###### remove one or more whitespaces from the beginning and end of each line
        self.source = re.sub(r'(^( +)|( +)$)',r' ',self.source,flags=re.IGNORECASE|re.MULTILINE)
        ###### remove multiple new lines
        self.source = re.sub(r'(\n+)',r'\n',self.source,flags=re.IGNORECASE|re.MULTILINE)
        ############ remove whitespace with single space
        self.source = re.sub(' +',' ',self.source,flags=re.IGNORECASE|re.MULTILINE)
        ######## remove line containg numbers followed by dot(.)
        self.source = re.sub(r'(^\s*)(\d+.)(\r*\n)',r' ',self.source,flags=re.IGNORECASE|re.MULTILINE)
        self.source = re.sub(r'&#x22;',r' ',self.source,flags=re.IGNORECASE|re.MULTILINE)
        self.source = re.sub(r'x22;', r' ', self.source, flags=re.IGNORECASE | re.MULTILINE)
        self.source = re.sub(r'&#x22;', r' ', self.source, flags=re.IGNORECASE | re.MULTILINE)
        self.source = re.sub(r'&#x27;', r' ', self.source, flags=re.IGNORECASE | re.MULTILINE)
        self.source = re.sub(r'x27;', r' ', self.source, flags=re.IGNORECASE | re.MULTILINE)
        self.source = re.sub(r'\\\\', r' ', self.source, flags=re.IGNORECASE | re.MULTILINE)
        self.source = re.sub(r'//', r' ', self.source, flags=re.IGNORECASE | re.MULTILINE)
        junk_list = ['xa4', 'xa4', 'xa7', 'xe6', 'xa9', 'xa6', 'xa8', 'u201a', 'u02dc', 'xbf', 'xba', 'u02dc', 'u2014',
                     'u2026', 'u2020', 'xbc', 'xef', 'u2022', 'u2122', 'xe5', 'xbd', 'x9d', 'xe7', 'xe2', 'xb8', 'xe8',
                     'u017d', 'xb4', 'xae', 'x90', 'xbb', 'xa5', 'u0192', 'xa0', 'u2018', 'xe9', 'u201c', 'xb5',
                     'u2013', 'xb6', 'u203a', 'u20ac', 'xb7', 'u0153', 'xb0', 'u201e', 'xc3', 'xaf', 'xa1', 'xe4',
                     'xad', 'xa2', 'xc2', 'xb9', 'u2026 - ']
        for j in junk_list:
            self.source = re.sub(j, r' ', self.source, flags=re.IGNORECASE | re.MULTILINE)
        self.source = re.sub(j, r' ', self.source, flags=re.IGNORECASE | re.MULTILINE)

        return self.source

    def getdata(self):
        return  self.source


# if __name__=='__main__':
#     filepath = r"jobs_monster_skills"
#     obj = Parse()
#     data = obj.parse("Archangel School, Kullu, Himachal Pradesh A rch a ngel School All rights reserved © Archangel School, Ragunathpur, Kullu (Himachal Pradesh) For God and Country Contact Us: School’s Postal Address: Archangel School, Raghunathpur, KULLU Himachal Pradesh (India) Pic Code: 175101 Phone: 01902-225566 Email: archangelschool (a) outlook.in Quick Links: Activities Photo Gallery Chairman's Message Principal's Message Vision and Mission School History Management and Teachers Location of School".encode("ascii",'ignore'))
#     print (data)
    # import ast
    # data=ast.literal_eval(open(filepath,"r").read())
    # newlist=[]
    # for d in data:
    #     newlist.append(obj.parse(d))
    # with open("jobs_monster_skills3", "w")as fileobj:
    #     fileobj.write(str(newlist))

# import spacy, random
#
# nlp = spacy.load("en_core_web_lg")
# doc = nlp(u"SOFTWARE ENGINEER 500")
# print doc.ents
# for k in doc.ents:
#     print k.text, k.label_, k.start_char, k.end_char