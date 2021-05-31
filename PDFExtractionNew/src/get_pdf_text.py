# -*- coding: utf-8 -*-

from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfpage import PDFTextExtractionNotAllowed
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfinterp import resolve1
from pdfminer.layout import LAParams
from pdfminer.converter import TextConverter
try:
    from StringIO import StringIO ## for Python 2
except ImportError:
    from io import StringIO ## for Python 3


class MyParser(object):
    def __init__(self, pdf):
        self.records = []
        # Create the document model from the file
        parser = PDFParser(open(pdf, 'rb'))
        document = PDFDocument(parser)
        # Try to parse the document
        # if not document.is_extractable:
        #     raise PDFTextExtractionNotAllowed
        # Create a PDF resource manager object
        # that stores shared resources.
        rsrcmgr = PDFResourceManager()
        # Create a buffer for the parsed text
        retstr = StringIO()
        # Spacing parameters for parsing
        laparams = LAParams()
        codec = 'utf-8'

        # Create a PDF device object
        device = TextConverter(rsrcmgr, retstr,
                               codec=codec,
                               laparams=laparams)
        # Create a PDF interpreter object
        #         import pdb;pdb.set_trace()
        if resolve1(document.catalog['Pages'])['Count'] < 1000:
            interpreter = PDFPageInterpreter(rsrcmgr, device)
            # Process each page contained in the document.
            m=0
            for page in PDFPage.create_pages(document):
                try:
                    if m>100:
                        break
                    interpreter.process_page(page)
                except:
                    pass
                m+=1
            lines = str(retstr.getvalue()).splitlines()
            for line in lines:
                self.handle_line(line)

    def handle_line(self, line):
        # Customize your line-by-line parser here
        self.records.append(line)
if __name__=="__main__":
    pdf_path = "E:\\PycharmProjects\\PDFExtractionNew\\venv\\src\\downloads\\From-surviving-to-thriving-Reimagining-the-post-COVID-19-return.pdf"
    pdf_path = "E:\\PycharmProjects\\PDFExtractionNew\\venv\\src\\downloads\\practice-test-C.pdf"
    pdf_path ="C:\\Users\\vktiwari\\Downloads\\COVID-19-Facts-and-Insights-June-1-vF.pdf"
    pdf_path = "C:\\Users\\vktiwari\\Downloads\\Article0.pdf"
    pdf_path = "C:\\Users\\vktiwari\\Downloads\\Article1.pdf"
    pdf_path = "C:\\Users\\vktiwari\\Downloads\\211th_Economic Indicators.pdf"
    pdf_path = "C:\\Users\\vktiwari\\Downloads\\1969_Medicare_AdvantageLEK_Executive_Insights_1.pdf"
    p_doc = MyParser(pdf_path)
    doc_text = ' '.join([k.strip() for k in p_doc.records if k])
    print (doc_text)