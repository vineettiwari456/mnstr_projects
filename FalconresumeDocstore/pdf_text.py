# -*-coding:utf-8 -*-

import sys
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.converter import XMLConverter, HTMLConverter, TextConverter
from pdfminer.layout import LAParams
import io
class PdfParser:
    def __init__(self):
        self.a= 1
    def pdfparser(self,data):

        fp = open(data, 'rb')
        rsrcmgr = PDFResourceManager()
        retstr = io.StringIO()
        codec = 'utf-8'
        laparams = LAParams()
        device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
        # Create a PDF interpreter object.
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        # Process each page contained in the document.

        for page in PDFPage.get_pages(fp):
            interpreter.process_page(page)
            data =  retstr.getvalue()

        return data


if __name__ == '__main__':
    path = "E:/PycharmProjects/NltkTest/venv/src/falconresume_final/resumes_download/84345442_1577691387269.pdf"
    obj_pdf= PdfParser()
    data = obj_pdf.pdfparser(path)
    print(data)
    # fd = open('destintaion_path.text', 'w',encoding="utf-8")
    # fd.write(data)
    # fd.close()


# print (convert_pdf_to_txt("E:\\PycharmProjects\\NltkTest\\venv\\src\\falconresume\\resumes_download\\65196735.pdf"))