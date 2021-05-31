# -*- coding: utf-8 -*-

from PIL import Image, ImageEnhance, ImageFilter
import pytesseract
import PyPDF2, struct, time
import codecs, uuid, os, sys, warnings

warnings.filterwarnings("ignore")


class ExtractImageText:
    def __init__(self):
        self.a = 1

    def next_image_pdf_data(self, pdf_pa):
        pdf = open(pdf_pa, "r",encoding='latin-1')
        pdf = bytes(pdf.read().encode("utf8").decode("utf8"))
        print(pdf)
        startmark = "\xff\xd8"
        startfix = 0
        endmark = "\xff\xd9"
        endfix = 2
        i = 0
        njpg = 0
        text_data = ""
        while True:
            try:
                istream = pdf.find("stream", i)
                if istream < 0:
                    break
                istart = pdf.find(startmark, istream, istream + 20)
                if istart < 0:
                    i = istream + 20
                    continue
                iend = pdf.find("endstream", istart)
                if iend < 0:
                    raise Exception("Didn't find end of stream!")
                iend = pdf.find(endmark, iend - 20)
                if iend < 0:
                    raise Exception("Didn't find end of JPG!")

                istart += startfix
                iend += endfix
                # print "JPG %d from %d to %d" % (njpg, istart, iend)
                jpg = pdf[istart:iend]
                fi = str(uuid.uuid1())[2:8] + "jpg%d.png" % njpg
                # temp_pdf_path = create_directory("PDF", '') + fi
                jpgfile = open(fi, "wb")
                jpgfile.write(jpg)

                jpgfile.close()
                # path_fi = os.path.join(os.getcwd(), fi).replace("\\","/")
                pytesseract.pytesseract.tesseract_cmd = 'C:/Program Files/Tesseract-OCR/tesseract'
                data = pytesseract.image_to_string(Image.open(fi))
                main_data = ' '.join(data.split())
                text_data = (str(text_data) + str(main_data)).encode("utf8")
                njpg += 1
                i = iend
                # time.sleep(1)
                os.remove(fi)
            except Exception as e:
                print('===------->>>>', e)
                break
                pass
        return str(text_data)


if __name__ == "__main__":
    obj = ExtractImageText()
    pdf_path = "E:\\PycharmProjects\\pdf_keyword_extraction\\venv\\src\\downloads\\Image_065.pdf"
    # doc_text = extract_pdf_image_text(pdf_path)
    doc_text = obj.next_image_pdf_data(pdf_path)
    print(doc_text)
