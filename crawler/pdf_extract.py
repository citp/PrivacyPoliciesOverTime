import sys
import pdftotext

try:
    # these modules are only used for benchmarking
    import textract
    import PyPDF2
    from tika import parser
except ImportError:
    pass

from os.path import isfile


def extract_text_from_pdf_by_tika(pdf_path):
    parsed = parser.from_file(pdf_path)
    return "" if parsed['content'] is None else parsed['content'] 


def extract_text_from_pdf_by_textract(pdf_path):
    return str(textract.process(pdf_path)).replace("\\n", "\n")


def extract_text_from_pdf_by_pypdf2(pdf_path):
    txt = ""
    pdf_file = open(pdf_path, 'rb')
    pdf_reader = PyPDF2.PdfFileReader(pdf_file)
    for page_no in range(pdf_reader.numPages):
        page = pdf_reader.getPage(page_no)
        txt += page.extractText() + "\n"
    return txt


def extract_text_from_pdf_by_pdftotext(pdf_path):
    # Load your PDF
    if not isfile(pdf_path):
        return None

    with open(pdf_path, "rb") as f:
        txt = pdftotext.PDF(f)
        return ("\n\n".join(txt))

def extract_pdf_txt_into_file(in_file, out_file):
    txt = extract_text_from_pdf_by_pdftotext(in_file)
    if txt is None:
        return
    with open(out_file, "w") as f:
        f.write(txt)


if __name__ == '__main__':
    if len(sys.argv) > 2:
        in_file = sys.argv[1]
        out_file = sys.argv[2]
        extract_pdf_txt_into_file(in_file, out_file)
    else:
        print("Usage: python3 pdf_extract.py in.pdf out.txt")

