import os
import re
import json
import pdfplumber
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from datetime import datetime
from pdf2image import convert_from_path
from PIL import ImageOps
import pytesseract

SOURCE_PDF_DIR = "../k3s.dranspo.se:8000/data/CVG-20-00069"
WORK_DIR = "/tmp/cvg_pipeline"
TEXT_DIR = "ocr/raw_text"
PARSED_DIR = os.path.join(WORK_DIR, "parsed")
OCR_TEXTS_DIR = "ocr/texts"

os.makedirs(TEXT_DIR, exist_ok=True)
os.makedirs(PARSED_DIR, exist_ok=True)
os.makedirs(OCR_TEXTS_DIR, exist_ok=True)

try:
    nltk.download("punkt_tab", quiet=True)
    nltk.download("stopwords", quiet=True)
except Exception as e:
    print(f"warning: could not download nltk data: {e}")

try:
    STOP_WORDS = set(stopwords.words("english"))
except:
    STOP_WORDS = set()

# find local pdf files in the source directory
def discover_local_pdf_files(pdf_dir):
    # scan directory and list all pdf files
    try:
        if not os.path.exists(pdf_dir):
            print(f"error: directory not found: {pdf_dir}")
            return []
        
        print(f"scanning for pdfs in {pdf_dir}...")
        pdf_files = [f for f in os.listdir(pdf_dir) if f.lower().endswith(".pdf")]
        
        if pdf_files:
            print(f"found {len(pdf_files)} pdf(s):")
            for pdf in pdf_files:
                print(f"  - {pdf}")
        else:
            print("no pdfs found in directory.")
        
        return pdf_files
    except Exception as e:
        print(f"error discovering pdfs: {e}")
        return []

# choose the largest pdf file to process (assuming it has the most content)
# select the largest pdf by file size
def select_largest_pdf(pdf_dir, pdf_files):
    # pick the largest PDF by file size
    if not pdf_files:
        return None

    largest_pdf = None
    largest_size = -1
    for pdf in pdf_files:
        pdf_path = os.path.join(pdf_dir, pdf)
        try:
            size = os.path.getsize(pdf_path)
        except Exception as e:
            print(f"warning: could not size {pdf_path}: {e}")
            continue

        if size > largest_size:
            largest_size = size
            largest_pdf = pdf

    if largest_pdf:
        print(f"largest pdf selected: {largest_pdf} ({largest_size} bytes)")

    return largest_pdf

pdf_files = discover_local_pdf_files(SOURCE_PDF_DIR)
largest_pdf = select_largest_pdf(SOURCE_PDF_DIR, pdf_files)
pdf_paths = [os.path.join(SOURCE_PDF_DIR, largest_pdf)] if largest_pdf else []

print("\n--- using local pdf files ---")
if not pdf_paths:
    print("no pdfs found to process.")

# extract text directly from a pdf
def extract_text_from_pdf(pdf_path):
    # extract text directly from pdf using pdfplumber
    try:
        text_output = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    text_output.append(text)
        return "\n".join(text_output) if text_output else ""
    except Exception as e:
        print(f"error extracting text from {pdf_path}: {e}")
        return ""

# score ocr text by readable word count
def score_ocr_text(text):
    # heuristic: favor outputs with more readable words
    words = re.findall(r"[A-Za-z]{3,}", text)
    return len(words)

# pick the best ocr result across rotations and mirroring
def ocr_best_for_page(page):
    # try rotations and mirroring; keep the most legible OCR output
    candidates = []
    for angle in (0, 90, 180, 270):
        rotated = page.rotate(angle, expand=True) if angle else page
        candidates.append(rotated)
        candidates.append(ImageOps.mirror(rotated))

    best_text = ""
    best_score = -1
    for image in candidates:
        text = pytesseract.image_to_string(image)
        score = score_ocr_text(text)
        if score > best_score:
            best_score = score
            best_text = text
    return best_text

# run ocr on selected pages and add page markers
def ocr_pdf_fallback(pdf_path):
    # use tesseract ocr to extract text from scanned pdf images
    try:
        print(f"    (trying OCR with rotation/mirroring...)")
        pages = convert_from_path(pdf_path, dpi=300)
        # keep header (first 3) and footer (last 3) pages to reduce noise
        if len(pages) > 6:
            pages = pages[:3] + pages[-3:]
        text_output = []
        for i, page in enumerate(pages):
            print(f"      ocr page {i+1}/{len(pages)}...")
            text = ocr_best_for_page(page)
            if text:
                text_output.append(text)
            page_marker = f"\n<<< OCR_PAGE_{i+1} >>>\n"
            text_output.append(page_marker)
        return "\n".join(text_output) if text_output else ""
    except Exception as e:
        print(f"    error ocring {pdf_path}: {e}")
        return ""

# try direct extraction, then fallback to ocr
def extract_text_hybrid(pdf_path):
    # try direct extraction first, fallback to ocr if text is minimal
    text = extract_text_from_pdf(pdf_path)
    if len(text) < 50:
        text = ocr_pdf_fallback(pdf_path)
    return text

# parse minimal case data for backup
def parse_case_data(text, filename):
    source_dir_name = os.path.basename(os.path.normpath(SOURCE_PDF_DIR))
    return {
        "case_number": source_dir_name,
        "raw_text": text
    }

# deprecated keyword extraction placeholder
def extract_keywords(text, min_len=4):
    # deprecated - no longer using keyword extraction
    return []

print("\n--- Processing PDFs ---")
records = []
for pdf in pdf_paths:
    filename = os.path.basename(pdf)
    print(f"Processing {filename}...")
    # use ocr to extract full text from the largest PDF
    text = ocr_pdf_fallback(pdf)

    if not text:
        # fallback to direct extraction if ocr yields nothing
        text = extract_text_from_pdf(pdf)

    if text:
        source_dir_name = os.path.basename(os.path.normpath(SOURCE_PDF_DIR))
        txt_path = os.path.join(TEXT_DIR, f"{source_dir_name}.txt")
        with open(txt_path, "w") as f:
            f.write(text)
        print(f"  Saved text to {txt_path}")

        parsed_data = parse_case_data(text, filename)
        
        json_path = os.path.join(PARSED_DIR, filename.replace(".pdf", ".json"))
        with open(json_path, "w") as f:
            json.dump(parsed_data, f, indent=2)
        print(f"  Saved parsed data to {json_path}")

        records.append(parsed_data)

print(f"\n" + "="*80)
print("OUTPUT LOCATIONS")
print("="*80)
print(f"raw text files: {TEXT_DIR}")
print(f"parsed json data: {PARSED_DIR}")
print(f"case summaries: {OCR_TEXTS_DIR}")
print(f"="*80)
