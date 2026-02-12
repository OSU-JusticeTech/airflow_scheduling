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
        # keep header (first 3) and footer (last 2) pages to reduce noise
        if len(pages) > 5:
            pages = pages[:3] + pages[-2:]
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

# parse structured case fields from extracted text
def parse_case_data(text, filename):
    # extract structured case data matching raw_cases database schema
    source_dir_name = os.path.basename(os.path.normpath(SOURCE_PDF_DIR))
    parsed_data = {
        "case_number": source_dir_name,
        "case_title": None,
        "case_description": None,
        "case_status": None,
        "case_filed_date": None,
        "first_party_name": None,
        "first_party_type": None,
        "first_attorney_name": None,
        "first_attorney_type": None,
        "first_event_start": None,
        "first_event_end": None,
        "first_event_judge": None,
        "parties": [],
        "attorneys": [],
        "events": [],
        "docket": [],
        "dispositions": [],
        "raw_html": text
    }
    
    # case number extraction (override default if detected)
    case_num_match = re.search(r'(\d{4}\s+)?CVG[- ](\d{2})[- ](\d{5,6})', text, re.IGNORECASE)
    if case_num_match:
        parsed_data["case_number"] = case_num_match.group(0).strip()
    
    # extract all dates for events and case_filed_date
    date_pattern = r'(\d{1,2}[/-]\d{1,2}[/-]\d{4})'
    dates = re.findall(date_pattern, text)
    if dates:
        parsed_data["case_filed_date"] = dates[0]
        # store additional dates as events
        for date in dates[1:]:
            parsed_data["events"].append({"event_date": date})
    
    # extract parties (plaintiff, defendant, appellant, respondent, petitioner)
    party_keywords = ['plaintiff', 'defendant', 'appellant', 'respondent', 'petitioner']
    for keyword in party_keywords:
        matches = re.finditer(rf'{keyword}[:\s]+([A-Z][A-Za-z\s,\.&\-]+?)(?:\n|$)', text, re.IGNORECASE)
        for match in matches:
            party_name = match.group(1).strip()
            if party_name and len(party_name) > 2:
                parsed_data["parties"].append({
                    "party_name": party_name,
                    "party_type": keyword.capitalize(),
                    "address_id": None
                })
    
    if parsed_data["parties"]:
        parsed_data["first_party_name"] = parsed_data["parties"][0]["party_name"]
        parsed_data["first_party_type"] = parsed_data["parties"][0]["party_type"]
    
    # extract attorneys
    attorney_pattern = r'(?:attorney|counsel|lawyer)[:\s]+([A-Z][A-Za-z\s\.&\-]+?)(?:\n|,|$)'
    attorney_matches = re.finditer(attorney_pattern, text, re.IGNORECASE)
    for match in attorney_matches:
        attorney_name = match.group(1).strip()
        if attorney_name and len(attorney_name) > 2:
            parsed_data["attorneys"].append({
                "attorney_name": attorney_name,
                "attorney_type": "Attorney",
                "party_id": None,
                "address_id": None
            })
    
    if parsed_data["attorneys"]:
        parsed_data["first_attorney_name"] = parsed_data["attorneys"][0]["attorney_name"]
        parsed_data["first_attorney_type"] = parsed_data["attorneys"][0]["attorney_type"]
    
    # extract judge information
    judge_pattern = r'(?:judge|hon\.?|justice)\s+([A-Z][A-Za-z\s\.&\-]+?)(?:\n|$)'
    judge_match = re.search(judge_pattern, text, re.IGNORECASE)
    if judge_match:
        judge_name = judge_match.group(1).strip()
        parsed_data["first_event_judge"] = judge_name
        # add to events if not already present
        parsed_data["events"].append({
            "event_judge": judge_name,
            "event_name": "Court Hearing"
        })
    
    # extract case status
    status_keywords = ['open', 'closed', 'pending', 'resolved', 'dismissed', 'settled', 'active']
    for keyword in status_keywords:
        if re.search(rf'\b{keyword}\b', text, re.IGNORECASE):
            parsed_data["case_status"] = keyword.capitalize()
            break
    
    # extract monetary amounts for docket
    amount_pattern = r'\$\s*([\d,]+(?:\.\d{2})?)'
    amounts = re.findall(amount_pattern, text)
    for amount in amounts:
        parsed_data["docket"].append({
            "docket_date": dates[0] if dates else None,
            "docket_text": f"Amount: ${amount}",
            "docket_amount": float(amount.replace(',', '')),
            "docket_currency": "USD"
        })
    
    # extract dispositions (case outcomes)
    disposition_keywords = ['dismissed', 'judgment', 'settlement', 'verdict', 'plea']
    for keyword in disposition_keywords:
        if re.search(rf'\b{keyword}\b', text, re.IGNORECASE):
            parsed_data["dispositions"].append({
                "disposition_status": keyword.capitalize(),
                "disposition_status_date": dates[0] if dates else None,
                "judge": judge_name if 'judge_name' in locals() else None,
                "disposition_date": dates[0] if dates else None
            })
    
    return parsed_data

# deprecated keyword extraction placeholder
def extract_keywords(text, min_len=4):
    # deprecated - no longer using keyword extraction
    return []

# build summary records for reporting
def generate_summary_report(records):
    # create database-ready records matching raw_cases table schema
    summary = []
    for record in records:
        summary.append({
            "case_number": record.get("case_number"),
            "case_title": record.get("case_title"),
            "case_description": record.get("case_description"),
            "case_status": record.get("case_status"),
            "case_filed_date": record.get("case_filed_date"),
            "first_party_name": record.get("first_party_name"),
            "first_party_type": record.get("first_party_type"),
            "first_attorney_name": record.get("first_attorney_name"),
            "first_attorney_type": record.get("first_attorney_type"),
            "first_event_start": record.get("first_event_start"),
            "first_event_end": record.get("first_event_end"),
            "first_event_judge": record.get("first_event_judge"),
            "parties": record.get("parties", []),
            "attorneys": record.get("attorneys", []),
            "events": record.get("events", []),
            "docket": record.get("docket", []),
            "dispositions": record.get("dispositions", []),
            "raw_html": record.get("raw_html")
        })
    return summary

print("\n--- Processing PDFs ---")
records = []
for pdf in pdf_paths:
    filename = os.path.basename(pdf)
    print(f"Processing {filename}...")
    # use OCR to extract full text from the largest PDF
    text = ocr_pdf_fallback(pdf)

    if not text:
        # fallback to direct extraction if OCR yields nothing
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

# generate summary report
print("\n" + "="*80)
print("EXTRACTION RESULTS")
print("="*80)

if records:
    for i, record in enumerate(records, 1):
        print(f"\n[Case {i}] {record.get('case_number', 'Unknown')}")
        print(f"  case_number: {record.get('case_number') or 'not found'}")
        print(f"  case_title: {record.get('case_title') or 'not found'}")
        print(f"  case_description: {record.get('case_description') or 'not found'}")
        print(f"  case_status: {record.get('case_status') or 'not found'}")
        print(f"  case_filed_date: {record.get('case_filed_date') or 'not found'}")
        print(f"  first_party_name: {record.get('first_party_name') or 'not found'}")
        print(f"  first_party_type: {record.get('first_party_type') or 'not found'}")
        print(f"  first_attorney_name: {record.get('first_attorney_name') or 'not found'}")
        print(f"  first_attorney_type: {record.get('first_attorney_type') or 'not found'}")
        print(f"  first_event_judge: {record.get('first_event_judge') or 'not found'}")
        print(f"  parties ({len(record.get('parties', []))}): {record.get('parties')}")
        print(f"  attorneys ({len(record.get('attorneys', []))}): {record.get('attorneys')}")
        print(f"  events ({len(record.get('events', []))}): {record.get('events')}")
        print(f"  docket ({len(record.get('docket', []))}): {record.get('docket')}")
        print(f"  dispositions ({len(record.get('dispositions', []))}): {record.get('dispositions')}")
else:
    print("no records processed.")

# summary JSON output disabled; keep OCR text and parsed JSON only

print(f"\n" + "="*80)
print("OUTPUT LOCATIONS")
print("="*80)
print(f"raw text files: {TEXT_DIR}")
print(f"parsed json data: {PARSED_DIR}")
print(f"case summaries: {OCR_TEXTS_DIR}")
print(f"="*80)
