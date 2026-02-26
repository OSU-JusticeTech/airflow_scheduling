
import os
import sys
import json
import subprocess
from pathlib import Path

# configuration
BASE_DIR = Path(__file__).parent.parent
OCR_DIR = BASE_DIR / "ocr"
CASE_DIRS_ROOT = BASE_DIR.parent / "k3s.dranspo.se:8000" / "data"
RAW_TEXT_DIR = OCR_DIR / "raw_text"
JSON_OUTPUT_DIR = OCR_DIR / "texts"
BATCH_SIZE = 3

# ensure output dirs exist
RAW_TEXT_DIR.mkdir(parents=True, exist_ok=True)
JSON_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def discover_case_folders(root_dir, limit=None):
    """find case folders in the root directory"""
    if not root_dir.exists():
        print(f"error: root directory not found: {root_dir}")
        return []
    
    folders = [
        d for d in root_dir.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    ]
    folders = sorted(folders, key=lambda p: p.name)

    # skip folders that already have output json
    unprocessed = []
    for folder in folders:
        output_json = JSON_OUTPUT_DIR / f"{folder.name}.json"
        if output_json.exists():
            continue
        unprocessed.append(folder)
    folders = unprocessed
    
    if limit:
        folders = folders[:limit]
    
    print(f"found {len(folders)} case folder(s)")
    return folders


def run_ocr_for_folder(case_folder):
    """run ocr extraction (inline test_run logic) for a single folder"""
    import pdfplumber
    import pytesseract
    from pdf2image import convert_from_path
    from PIL import ImageOps
    import re
    
    print(f"\n{'='*80}")
    print(f"processing: {case_folder.name}")
    print(f"{'='*80}")
    
    # find pdfs
    pdf_files = list(case_folder.glob("*.pdf"))
    if not pdf_files:
        print(f"  no pdfs found in {case_folder.name}")
        return None
    
    # select largest
    largest_pdf = max(pdf_files, key=lambda p: p.stat().st_size)
    print(f"  selected: {largest_pdf.name} ({largest_pdf.stat().st_size} bytes)")
    
    # score ocr text by readable word count
    def score_ocr_text(text):
        words = re.findall(r"[A-Za-z]{3,}", text)
        return len(words)
    
    # pick the best ocr result across rotations and mirroring
    def ocr_best_for_page(page):
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
    def ocr_pdf(pdf_path):
        try:
            print(f"    running ocr...")
            pages = convert_from_path(str(pdf_path), dpi=300)
            # keep header (first 3) and footer (last 3) pages to reduce noise
            if len(pages) > 6:
                pages = pages[:3] + pages[-3:]
            text_output = []
            for i, page in enumerate(pages):
                print(f"      page {i+1}/{len(pages)}...")
                text = ocr_best_for_page(page)
                if text:
                    text_output.append(text)
                page_marker = f"\n<<< OCR_PAGE_{i+1} >>>\n"
                text_output.append(page_marker)
            return "\n".join(text_output) if text_output else ""
        except Exception as e:
            print(f"    ocr error: {e}")
            return ""
    
    # try direct extraction fallback
    def extract_text_direct(pdf_path):
        try:
            text_output = []
            with pdfplumber.open(str(pdf_path)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        text_output.append(text)
            return "\n".join(text_output) if text_output else ""
        except Exception as e:
            print(f"    direct extraction error: {e}")
            return ""
    
    # run ocr
    text = ocr_pdf(largest_pdf)
    if not text:
        print(f"    ocr failed, trying direct extraction...")
        text = extract_text_direct(largest_pdf)
    
    if not text:
        print(f"  no text extracted")
        return None
    
    # save raw text
    case_id = case_folder.name
    txt_path = RAW_TEXT_DIR / f"{case_id}.txt"
    txt_path.write_text(text, encoding="utf-8")
    print(f"  saved raw text: {txt_path}")
    
    return case_id


def run_address_extraction_for_case(case_id):
    """run address extraction from printable.html for a single case"""
    print(f"\n  running address extraction for {case_id}...")
    
    # case_id is already the full folder name like "CVG-20-00287"
    case_folder = CASE_DIRS_ROOT / case_id
    if not case_folder.exists():
        print(f"    error: case folder not found: {case_folder}")
        return False
    
    cmd = [
        sys.executable,
        str(OCR_DIR / "html_printable_overwrite.py"),
        str(case_folder)
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print(f"  address extraction complete for {case_id}")
            return True
        else:
            print(f"    address extraction error: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print(f"    address extraction timeout")
        return False
    except Exception as e:
        print(f"    address extraction exception: {e}")
        return False


def run_gliner_for_case(case_id):
    """run gliner fill for a single case"""
    print(f"\n  running gliner fill for {case_id}...")
    
    txt_path = RAW_TEXT_DIR / f"{case_id}.txt"
    if not txt_path.exists():
        print(f"    error: raw text not found: {txt_path}")
        return False
    
    json_output = JSON_OUTPUT_DIR / f"{case_id}.json"
    
    cmd = [
        sys.executable,
        str(OCR_DIR / "gliner_fill.py"),
        "--text-path", str(txt_path),
        "--output", str(json_output),
        "--overwrite"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode == 0:
            print(f"  gliner complete: {json_output}")
            return True
        else:
            print(f"    gliner error: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print(f"    gliner timeout")
        return False
    except Exception as e:
        print(f"    gliner exception: {e}")
        return False


def main():
    print(f"batch etl starting (max {BATCH_SIZE} folders)")
    
    case_folders = discover_case_folders(CASE_DIRS_ROOT, limit=BATCH_SIZE)
    
    if not case_folders:
        print("no case folders to process")
        return
    
    success_count = 0
    fail_count = 0
    
    for folder in case_folders:
        case_id = run_ocr_for_folder(folder)
        if not case_id:
            fail_count += 1
            continue
        
        if run_gliner_for_case(case_id):
            # run address extraction after successful gliner processing
            run_address_extraction_for_case(case_id)
            success_count += 1
        else:
            fail_count += 1
    
    print(f"\n{'='*80}")
    print(f"batch etl complete")
    print(f"  successful: {success_count}")
    print(f"  failed: {fail_count}")
    print(f"  raw text dir: {RAW_TEXT_DIR}")
    print(f"  json output dir: {JSON_OUTPUT_DIR}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
