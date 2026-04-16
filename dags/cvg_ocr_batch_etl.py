
from __future__ import annotations

import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.runtime import ensure_project_root_on_path

ensure_project_root_on_path()


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


OCR_DIR = Path(os.getenv("CVG_OCR_DIR", str(PROJECT_ROOT / "ocr"))).expanduser()
CASE_DIRS_ROOT = Path(
    os.getenv("CVG_CASE_DIRS_ROOT", str(PROJECT_ROOT.parent / "k3s.dranspo.se:8000" / "data"))
).expanduser()
RAW_TEXT_DIR = Path(os.getenv("CVG_RAW_TEXT_DIR", str(OCR_DIR / "raw_text"))).expanduser()
JSON_OUTPUT_DIR = Path(os.getenv("CVG_JSON_OUTPUT_DIR", str(OCR_DIR / "texts"))).expanduser()
BATCH_SIZE = _int_env("CVG_BATCH_SIZE", 3)
GLINER_TIMEOUT_SECONDS = _int_env("CVG_GLINER_TIMEOUT_SECONDS", 180)
ADDRESS_TIMEOUT_SECONDS = _int_env("CVG_ADDRESS_TIMEOUT_SECONDS", 60)
DEFAULT_SCHEDULE = os.getenv("CVG_OCR_SCHEDULE", "@daily")
DEFAULT_CATCHUP = _bool_env("CVG_OCR_CATCHUP", False)
DEFAULT_START_DATE = datetime.fromisoformat(os.getenv("CVG_OCR_START_DATE", "2026-03-11"))

RAW_TEXT_DIR.mkdir(parents=True, exist_ok=True)
JSON_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def discover_case_ids(root_dir: Path, limit: int | None = None) -> list[str]:
    if not root_dir.exists():
        print(f"error: root directory not found: {root_dir}")
        return []

    folders = sorted(
        [d for d in root_dir.iterdir() if d.is_dir() and not d.name.startswith(".")],
        key=lambda p: p.name,
    )

    unprocessed_ids = []
    for folder in folders:
        # skip cases that already produced final json output
        output_json = JSON_OUTPUT_DIR / f"{folder.name}.json"
        if not output_json.exists():
            unprocessed_ids.append(folder.name)

    if limit:
        unprocessed_ids = unprocessed_ids[:limit]

    print(f"found {len(unprocessed_ids)} case folder(s) to process")
    return unprocessed_ids


def _score_ocr_text(text: str) -> int:
    return len(re.findall(r"[A-Za-z]{3,}", text))


def _ocr_best_for_page(page, image_ops_module, pytesseract_module) -> str:
    candidates = []
    # try rotations + mirrored variants and keep the most readable text
    for angle in (0, 90, 180, 270):
        rotated = page.rotate(angle, expand=True) if angle else page
        candidates.append(rotated)
        candidates.append(image_ops_module.mirror(rotated))

    best_text = ""
    best_score = -1
    for image in candidates:
        text = pytesseract_module.image_to_string(image)
        score = _score_ocr_text(text)
        if score > best_score:
            best_score = score
            best_text = text
    return best_text


def _ocr_pdf(pdf_path: Path, convert_from_path_func, image_ops_module, pytesseract_module) -> str:
    try:
        print("    running ocr...")
        pages = convert_from_path_func(str(pdf_path), dpi=300)
        # sample beginning/end pages to reduce runtime and noise
        if len(pages) > 6:
            pages = pages[:3] + pages[-3:]

        text_output = []
        for i, page in enumerate(pages):
            print(f"      page {i + 1}/{len(pages)}...")
            text = _ocr_best_for_page(page, image_ops_module, pytesseract_module)
            if text:
                text_output.append(text)
            text_output.append(f"\n<<< OCR_PAGE_{i + 1} >>>\n")
        return "\n".join(text_output) if text_output else ""
    except Exception as exc:
        print(f"    ocr error: {exc}")
        return ""


def _extract_text_direct(pdf_path: Path, pdfplumber_module) -> str:
    try:
        text_output = []
        with pdfplumber_module.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    text_output.append(text)
        return "\n".join(text_output) if text_output else ""
    except Exception as exc:
        print(f"    direct extraction error: {exc}")
        return ""


def run_ocr_for_case(case_id: str) -> str | None:
    import pdfplumber
    import pytesseract
    from pdf2image import convert_from_path
    from PIL import ImageOps

    case_folder = CASE_DIRS_ROOT / case_id
    if not case_folder.exists():
        print(f"error: case folder not found: {case_folder}")
        return None

    print(f"\n{'=' * 80}")
    print(f"processing: {case_id}")
    print(f"{'=' * 80}")

    pdf_files = list(case_folder.glob("*.pdf"))
    if not pdf_files:
        print(f"  no pdfs found in {case_id}")
        return None

    largest_pdf = max(pdf_files, key=lambda p: p.stat().st_size)
    print(f"  selected: {largest_pdf.name} ({largest_pdf.stat().st_size} bytes)")

    text = _ocr_pdf(largest_pdf, convert_from_path, ImageOps, pytesseract)
    if not text:
        # fallback if image ocr returns empty text
        print("    ocr failed, trying direct extraction...")
        text = _extract_text_direct(largest_pdf, pdfplumber)

    if not text:
        print("  no text extracted")
        return None

    txt_path = RAW_TEXT_DIR / f"{case_id}.txt"
    txt_path.write_text(text, encoding="utf-8")
    print(f"  saved raw text: {txt_path}")
    return case_id


def run_gliner_for_case(case_id: str | None) -> str | None:
    if not case_id:
        return None

    print(f"\n  running gliner fill for {case_id}...")
    txt_path = RAW_TEXT_DIR / f"{case_id}.txt"
    if not txt_path.exists():
        print(f"    error: raw text not found: {txt_path}")
        return None

    json_output = JSON_OUTPUT_DIR / f"{case_id}.json"
    cmd = [
        sys.executable,
        str(OCR_DIR / "gliner_fill.py"),
        "--text-path",
        str(txt_path),
        "--output",
        str(json_output),
        "--overwrite",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=GLINER_TIMEOUT_SECONDS)
        if result.returncode == 0:
            print(f"  gliner complete: {json_output}")
            return case_id

        print(f"    gliner error: {result.stderr}")
        return None
    except subprocess.TimeoutExpired:
        print("    gliner timeout")
        return None
    except Exception as exc:
        print(f"    gliner exception: {exc}")
        return None


def run_address_extraction_for_case(case_id: str | None) -> bool:
    if not case_id:
        return False

    print(f"\n  running address extraction for {case_id}...")
    case_folder = CASE_DIRS_ROOT / case_id
    if not case_folder.exists():
        print(f"    error: case folder not found: {case_folder}")
        return False

    cmd = [sys.executable, str(OCR_DIR / "html_printable_overwrite.py"), str(case_folder)]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=ADDRESS_TIMEOUT_SECONDS)
        if result.returncode == 0:
            print(f"  address extraction complete for {case_id}")
            return True

        print(f"    address extraction error: {result.stderr}")
        return False
    except subprocess.TimeoutExpired:
        print("    address extraction timeout")
        return False
    except Exception as exc:
        print(f"    address extraction exception: {exc}")
        return False


def load_gliner_json_to_db(case_id: str | None, dag_run_id: str | None = None, task_id: str | None = None) -> bool:
    if not case_id:
        return False

    json_path = JSON_OUTPUT_DIR / f"{case_id}.json"
    if not json_path.exists():
        print(f"    error: gliner json not found: {json_path}")
        return False

    try:
        from utils.gliner_django_loader import load_gliner_case_json

        result = load_gliner_case_json(json_path, dag_run_id=dag_run_id, task_id=task_id)
        print(f"  loaded case {result['case_number']} (id={result['case_id']}) to postgres")
        return True
    except Exception as exc:
        print(f"    db load exception for {case_id}: {exc}")
        return False


with DAG(
    dag_id="cvg_ocr_batch_etl",
    start_date=DEFAULT_START_DATE,
    schedule_interval=DEFAULT_SCHEDULE,
    catchup=DEFAULT_CATCHUP,
    max_active_runs=1,
    tags=["ocr", "cvg", "etl"],
) as dag:

    @task(task_id="discover_case_ids")
    def discover_case_ids_task() -> list[str]:
        return discover_case_ids(CASE_DIRS_ROOT, limit=BATCH_SIZE)

    @task(task_id="run_ocr")
    def run_ocr_task(case_id: str) -> str | None:
        return run_ocr_for_case(case_id)

    @task(task_id="run_gliner")
    def run_gliner_task(case_id: str | None) -> str | None:
        return run_gliner_for_case(case_id)

    @task(task_id="run_address_extraction")
    def run_address_task(case_id: str | None) -> bool:
        return run_address_extraction_for_case(case_id)

    @task(task_id="load_gliner_json_to_db")
    def load_gliner_to_db_task(case_id: str | None) -> bool:
        context = get_current_context()
        dag_run = context.get("dag_run")
        dag_run_id = dag_run.run_id if dag_run else None
        task_id = context.get("task").task_id if context.get("task") else None
        return load_gliner_json_to_db(case_id, dag_run_id=dag_run_id, task_id=task_id)

    case_ids = discover_case_ids_task()
    # map each case id through ocr -> gliner -> address extraction
    ocr_case_ids = run_ocr_task.expand(case_id=case_ids)
    gliner_case_ids = run_gliner_task.expand(case_id=ocr_case_ids)
    addressed_case_flags = run_address_task.expand(case_id=gliner_case_ids)
    load_gliner_to_db_task.expand(case_id=gliner_case_ids).set_upstream(addressed_case_flags)
