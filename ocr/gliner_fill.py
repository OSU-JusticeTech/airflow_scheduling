import argparse
import json
import os
import re
from datetime import datetime

from gliner import GLiNER

DEFAULT_MODEL = "urchade/gliner_small"
DEFAULT_THRESHOLD = 0.35

LABELS = [
    "case number",
    "court name",
    "filing date",
    "plaintiff name",
    "defendant name",
    "attorney name",
    "judge name",
    "disposition",
]


# load json data from disk
def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# save json data to disk
def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


# extract a cvg-style case number from text
def extract_case_number(text):
    match = re.search(r"\bCVG[- ]?\d{2}[- ]?\d{5,6}\b", text, re.IGNORECASE)
    return match.group(0).strip() if match else None


# extract numeric dates from text
def extract_dates(text):
    return re.findall(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{4}\b", text)


# extract month-name dates from text
def extract_month_dates(text):
    # match dates like "JAN 17 2020" or "January 17, 2020"
    pattern = r"\b(?:Jan|January|Feb|February|Mar|March|Apr|April|May|Jun|June|Jul|July|Aug|August|Sep|Sept|September|Oct|October|Nov|November|Dec|December)\b\s+\d{1,2}(?:,)?\s+\d{4}"
    return re.findall(pattern, text, re.IGNORECASE)


# dedupe and normalize gliner entities
def normalize_entities(entities):
    cleaned = []
    seen = set()
    for ent in entities:
        text = ent.get("text", "").strip()
        label = ent.get("label")
        score = ent.get("score", 0)
        key = (text.lower(), label)
        if not text or key in seen:
            continue
        seen.add(key)
        cleaned.append({"text": text, "label": label, "score": score})
    return cleaned


# group entities by label and sort by score
def group_by_label(entities):
    grouped = {}
    for ent in entities:
        grouped.setdefault(ent["label"], []).append(ent)
    for label in grouped:
        grouped[label].sort(key=lambda e: e.get("score", 0), reverse=True)
    return grouped


# pick the highest scored entity for a label
def pick_first(grouped, label):
    items = grouped.get(label, [])
    return items[0]["text"] if items else None


# return a case-insensitive unique list
def unique_list(values):
    result = []
    seen = set()
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


# update a field only when empty or overwriting
def update_if_empty(target, key, value, overwrite=False):
    if overwrite or target.get(key) in (None, "", []):
        target[key] = value


# build party records from names
def build_parties(plaintiffs, defendants):
    parties = []
    for name in plaintiffs:
        parties.append({"party_name": name, "party_type": "Plaintiff", "address_id": None})
    for name in defendants:
        parties.append({"party_name": name, "party_type": "Defendant", "address_id": None})
    return parties


# build attorney records from names
def build_attorneys(attorneys):
    return [
        {
            "attorney_name": name,
            "attorney_type": "Attorney",
            "party_id": None,
            "address_id": None,
        }
        for name in attorneys
    ]


# build disposition records from statuses
def build_dispositions(dispositions, filed_date):
    results = []
    for status in dispositions:
        results.append(
            {
                "disposition_status": status,
                "disposition_status_date": filed_date,
                "judge": None,
                "disposition_date": filed_date,
            }
        )
    return results


# infer case status from dispositions or keywords
def infer_case_status(text, dispositions):
    if dispositions:
        return dispositions[0]
    for keyword in ["dismissed", "judgment", "settlement", "verdict", "plea"]:
        if re.search(rf"\b{keyword}\b", text, re.IGNORECASE):
            return keyword.capitalize()
    return None


# read raw text content from a file
def read_text_file(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


# choose the best source text for extraction
def select_source_text(input_path, data, text_path=None):
    # prefer explicit text file, then raw_text/raw_html fallback
    if text_path:
        text = read_text_file(text_path)
        if text.strip():
            return text

    if data.get("raw_text"):
        return data.get("raw_text", "")
    if data.get("raw_html"):
        return data.get("raw_html", "")

    base = os.path.splitext(os.path.basename(input_path))[0]
    default_text_path = os.path.join("ocr", "raw_text", f"{base}.txt")
    text = read_text_file(default_text_path)
    return text


# create a base record for gliner output
def init_base_record(case_id, raw_text):
    return {
        "case_number": case_id,
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
        "raw_text": raw_text,
    }


# derive a case id from input or raw text filename
def derive_case_id(input_path, text_path):
    if text_path:
        return os.path.splitext(os.path.basename(text_path))[0]
    if input_path:
        base = os.path.splitext(os.path.basename(input_path))[0]
        if base.startswith("unknown_"):
            try:
                raw_text_files = [
                    f for f in os.listdir(os.path.join("ocr", "raw_text")) if f.endswith(".txt")
                ]
            except Exception:
                raw_text_files = []
            if len(raw_text_files) == 1:
                return os.path.splitext(raw_text_files[0])[0]
        return base
    return "unknown"


# keep header and footer pages based on markers
def select_important_pages(text):
    # Keep header (first 2) and footer (last 2) pages using OCR markers.
    pages = re.split(r"<<<\s*OCR_PAGE_\d+\s*>>>", text)
    pages = [page.strip() for page in pages if page.strip()]
    if not pages:
        return ""
    if len(pages) > 4:
        selected_pages = pages[:2] + pages[-2:]
    else:
        selected_pages = pages
    return "\n".join(selected_pages)


# normalize label casing and whitespace
def normalize_label(label):
    return label.strip().lower()


# filter out noisy or invalid name candidates
def is_reasonable_name(value):
    if not value:
        return False
    value = value.strip()
    if len(value) < 3 or len(value) > 80:
        return False
    if re.search(r"\b(premises|possession|process|hearing|lease|agreement)\b", value, re.IGNORECASE):
        return False
    return True


# extract a party name from nearby label lines
def extract_party_from_lines(lines, label):
    # find a label line and take the previous non-empty line as the name
    label_regex = re.compile(label, re.IGNORECASE)
    for i, line in enumerate(lines):
        if label_regex.search(line):
            for j in range(i - 1, -1, -1):
                candidate = lines[j].strip()
                if candidate:
                    if is_reasonable_name(candidate):
                        return candidate
                    break
    return None


# extract plaintiff and defendant names from text
def extract_parties_from_text(text):
    plaintiffs = []
    defendants = []
    lines = [line.strip() for line in text.splitlines()]

    for match in re.finditer(r"Plaintiff\(s\):\s*(.+)", text, re.IGNORECASE):
        name = match.group(1).strip()
        if is_reasonable_name(name):
            plaintiffs.append(name)

    for match in re.finditer(r"Defendant\(s\):\s*(.+)", text, re.IGNORECASE):
        name = match.group(1).strip()
        if is_reasonable_name(name):
            defendants.append(name)

    plaintiff_line = extract_party_from_lines(lines, r"\bPLAINTIFF\b")
    defendant_line = extract_party_from_lines(lines, r"\bDEFENDANT\b")
    if plaintiff_line:
        plaintiffs.append(plaintiff_line)
    if defendant_line:
        defendants.append(defendant_line)

    return unique_list(plaintiffs), unique_list(defendants)


# extract attorney names from text context
def extract_attorneys_from_text(text):
    attorneys = []
    lines = [line.strip() for line in text.splitlines()]
    for i, line in enumerate(lines):
        if re.search(r"Attorney for", line, re.IGNORECASE):
            for j in range(i - 1, -1, -1):
                candidate = lines[j].strip()
                if candidate:
                    if is_reasonable_name(candidate):
                        attorneys.append(candidate)
                    break
    return unique_list(attorneys)


# run gliner extraction and write the output json
def run_gliner_fill(input_path, output_path, model_name, threshold, overwrite, text_path=None):
    data = None
    if input_path and os.path.exists(input_path):
        data = load_json(input_path)
    else:
        case_id = derive_case_id(input_path, text_path)
        raw_text = read_text_file(text_path) if text_path else ""
        if not raw_text and case_id != "unknown":
            fallback_text = os.path.join("ocr", "raw_text", f"{case_id}.txt")
            raw_text = read_text_file(fallback_text)
        data = init_base_record(case_id, raw_text)
    raw_text = select_source_text(input_path, data, text_path)
    clean_text = select_important_pages(raw_text) if raw_text else ""

    if not raw_text.strip():
        raise ValueError("no OCR text found; cannot run GLiNER")

    model = GLiNER.from_pretrained(model_name)
    text_for_model = clean_text if clean_text else raw_text
    entities = model.predict_entities(text_for_model, LABELS, threshold=threshold)
    entities = normalize_entities(entities)
    grouped = group_by_label(entities)

    case_id = derive_case_id(input_path, text_path)
    case_number = extract_case_number(text_for_model) or case_id
    month_dates = extract_month_dates(text_for_model)
    numeric_dates = extract_dates(text_for_model)
    filed_date = month_dates[0] if month_dates else (numeric_dates[0] if numeric_dates else None)

    plaintiffs = [e["text"] for e in grouped.get("plaintiff name", [])]
    defendants = [e["text"] for e in grouped.get("defendant name", [])]
    attorneys = [e["text"] for e in grouped.get("attorney name", [])]
    dispositions = [e["text"].capitalize() for e in grouped.get("disposition", [])]

    extracted_plaintiffs, extracted_defendants = extract_parties_from_text(text_for_model)
    extracted_attorneys = extract_attorneys_from_text(text_for_model)

    plaintiffs = unique_list(plaintiffs + extracted_plaintiffs)
    defendants = unique_list(defendants + extracted_defendants)
    attorneys = unique_list(attorneys + extracted_attorneys)
    dispositions = unique_list(dispositions)

    case_title = None
    if not case_title and plaintiffs and defendants:
        case_title = f"{plaintiffs[0]} v. {defendants[0]}"

    case_description = None
    case_status = infer_case_status(text_for_model, dispositions)
    first_party_name = plaintiffs[0] if plaintiffs else (defendants[0] if defendants else None)
    first_party_type = "Plaintiff" if plaintiffs else ("Defendant" if defendants else None)
    first_attorney_name = attorneys[0] if attorneys else None
    first_attorney_type = "Attorney" if first_attorney_name else None
    first_event_judge = pick_first(grouped, "judge name")

    update_if_empty(data, "case_number", case_number, overwrite)
    update_if_empty(data, "case_title", case_title, overwrite)
    update_if_empty(data, "case_description", case_description, overwrite)
    update_if_empty(data, "case_status", case_status, overwrite)
    update_if_empty(data, "case_filed_date", filed_date, overwrite)
    update_if_empty(data, "first_party_name", first_party_name, overwrite)
    update_if_empty(data, "first_party_type", first_party_type, overwrite)
    update_if_empty(data, "first_attorney_name", first_attorney_name, overwrite)
    update_if_empty(data, "first_attorney_type", first_attorney_type, overwrite)
    update_if_empty(data, "first_event_judge", first_event_judge, overwrite)

    if overwrite or not data.get("parties"):
        data["parties"] = build_parties(plaintiffs, defendants)
    if overwrite or not data.get("attorneys"):
        data["attorneys"] = build_attorneys(attorneys)
    if overwrite or not data.get("dispositions"):
        data["dispositions"] = build_dispositions(dispositions, filed_date)

    data["gliner_meta"] = {
        "model": model_name,
        "threshold": threshold,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }

    save_json(output_path, data)


# parse cli args and run the fill pipeline
def main():
    parser = argparse.ArgumentParser(description="Fill case JSON using GLiNER")
    parser.add_argument(
        "--input",
        default="ocr/texts/unknown_20260129_111712.json",
        help="Path to input JSON file",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Path to output JSON file (default: <input>_gliner.json)",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help="GLiNER model name or path",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help="GLiNER score threshold",
    )
    parser.add_argument(
        "--text-path",
        default="",
        help="Optional path to a raw text file to analyze",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing non-empty fields",
    )

    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    text_path = args.text_path or None

    if not output_path:
        case_id = derive_case_id(input_path, text_path)
        output_path = os.path.join("ocr", "texts", f"{case_id}.json")

    run_gliner_fill(input_path, output_path, args.model, args.threshold, args.overwrite, text_path)
    print(f"saved: {output_path}")


if __name__ == "__main__":
    main()
