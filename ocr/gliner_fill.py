import argparse
import json
import os
import re
from datetime import datetime

from gliner import GLiNER

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

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
    "address",
]


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def extract_case_number(text):
    match = re.search(r"\bCVG[- ]?\d{2}[- ]?\d{5,6}\b", text, re.IGNORECASE)
    return match.group(0).strip() if match else None


def extract_dates(text):
    return re.findall(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{4}\b", text)


def extract_month_dates(text):
    pattern = r"\b(?:Jan|January|Feb|February|Mar|March|Apr|April|May|Jun|June|Jul|July|Aug|August|Sep|Sept|September|Oct|October|Nov|November|Dec|December)\b\s+\d{1,2}(?:,)?\s+\d{4}"
    return re.findall(pattern, text, re.IGNORECASE)


def extract_unpaid_amount(text):
    # extract unpaid balance or amount owed from ocr text
    patterns = [
        r'balance\s+due\s*:?\s*\$?\s*([\d,]+\.?\d{0,2})',
        r'unpaid\s+(?:balance|amount)\s*:?\s*\$?\s*([\d,]+\.?\d{0,2})',
        r'amount\s+owed\s*:?\s*\$?\s*([\d,]+\.?\d{0,2})',
        r'total\s+due\s*:?\s*\$?\s*([\d,]+\.?\d{0,2})',
        r'balance\s+notification\s*:?\s*\$?\s*([\d,]+\.?\d{0,2})',
        r'outstanding\s+(?:balance|amount)\s*:?\s*\$?\s*([\d,]+\.?\d{0,2})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            amount_str = match.group(1).replace(',', '')
            try:
                amount = float(amount_str)
                return f"${amount:,.2f}"
            except ValueError:
                continue
    
    return None


# extract addresses from html file using patterns and gliner
def extract_addresses_from_html(case_directory, model):
    addresses = []
    if not case_directory or not os.path.exists(case_directory):
        return addresses
    
    printable_html_path = os.path.join(case_directory, "printable.html")
    
    if os.path.exists(printable_html_path):
        try:
            if BeautifulSoup is None:
                return addresses
            
            with open(printable_html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            soup = BeautifulSoup(html_content, 'html.parser')
            html_text = soup.get_text()
            
            address_patterns = [
                r'premises at ([^.\n]+)',
                r'serve defendant[^\n]*at ([^.\n]+)',
                r'at (\d+\s+[A-Z][A-Z\s]+(?:ST|STREET|AVE|AVENUE|DR|DRIVE|RD|ROAD|BLVD|BOULEVARD)(?:\s+(?:APT|APARTMENT|UNIT|STE|SUITE|#)\s*\w+)?\s*,?\s*[A-Z]+\s*,?\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?)',
            ]
            
            for pattern in address_patterns:
                matches = re.findall(pattern, html_text, re.IGNORECASE)
                for match in matches:
                    clean_match = match.strip()
                    if re.search(r'\d+.*(?:DR|DRIVE|ST|STREET|AVE|AVENUE)', clean_match, re.IGNORECASE):
                        if is_reasonable_address(clean_match):
                            addresses.append(parse_address_string(clean_match))
                            break
                if addresses:
                    break
            
            if len(addresses) < 2:
                entities = model.predict_entities(html_text, ["address", "location"], threshold=0.3)
                
                for entity in entities:
                    if entity.get("label") in ["address", "location"] and entity.get("text"):
                        address_text = entity["text"].strip()
                        if not re.search(r'\b(court|courtroom|clerk|municipal)\b', address_text, re.IGNORECASE):
                            if is_reasonable_address(address_text):
                                addresses.append(parse_address_string(address_text))
        
        except Exception:
            pass
    
    return addresses


def extract_addresses_after_at(text):
    addresses = []
    lines = [line.strip() for line in text.splitlines()]
    
    for line in lines:
        # improved pattern that stops at logical address boundaries
        at_matches = re.finditer(r'\b(?:at|located at|residing at|address at)\s+(\d+\s+[A-Z][A-Z\s]+(?:ST|STREET|AVE|AVENUE|DR|DRIVE|RD|ROAD|BLVD|BOULEVARD)(?:\s+(?:APT|APARTMENT|UNIT|STE|SUITE|#)\s*\w+)?\s*,?\s*[A-Z]+\s*,?\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?)', line, re.IGNORECASE)
        
        for match in at_matches:
            candidate = match.group(1).strip()
            if candidate and is_reasonable_address(candidate):
                parsed_addr = parse_address_string(candidate)
                addresses.append(parsed_addr)
    
    return addresses


# comprehensive address extraction with priority assignment
def extract_addresses_comprehensive(text, party_names, input_path=None, text_path=None, model=None):
    party_addresses = {}
    all_found_addresses = []
    
    case_directory = None
    if input_path and os.path.exists(input_path):
        if os.path.isdir(input_path):
            case_directory = input_path
        else:
            case_directory = os.path.dirname(input_path)
    elif text_path and os.path.exists(text_path):
        case_directory = os.path.dirname(text_path)
    
    if case_directory and model:
        html_addresses = extract_addresses_from_html(case_directory, model)
        all_found_addresses.extend(html_addresses)
    
    if len(all_found_addresses) < 2:
        at_addresses = extract_addresses_after_at(text)
        all_found_addresses.extend(at_addresses)
    
    if not all_found_addresses:
        party_addresses = extract_party_addresses(text, party_names)
        return party_addresses
    
    defendants = [name for name in party_names if any(keyword in name.lower() for keyword in ['lewis', 'defendant'])]
    plaintiffs = [name for name in party_names if name not in defendants]
    
    if defendants and all_found_addresses:
        party_addresses[defendants[0]] = all_found_addresses[0]
    
    remaining_parties = plaintiffs + defendants[1:]
    for i, party_name in enumerate(remaining_parties):
        if i + 1 < len(all_found_addresses):
            party_addresses[party_name] = all_found_addresses[i + 1]
        elif all_found_addresses and party_name not in party_addresses:
            party_addresses[party_name] = all_found_addresses[0]
    
    return party_addresses


def extract_party_addresses(text, party_names):
    party_addresses = {}
    lines = [line.strip() for line in text.splitlines()]
    
    for party_name in party_names:
        for i, line in enumerate(lines):
            if party_name.lower() in line.lower():
                for j in range(i + 1, min(i + 6, len(lines))):
                    candidate = lines[j].strip()
                    if candidate and is_reasonable_address(candidate):
                        party_addresses[party_name] = parse_address_string(candidate)
                        break
                if party_name in party_addresses:
                    break
    
    return party_addresses


def is_reasonable_address(value):
    if not value:
        return False
    value = value.strip()
    if len(value) < 5 or len(value) > 150:
        return False
    if not re.search(r"\d", value):
        return False
    if re.search(r"\b(case|court|hearing|plaintiff|defendant|attorney|judge)\b", value, re.IGNORECASE):
        return False
    return True


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


def group_by_label(entities):
    grouped = {}
    for ent in entities:
        grouped.setdefault(ent["label"], []).append(ent)
    for label in grouped:
        grouped[label].sort(key=lambda e: e.get("score", 0), reverse=True)
    return grouped


def pick_first(grouped, label):
    items = grouped.get(label, [])
    return items[0]["text"] if items else None


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


def update_if_empty(target, key, value, overwrite=False):
    if overwrite or target.get(key) in (None, "", []):
        target[key] = value


def build_parties(plaintiffs, defendants, party_addresses):
    parties = []
    for name in plaintiffs:
        address = party_addresses.get(name, None)
        parties.append({
            "party_name": name,
            "party_type": "Plaintiff",
            "address_id": None,
            "address": address
        })
    for name in defendants:
        address = party_addresses.get(name, None)
        parties.append({
            "party_name": name,
            "party_type": "Defendant",
            "address_id": None,
            "address": address
        })
    return parties


def build_attorneys(attorneys, attorney_addresses=None):
    results = []
    for name in attorneys:
        results.append({
            "attorney_name": name,
            "attorney_type": "Attorney",
            "party_id": None
        })
    return results


# parse address string into components
def parse_address_string(address_str):
    address_str = address_str.strip()
    full_address = address_str
    
    # basic address structure
    result = {
        "address_type": "Physical",
        "address_line1": address_str,
        "address_line2": full_address,
        "city": None,
        "state": None,
        "country": "USA",
        "postal_code": None,
    }
    
    remaining_addr = address_str
    
    zip_match = re.search(r'\b(\d{5})(?:-\d{4})?\s*$', remaining_addr)
    if zip_match:
        result["postal_code"] = zip_match.group(1)
        remaining_addr = remaining_addr[:zip_match.start()].strip()
    
    state_match = re.search(r'\b([A-Z]{2})\s*$', remaining_addr)
    if state_match:
        result["state"] = state_match.group(1)
        remaining_addr = remaining_addr[:state_match.start()].strip()
    
    remaining_addr = remaining_addr.rstrip(',').strip()
    
    if ',' in remaining_addr:
        parts = remaining_addr.rsplit(',', 1)
        if len(parts) == 2:
            street_part = parts[0].strip()
            city_part = parts[1].strip()
            if city_part:
                result["city"] = city_part
                result["address_line1"] = street_part
            else:
                result["address_line1"] = remaining_addr
        else:
            result["address_line1"] = remaining_addr
    else:
        words = remaining_addr.split()
        if len(words) >= 2:
            result["city"] = words[-1]
            result["address_line1"] = ' '.join(words[:-1])
        else:
            result["address_line1"] = remaining_addr
    
    return result


# extract judge name from html file
def extract_judge_from_html(case_directory):
    judge_name = None
    if not case_directory or not os.path.exists(case_directory):
        return judge_name
    
    printable_html_path = os.path.join(case_directory, "printable.html")
    
    if os.path.exists(printable_html_path):
        try:
            if BeautifulSoup is None:
                return judge_name
            
            with open(printable_html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            soup = BeautifulSoup(html_content, 'html.parser')
            html_text = soup.get_text()
            
            # look for judge patterns
            judge_patterns = [
                r'judge[:\s]+([A-Z][a-zA-Z\s\.]+)',
                r'the honorable\s+([A-Z][a-zA-Z\s\.]+)',
                r'hon\.?\s+([A-Z][a-zA-Z\s\.]+)',
            ]
            
            for pattern in judge_patterns:
                matches = re.findall(pattern, html_text, re.IGNORECASE)
                for match in matches:
                    clean_match = match.strip()
                    # remove common unwanted suffixes
                    clean_match = re.sub(r'\s*(view scanned document|scanned|document).*$', '', clean_match, flags=re.IGNORECASE)
                    clean_match = re.sub(r'\n.*$', '', clean_match)  # remove everything after newline
                    # take only the first word(s) that look like a name
                    name_parts = clean_match.split()
                    if name_parts:
                        clean_match = ' '.join(name_parts[:2]).strip()  # take up to 2 words
                    
                    if len(clean_match) > 2 and len(clean_match) < 30:
                        # basic validation for judge names
                        if not re.search(r'\b(court|clerk|case|defendant|plaintiff|view|document|scanned)\b', clean_match, re.IGNORECASE):
                            judge_name = clean_match
                            break
                if judge_name:
                    break
        
        except Exception:
            pass
    
    return judge_name


def build_dispositions(dispositions, filed_date, judge_name=None):
    results = []
    for status in dispositions:
        results.append(
            {
                "disposition_status": status,
                "disposition_status_date": filed_date,
                "judge": judge_name,
                "disposition_date": filed_date,
            }
        )
    return results


def infer_case_status(text, dispositions):
    if dispositions:
        return dispositions[0]
    for keyword in ["dismissed", "judgment", "settlement", "verdict", "plea"]:
        if re.search(rf"\b{keyword}\b", text, re.IGNORECASE):
            return keyword.capitalize()
    return None


def read_text_file(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


def select_source_text(input_path, data, text_path=None):
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


# filter pages by ocr markers
def select_important_pages(text):
    # keep header (first 2) and footer (last 2) pages using ocr markers
    pages = re.split(r"<<<\s*OCR_PAGE_\d+\s*>>>", text)
    pages = [page.strip() for page in pages if page.strip()]
    if not pages:
        return ""
    if len(pages) > 4:
        selected_pages = pages[:2] + pages[-2:]
    else:
        selected_pages = pages

    def header_score(page_text):
        keywords = ["case number", "plaintiff", "defendant", "court", "landlord"]
        return sum(1 for keyword in keywords if keyword in page_text.lower())

    header_page = max(selected_pages, key=header_score)
    ordered_pages = [header_page]
    for page in selected_pages:
        if page != header_page:
            ordered_pages.append(page)

    return "\n".join(ordered_pages)


# normalize label text
def normalize_label(label):
    return label.strip().lower()


# filter invalid name candidates
def is_reasonable_name(value):
    if not value:
        return False
    value = value.strip()
    if len(value) < 3 or len(value) > 80:
        return False
    if re.search(r"\b(premises|possession|process|hearing|lease|agreement)\b", value, re.IGNORECASE):
        return False
    return True


# extract party name from label context
def extract_party_from_lines(lines, label):
    # find label line and take previous line as name
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


# extract party names from text
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


# extract attorney names
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


# cleanup party roles
def apply_party_cleanup_rules(data):
    parties = data.get("parties", [])
    attorneys = data.get("attorneys", [])

    attorney_names = {
        entry.get("attorney_name", "").strip().lower()
        for entry in attorneys
        if entry.get("attorney_name")
    }

    # remove attorneys from parties
    parties = [
        party
        for party in parties
        if party.get("party_name", "").strip().lower() not in attorney_names
    ]

    # dedupe parties by case-insensitive name and role
    deduped_parties = []
    seen_party_keys = set()
    for party in parties:
        name_key = party.get("party_name", "").strip().lower()
        role_key = party.get("party_type", "").strip()
        if not name_key:
            continue
        party_key = (name_key, role_key)
        if party_key in seen_party_keys:
            continue
        seen_party_keys.add(party_key)
        deduped_parties.append(party)

    parties = deduped_parties

    # resolve dual plaintiff/defendant roles
    role_map = {}
    for party in parties:
        name_key = party.get("party_name", "").strip().lower()
        if not name_key:
            continue
        role_map.setdefault(name_key, set()).add(party.get("party_type"))

    cleaned_parties = []
    for party in parties:
        name_key = party.get("party_name", "").strip().lower()
        roles = role_map.get(name_key, set())
        if "Plaintiff" in roles and "Defendant" in roles:
            if party.get("party_type") != "Defendant":
                continue
        cleaned_parties.append(party)

    parties = cleaned_parties

    # prefer company plaintiffs
    company_keywords = ("llc", "inc", "corp", "co", "company", "ltd")
    plaintiff_parties = [p for p in parties if p.get("party_type") == "Plaintiff"]
    company_plaintiffs = [
        p
        for p in plaintiff_parties
        if any(keyword in p.get("party_name", "").lower() for keyword in company_keywords)
    ]
    person_plaintiffs = [
        p
        for p in plaintiff_parties
        if p.get("party_name")
        and " " in p.get("party_name")
        and not any(keyword in p.get("party_name", "").lower() for keyword in company_keywords)
    ]

    if company_plaintiffs and person_plaintiffs:
        company_names = {p.get("party_name") for p in company_plaintiffs}
        parties = [
            p
            for p in parties
            if p.get("party_type") != "Plaintiff" or p.get("party_name") in company_names
        ]
        data["first_party_name"] = company_plaintiffs[0].get("party_name")
        data["first_party_type"] = "Plaintiff"

    data["parties"] = parties


def run_gliner_fill(input_path, output_path, model_name, threshold, overwrite, text_path=None):
    data = None
    
    if input_path and os.path.exists(input_path) and not os.path.isdir(input_path):
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
    
    all_parties = plaintiffs + defendants
    party_addresses = extract_addresses_comprehensive(text_for_model, all_parties, input_path, text_path, model)
    
    # extract judge from html
    case_directory = None
    if input_path and os.path.exists(input_path):
        if os.path.isdir(input_path):
            case_directory = input_path
        else:
            case_directory = os.path.dirname(input_path)
    elif text_path and os.path.exists(text_path):
        case_directory = os.path.dirname(text_path)
    
    html_judge = extract_judge_from_html(case_directory)
    
    # extract unpaid amount from ocr text
    unpaid_amount = extract_unpaid_amount(text_for_model)

    case_title = None
    if not case_title and plaintiffs and defendants:
        case_title = f"{plaintiffs[0]} v. {defendants[0]}"

    case_description = None
    case_status = infer_case_status(text_for_model, dispositions)
    first_party_name = plaintiffs[0] if plaintiffs else (defendants[0] if defendants else None)
    first_party_type = "Plaintiff" if plaintiffs else ("Defendant" if defendants else None)
    first_attorney_name = attorneys[0] if attorneys else None
    first_attorney_type = "Attorney" if first_attorney_name else None
    first_event_judge = html_judge or pick_first(grouped, "judge name")

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
    update_if_empty(data, "total_unpaid_amount_defendent", unpaid_amount, overwrite)

    if overwrite or not data.get("parties"):
        data["parties"] = build_parties(plaintiffs, defendants, party_addresses)
    if overwrite or not data.get("attorneys"):
        data["attorneys"] = build_attorneys(attorneys)
    if overwrite or not data.get("dispositions"):
        data["dispositions"] = build_dispositions(dispositions, filed_date, html_judge)

    data["gliner_meta"] = {
        "model": model_name,
        "threshold": threshold,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }

    apply_party_cleanup_rules(data)

    save_json(output_path, data)


# main cli handler
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
