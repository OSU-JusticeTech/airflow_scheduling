import re
from bs4 import BeautifulSoup

from utils.data_insertion import LOG

# --- parties ---
def extract_parties_from_text(text):
    parties = []
    party_pattern = re.compile(
        r"\d+\s+Name\s+(?P<name>.+?)\s+Type\s+(?P<type>\w+)\s+Address\s+(?P<address>.+?)\s+City\s+(?P<city>.+?)\s+State/Zip\s+(?P<state_zip>.+?)(?:\s|$)",
        re.I
    )
    for m in party_pattern.finditer(text):
        parties.append({
            "type": m.group("type").title(),
            "name": m.group("name").strip(),
            "address": m.group("address").strip(),
            "city": m.group("city").strip(),
            "state_zip": m.group("state_zip").strip()
        })
    return parties

# --- attorneys ---
def extract_attorneys_from_text(text):
    attorneys = []
    atty_pattern = re.compile(
        r"Name:\s*(?P<name>.+?)\s+Party Type:\s*(?P<party_type>.+?)\s+Address:\s*(?P<address>.+?)\s+City/St/Zip:\s*(?P<city_state_zip>.+?)(?:\s|$)",
        re.I
    )
    for m in atty_pattern.finditer(text):
        city_state_zip = m.group("city_state_zip").split(',')
        city = city_state_zip[0].strip()
        state_zip = city_state_zip[1].strip() if len(city_state_zip) > 1 else None
        attorneys.append({
            "party_type": m.group("party_type").title(),
            "name": m.group("name").strip(),
            "address": m.group("address").strip(),
            "city": city,
            "state_zip": state_zip
        })
    return attorneys

# --- disposition ---
def extract_disposition_from_text(text):
    """
    Extract the disposition table block.
    """
    start_marker = "Status Date Disposition Code Disposition Date Judge"
    end_marker = "Scroll to top of page"

    start_idx = text.find(start_marker)
    if start_idx == -1:
        LOG.debug("[debug] disposition table header not found")
        return None

    # slice text from header onward
    disposition_text = text[start_idx:]
    end_idx = disposition_text.find(end_marker)
    if end_idx != -1:
        disposition_text = disposition_text[:end_idx].strip()

    # try to extract first status and date from this block
    status_match = re.search(r"(CLOSED|OPEN|PENDING|DISMISSED)\s+([0-9]{2}/[0-9]{2}/[0-9]{4})", disposition_text, re.I)
    status = status_match.group(1).upper() if status_match else None
    status_date = status_match.group(2) if status_match else None

    return {
        "status": status,
        "status_date": status_date,
        "disposition_text": disposition_text
    }

# --- docket entries ---
def extract_dockets_from_text(text):
    """
    Extract the docket table block.
    """
    start_marker = "Docket Date Text Amount Balance"
    end_marker = "Scroll to top of page"

    start_idx = text.find(start_marker)
    if start_idx == -1:
        LOG.debug("[debug] docket table header not found")
        return []

    docket_text = text[start_idx:]
    end_idx = docket_text.find(end_marker)
    if end_idx != -1:
        docket_text = docket_text[:end_idx].strip()

    # now extract each row
    dockets = []
    # match lines like: 02/24/2025 DISMISSED BY PLAINTIFF ... (date first, then text)
    row_pattern = re.compile(r"(\d{2}/\d{2}/\d{4})\s+(.+?)(?:\s{2,}|$)", re.I)
    for m in row_pattern.finditer(docket_text):
        date_str = m.group(1).strip()
        entry_text = m.group(2).strip()
        # normalize date
        m_parts = date_str.split("/")
        if len(m_parts) == 3:
            m_, d_, y_ = m_parts
            formatted_date = f"{y_}-{int(m_):02d}-{int(d_):02d}"
        else:
            formatted_date = None
        dockets.append({
            "date": formatted_date,
            "entry": entry_text
        })

    return dockets




def parse_case_html(text):
    """
    Extract structured info from raw HTML/text with proper stripping.
    Case title ends at 'Case Information'.
    Case description captures text after title up to the first structured section.
    """
    result = {
        "case_number": None,
        "case_title": None,
        "case_description": None,
        "case_status": None,
        "case_filed_date": None,
        "parties": [],
        "attorneys": [],
        "disposition": None, 
        "dockets": []
    }

    raw_text = text
    if "<html" in text.lower():
        soup = BeautifulSoup(text, "html.parser")
        raw_text = soup.get_text(separator="\n")  # Convert HTML to plain text

    # collapse newlines to spaces for robust matching
    clean_text = " ".join(raw_text.splitlines())

    # case number
    match = re.search(
        r"Case\s+(?:No\.?|Number)\s*[:\-]?\s*([0-9]{4}\s*[A-Z]{3}\s*\d{6})",
        clean_text,
        re.I
    )
    if match:
        result["case_number"] = match.group(1).strip()

    # filed date
    match = re.search(r"Filed[:\s]+([0-9/]+)", clean_text, re.I)
    if match:
        m, d, y = match.group(1).split("/")
        result["case_filed_date"] = f"{y}-{int(m):02d}-{int(d):02d}"

    # case status
    match = re.search(r"Status[:\s]+([A-Z]+)", clean_text, re.I)
    if match:
        result["case_status"] = match.group(1).strip()

    # case title ends at 'Case Information'
    title_match = re.search(r"(.*?Case Information)", raw_text, re.I)
    if title_match:
        result["case_title"] = title_match.group(1).strip()

    # description: text after title but stop before structured sections
    sections = ["Overview", "Parties", "Attorneys", "Disposition", "Financial Summary", 
                "Receipts", "Events", "Docket", "Print", "PDF", "Help"]
    title_end_idx = raw_text.find(result["case_title"]) + len(result["case_title"]) if result["case_title"] else 0
    remaining_text = raw_text[title_end_idx:]
    stop_idx = len(remaining_text)
    for sec in sections:
        idx = remaining_text.find(sec)
        if idx != -1 and idx < stop_idx:
            stop_idx = idx
    result["case_description"] = remaining_text[:stop_idx].strip() if stop_idx > 0 else None


    result["parties"] = extract_parties_from_text(raw_text) 
    result["attorneys"] = extract_attorneys_from_text(raw_text) 
    result["disposition"] = extract_disposition_from_text(raw_text)
    result["dockets"] = extract_dockets_from_text(raw_text)

    return result
