import re
from bs4 import BeautifulSoup

from utils.data_insertion import LOG

"""
Extract the parties table block.
"""
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

"""
Extract the attorney table block.
"""
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
            "attorney_name": m.group("name").strip(),
            "address": m.group("address").strip(),
            "city": city,
            "state_zip": state_zip
        })
    return attorneys

"""
Extract the disposition table block.
"""
def extract_disposition_from_text(text):
    start_marker = "Status Date Disposition Code Disposition Date Judge"
    end_marker = "Scroll to top of page"

    # get the block of text for disposition
    start_idx = text.find(start_marker)
    if start_idx == -1:
         LOG.debug("[debug] disposition table header not found")
         return None

    disposition_text = text[start_idx + len(start_marker):]
    end_idx = disposition_text.find(end_marker)
    if end_idx != -1:
        disposition_text = disposition_text[:end_idx].strip()

    # whitespace normalization
    clean_text = re.sub(r'\s+', ' ', disposition_text).strip()

    # regex to capture single disposition record
    disposition_pattern = re.compile(
        r"^(?P<status>CLOSED|OPEN|PENDING|DISMISSED|JUDGMENT|OTHER\sTERMINATION)[/\s]*" 
        r"(?P<status_date>\d{2}/\d{2}/\d{4})\s*"
        r"(?P<code_description>[A-Z\s]+?)" 
        r"(?P<disposition_date>\d{2}/\d{2}/\d{4})\s*"
        r"(?P<judge>[A-Z\s]+)",
        re.I
    )
    
    m = disposition_pattern.match(clean_text)
    
    if m:
        def normalize_date(date_str):
            try:
                m, d, y = date_str.split('/')
                return f"{y}-{m}-{d}"
            except:
                return None
                
        # return record dict
        return {
            "disposition_status": m.group("status").upper(),
            "disposition_status_date": normalize_date(m.group("status_date")),
            "disposition_code_text": m.group("code_description").strip(),
            "disposition_date": normalize_date(m.group("disposition_date")),
            "judge": m.group("judge").strip(),
            # debugging info data
            "disposition_text": clean_text
        }
    else:
        LOG.debug(f"[debug] Disposition record match failed on text: {clean_text[:100]}")
        return None
    

'''
Extract the events table block.
'''
def extract_events_from_text(text):
    LOG.info("[checkpoint] Starting event extraction.")
    events = []

    header = "Events Event Date Start End Judge Ct.Rm. Result"
    idx = text.find(header)
    if idx == -1:
        # print("[DEBUG] Events: header not found")
        return []

    block = text[idx + len(header):]

    # cut at Docket or Scroll — leave spacing intact
    end_match = re.search(r"(Docket|Scroll to top)", block, re.I)
    if end_match:
        block = block[:end_match.start()]

    # print("\n[DEBUG] Raw Events Block (first 200 chars):")
    # print(block[:200])
    # print("---")

    # extract event chunks safely -- no collapsing of spaces TOKENIZING 
    tokens = re.split(r"(\s+)", block)  # keep whitespace
    # Example: ["EVICTION", " ", "HEARING", " ", "-", " ", "FCRS", " ", "05/22/2023", ...]

    # Helper to detect date token
    def is_date(tok):
        return re.match(r"\d{2}/\d{2}/\d{4}", tok) is not None

    # find date positions (event boundaries)
    date_positions = [i for i, tok in enumerate(tokens) if is_date(tok)]

    if not date_positions:
        print("[DEBUG] No dates found → no events")
        return []

    # Add an end marker
    date_positions.append(len(tokens))

    # print(f"[DEBUG] Found {len(date_positions)-1} event boundaries")

    # extract event chunks safely -- no collapsing of spaces
    chunks = []
    for idx in range(len(date_positions) - 1):
        start = date_positions[idx] - 1  
        while start > 0 and not tokens[start].strip():
            start -= 1 

        end = date_positions[idx + 1] - 1
        chunk = "".join(tokens[start:end]).strip()
        chunks.append(chunk)

    print(f"[DEBUG] Extracted {len(chunks)} isolated event chunks")

    # parse each chunk cleanly
    chunk_regex = re.compile(
        r"(?P<name>.*?)\s+"
        r"(?P<date>\d{2}/\d{2}/\d{4})\s+"
        r"(?P<start>\d{2}:\d{2}\s*[AP]M)\s+"
        r"(?P<end>\d{2}:\d{2}\s*[AP]M)\s+"
        r"(?P<judge>\S+)\s+"
        r"(?P<courtroom>\S+)\s+"
        r"(?P<result>.*)",
        re.DOTALL
    )

    for chunk in chunks:
        # print("[DEBUG] Parsing chunk:", chunk[:120], "...")
        m = chunk_regex.match(chunk)
        if not m:
            print("[DEBUG] Chunk parse FAIL:", chunk[:100])
            continue

        mm, dd, yyyy = m.group("date").split("/")
        events.append({
            "event_name": m.group("name").strip(),
            "date": f"{yyyy}-{mm}-{dd}",
            "event_start_time": m.group("start"),
            "event_end_time": m.group("end"),
            "event_judge": m.group("judge"),
            "event_courtroom": m.group("courtroom"),
            "event_result": m.group("result").strip(),
        })

    LOG.info("[checkpoint] Extracted %d events", len(events))
    return events



"""
Extract the docket table block.
"""
def extract_dockets_from_text(text):

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

    # Clean up excess whitespace/newlines into single spaces
    clean_text = re.sub(r'\s+', ' ', docket_text).strip()

    dockets = []
    
    # 💡 REVISED REGEX: Look for the date, followed by text, then two monetary values at the end.
    # The money pattern: [$]?\d+\.\d{2} matches $123.00, $0.00, 123.00, etc.
    # We use a large, optional space capture (\s{1,20}?) to separate text from amount.
    row_pattern = re.compile(
        # Group 1: Date (MM/DD/YYYY)
        r"(?P<date>\d{2}/\d{2}/\d{4})\s*"
        
        # Group 2: Text (Captures everything until it hits the final financial columns)
        # We look for text that precedes two floating point numbers/currency.
        r"(?P<entry_text>.+?)\s*"

        # Group 3: Amount (Matches money format - e.g., $123.00 or just 123.00)
        # Financial columns are often non-greedily separated by spaces.
        r"(?P<amount>\$?\d+\.\d{2})\s*"
        
        # Group 4: Balance (Matches money format)
        r"(?P<balance>\$?\d+\.\d{2})\s*",
        
        re.I
    )
    
    # Check if a specific row in your data (like the last one) has no financial data
    # The last entry in your sample: "PETITION IN FE&D FILED $123.00 $0.00 Receipt: 24641856 Date: 03/20/2024"
    
    # Use a simpler date-to-date pattern and then process the interior if the full row is too complex:
    # ------------------------------------------------------------------------------------------------
    date_split_pattern = re.compile(r"(\d{2}/\d{2}/\d{4})(.+?)(?=\d{2}/\d{2}/\d{4}|$)", re.DOTALL)
    docket_chunks = date_split_pattern.findall(docket_text)
    
    # Define inner pattern for amount/balance inside the text chunk
    inner_pattern = re.compile(
        r"(?P<text>.+?)\s*"
        r"(?P<amount>\$?\d+\.\d{2})\s*"
        r"(?P<balance>\$?\d+\.\d{2})",
        re.I | re.DOTALL
    )
    
    for date_str, chunk_text in docket_chunks:
        amount = None
        balance = None
        entry_text = chunk_text.strip()
        
        # Try to find the financial data at the end of the chunk
        m = inner_pattern.search(entry_text)
        
        if m:
            # If financial data is found, split the entry text accordingly
            amount = m.group("amount").strip()
            balance = m.group("balance").strip()
            # The entry text is everything *before* the amount field was captured
            entry_text = entry_text[:m.start("amount")].strip()
            
        
        # normalize date
        m_parts = date_str.split("/")
        if len(m_parts) == 3:
            m_, d_, y_ = m_parts
            formatted_date = f"{y_}-{int(m_):02d}-{int(d_):02d}"
        else:
            formatted_date = None
        
        dockets.append({
            "docket_date": formatted_date,
            "docket_text": entry_text,
            "docket_amount": amount,
            "docket_balance": balance
        })

    return dockets


"""
Extract structured info from raw HTML/text with proper stripping.
Case title ends at 'Case Information'.
Case description captures text after title up to the first structured section.
"""
def parse_case_html(text):
    result = {
        "case_number": None,
        "case_title": None,
        "case_description": None,
        "case_status": None,
        "case_filed_date": None,
        "parties": [],
        "attorneys": [],
        "disposition": None, 
        "dockets": [],
        "events": []
    }

    raw_text = text
    if "<html" in text.lower():
        soup = BeautifulSoup(text, "html.parser")
        raw_text = soup.get_text(separator="\n")  # Convert HTML to plain text

    # collapse newlines to spaces
    clean_text = " ".join(raw_text.splitlines())

    # case #
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
    result["events"] = extract_events_from_text(raw_text)

    return result
