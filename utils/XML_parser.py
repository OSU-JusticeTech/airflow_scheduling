from bs4 import BeautifulSoup
import datetime
from pprint import pprint
from collections import defaultdict
import json

import glob
from tqdm import tqdm
from multiprocessing import Pool

from utils.pyschema import Case

def parse_case(fn):
    # Parse HTML with BeautifulSoup
    with open(fn) as f:
        html = f.read()
    soup = BeautifulSoup(html, 'html.parser')

    # Extract plaintiff and defendant from the first <td> tag

    # Extract case number, status, and filed date from the second <td> tag
    case_info = soup.find_all('td', class_='page_header')[1].text.strip()
    case_no = case_info.split("Case No.")[1].split("\n")[0].strip()
    status = case_info.split("Status:")[1].split("\n")[0].strip()
    filed_date = case_info.split("Filed:")[1].strip()

    # Parse and format the Filed Date into MM/DD/YYYY format
    filed_date_obj = datetime.datetime.strptime(filed_date, "%m/%d/%Y")

    parsed_data = {
        'Case Number': case_no,
        'Status': status,
        'Filed Date': filed_date_obj,
    }
    # Find the table with id 'pty_table'
    table = soup.find('table', {'id': 'pty_table'})

    # Initialize a list to store the party data
    parties = []

    # Iterate through each row in the table
    rows = table.find_all('tr')
    current_party = None

    for row in rows:
        cells = row.find_all('td')
        title = None
        # print("cells of row", cells)
        for cell in cells:
            if cell.get("rowspan") is not None:
                party_id = cell.get_text(strip=True)
                if current_party is not None:
                    parties.append(current_party)
                current_party = {"party_number": party_id}
            if cell.get('class') == ["title"]:
                title = cell.get_text(strip=True)
            if cell.get('class') == ["data"] and title is not None:
                data = cell.decode_contents().replace("<br/>", "\n")  # .get_text(strip=True)
                current_party[title] = data
                title = None

    # Don't forget to append the last party's data
    if current_party:
        parties.append(current_party)

    # Find the table with id 'atty_table'
    table = soup.find('table', {'id': 'atty_table'})

    if table is not None:
        # Extract attorney details
        attorneys = []

        # Iterate through each row in the table
        rows = table.find_all('tr')

        current_attorney = None
        assert len(rows) % 2 == 0
        for rid, row in enumerate(rows):
            cells = row.find_all('td')
            if rid % 2 == 0 and current_attorney is not None:
                attorneys.append(current_attorney)
                current_attorney = None
            if current_attorney is None:
                current_attorney = {}
            for cell in cells:
                if cell.get('class') == ["title"]:
                    title = cell.get_text(strip=True)
                if cell.get('class') == ["data"] and title is not None:
                    data = cell.decode_contents().replace("<br/>", "\n")  # .get_text(strip=True)
                    current_attorney[title] = data
                    title = None
        if current_attorney is not None:
            attorneys.append(current_attorney)
        parsed_data['Attorneys'] = attorneys

    table = soup.find('table', {'id': 'dsp_table'})

    if table is not None:
        # Extract the data rows
        header = table.find_all('tr')[0]
        columns = [a.get_text() for a in header.find_all('td', class_="title")]

        disp = []
        for row in table.find_all('tr')[1:]:
            data = [a.decode_contents().replace("<br/>", "\n") for a in row.find_all('td', class_="data")]
            # print(data)
            disp.append(dict(zip(columns, data)))
        parsed_data['Disposition'] = disp

    table = soup.find('table', {'id': 'dkt_appl_table'})

    if table is not None:
        # Extract the data rows
        header = table.find_all('tr')[0]
        columns = [a.get_text() for a in header.find_all('td', class_="title")]

        costs = []
        for row in table.find_all('tr')[1:]:
            data = [a.decode_contents().replace("<br/>", "\n") for a in row.find_all('td', class_="data")]
            # print(data)
            costs.append(dict(zip(columns, data)))
        parsed_data['Finances'] = costs

    table = soup.find('table', {'id': 'evnt_table'})

    if table is not None:
        # Extract the data rows
        header = table.find_all('tr')[0]
        columns = [a.get_text() for a in header.find_all('td', class_="title")]

        events = []
        for row in table.find_all('tr')[1:]:
            data = [a.decode_contents().replace("<br/>", "\n") for a in row.find_all('td', class_="data")]
            # print(data)
            events.append(dict(zip(columns, data)))
        parsed_data['Events'] = events

    # Find the table with id 'dkt_table'
    table = soup.find('table', {'id': 'dkt_table'})

    # Extract the data rows
    docket = []
    current_entry = None
    if table is not None:
        for row in table.find_all('tr')[1:]:  # Skip the first row which contains the headers
            cells = row.find_all('td')
            if row.get('class') == ["dkt_text"]:
                current_entry["extra"] = cells[1].decode_contents()
            else:
                if current_entry is not None:
                    docket.append(current_entry)
                current_entry = {"Date": datetime.datetime.strptime(cells[0].get_text(strip=True), "%m/%d/%Y"),
                                "Text": cells[1].get_text(strip=True),
                                "Amount": cells[2].get_text(strip=True),
                                "Balance": cells[3].get_text(strip=True)
                                }
        if current_entry is not None:
            docket.append(current_entry)

    # Output the parsed data
    parsed_data.update({
        'Parties': parties,
        'Docket': docket
    })

    return parsed_data

def parse_validate(fn):
    try:
        c = parse_case(fn)
        return make_case(c)
    except Exception as e:
        print("error in case", fn)
        print(e.__repr__())
        raise e

def parse_case_html(html_content):
    """Parse case from HTML content string (not file)"""
    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extract plaintiff and defendant from the first <td> tag

    # Extract case number, status, and filed date from the second <td> tag
    case_info = soup.find_all('td', class_='page_header')[1].text.strip()
    case_no = case_info.split("Case No.")[1].split("\n")[0].strip()
    status = case_info.split("Status:")[1].split("\n")[0].strip()
    filed_date = case_info.split("Filed:")[1].strip()

    # Parse and format the Filed Date into MM/DD/YYYY format
    filed_date_obj = datetime.datetime.strptime(filed_date, "%m/%d/%Y")

    parsed_data = {
        'Case Number': case_no,
        'Status': status,
        'Filed Date': filed_date_obj,
    }
    # Find the table with id 'pty_table'
    table = soup.find('table', {'id': 'pty_table'})

    # Initialize a list to store the party data
    parties = []

    # Iterate through each row in the table
    rows = table.find_all('tr')
    current_party = None

    for row in rows:
        cells = row.find_all('td')
        title = None
        # print("cells of row", cells)
        for cell in cells:
            if cell.get("rowspan") is not None:
                party_id = cell.get_text(strip=True)
                if current_party is not None:
                    parties.append(current_party)
                current_party = {"party_number": party_id}
            if cell.get('class') == ["title"]:
                title = cell.get_text(strip=True)
            if cell.get('class') == ["data"] and title is not None:
                data = cell.decode_contents().replace("<br/>", "\n")  # .get_text(strip=True)
                current_party[title] = data
                title = None

    # Don't forget to append the last party's data
    if current_party:
        parties.append(current_party)

    # Find the table with id 'atty_table'
    table = soup.find('table', {'id': 'atty_table'})

    if table is not None:
        # Extract attorney details
        attorneys = []

        # Iterate through each row in the table
        rows = table.find_all('tr')

        current_attorney = None
        assert len(rows) % 2 == 0
        for rid, row in enumerate(rows):
            cells = row.find_all('td')
            if rid % 2 == 0 and current_attorney is not None:
                attorneys.append(current_attorney)
                current_attorney = None
            if current_attorney is None:
                current_attorney = {}
            for cell in cells:
                if cell.get('class') == ["title"]:
                    title = cell.get_text(strip=True)
                if cell.get('class') == ["data"] and title is not None:
                    data = cell.decode_contents().replace("<br/>", "\n")  # .get_text(strip=True)
                    current_attorney[title] = data
                    title = None
        if current_attorney is not None:
            attorneys.append(current_attorney)
        parsed_data['Attorneys'] = attorneys

    table = soup.find('table', {'id': 'dsp_table'})

    if table is not None:
        # Extract the data rows
        header = table.find_all('tr')[0]
        columns = [a.get_text() for a in header.find_all('td', class_="title")]

        disp = []
        for row in table.find_all('tr')[1:]:
            data = [a.decode_contents().replace("<br/>", "\n") for a in row.find_all('td', class_="data")]
            # print(data)
            disp.append(dict(zip(columns, data)))
        parsed_data['Disposition'] = disp

    table = soup.find('table', {'id': 'dkt_appl_table'})

    if table is not None:
        # Extract the data rows
        header = table.find_all('tr')[0]
        columns = [a.get_text() for a in header.find_all('td', class_="title")]

        costs = []
        for row in table.find_all('tr')[1:]:
            data = [a.decode_contents().replace("<br/>", "\n") for a in row.find_all('td', class_="data")]
            # print(data)
            costs.append(dict(zip(columns, data)))
        parsed_data['Finances'] = costs

    table = soup.find('table', {'id': 'evnt_table'})

    if table is not None:
        # Extract the data rows
        header = table.find_all('tr')[0]
        columns = [a.get_text() for a in header.find_all('td', class_="title")]

        events = []
        for row in table.find_all('tr')[1:]:
            data = [a.decode_contents().replace("<br/>", "\n") for a in row.find_all('td', class_="data")]
            # print(data)
            events.append(dict(zip(columns, data)))
        parsed_data['Events'] = events

    # Find the table with id 'dkt_table'
    table = soup.find('table', {'id': 'dkt_table'})

    # Extract the data rows
    docket = []
    current_entry = None
    if table is not None:
        for row in table.find_all('tr')[1:]:  # Skip the first row which contains the headers
            cells = row.find_all('td')
            if row.get('class') == ["dkt_text"]:
                current_entry["extra"] = cells[1].decode_contents()
            else:
                if current_entry is not None:
                    docket.append(current_entry)
                current_entry = {"Date": datetime.datetime.strptime(cells[0].get_text(strip=True), "%m/%d/%Y"),
                                "Text": cells[1].get_text(strip=True),
                                "Amount": cells[2].get_text(strip=True),
                                "Balance": cells[3].get_text(strip=True)
                                }
        if current_entry is not None:
            docket.append(current_entry)

    # Output the parsed data
    parsed_data.update({
        'Parties': parties,
        'Docket': docket
    })

    return parsed_data


def make_case(c):
    sanitise = {"case_number": c["Case Number"],
                "parties": [],
                "docket": [{**{k.lower(): v for k, v in p.items()},**{"balance":p["Balance"][1:].replace(",","") if p["Balance"] != "" else None,
                                                                     "amount":p["Amount"][1:].replace(",","") if p["Amount"] != "" else None}} for p in c["Docket"]],
                "dispositions": [{**{k.lower(): v for k, v in d.items()},
                                  **{'code': d['Disposition Code'],
                                     'date': None if d['Disposition Date'] == "" else datetime.datetime.strptime(
                                         d['Disposition Date'], "%m/%d/%Y").date(),
                                     'status_date': datetime.datetime.strptime(d['Status Date'], "%m/%d/%Y").date()}}
                                 for d in c.get("Disposition",[])],
                "attorneys": [],
                "finances": [],
                "events": []}
    #print(sanitise["dispositions"])
    for p in c["Parties"]:
        
        # Try enhanced party data processing first
        try:
            party_address = process_party_data(p)
            if party_address and hasattr(party_address, 'address'):
                # Successfully created SideAddress object from party - keep as object
                sanitise["parties"].append(party_address)
                continue
        except Exception as e:
            pass
        # Fallback to original processing
        sanp = {k.lower(): v for k, v in p.items()}
        if 'State/Zip' in p:
            sanp.update({'state': p['State/Zip'].split("/")[0],
                         'zip': p['State/Zip'].split("/")[1],
                         'address': p.get('Address', "").split("\n")})
        # print("sanp", sanp)
        sanitise["parties"].append(sanp)
    if "Attorneys" in c:
        sanitise["attorneys"] = [process_attorney_data(p) for p in c["Attorneys"]]
    if "Events" in c:
        sanitise["events"] = [{**{k.lower(): v for k, v in e.items()},
                               **{"room": e["Ct.Rm."],
                                  "start": datetime.datetime.strptime(e["Date"] + e["Start"], "%m/%d/%Y%I:%M %p"),
                                  "end": datetime.datetime.strptime(e["Date"] + e["End"], "%m/%d/%Y%I:%M %p")}} for e in
                              c["Events"]]
    if "Finances" in c:
        sanitise["finances"] = [{**{k[7:].lower(): v[1:].replace(",","") if v.startswith("$") else v.replace("<b>","").replace("</b>","") for k, v in e.items()},
                               **{"balance": e["Balance"][1:].replace(",",""),
                                  }} for e in
                              c["Finances"]]

    
    case = Case(**sanitise)
    return case

def process_attorney_data(attorney_raw_data):
    """Process attorney data and create proper Attorney objects"""
    try:
        # Build attorney data dict with safer parsing
        attorney_data = {k.lower().rstrip(':'): v for k, v in attorney_raw_data.items()}
        
        # Safely parse Party Type
        if 'Party Type:' in attorney_raw_data:
            party_type_parts = attorney_raw_data['Party Type:'].split(" - ")
            attorney_data['type'] = party_type_parts[0] if len(party_type_parts) > 0 else ''
            attorney_data['role'] = party_type_parts[1] if len(party_type_parts) > 1 else ''
        
        # Safely parse Address
        if 'Address:' in attorney_raw_data:
            attorney_data['address'] = attorney_raw_data["Address:"].split("\n")
        
        # Safely parse City/St/Zip with consistent approach
        if 'City/St/Zip:' in attorney_raw_data:
            city_state_zip = attorney_raw_data['City/St/Zip:']
            try:
                # Split by comma first
                city_part = city_state_zip.split(",")[0].strip()
                state_zip_part = city_state_zip.split(",")[1].strip()
                
                # Split state and zip by space
                state_zip_parts = state_zip_part.split()
                
                attorney_data['city'] = city_part
                attorney_data['state'] = state_zip_parts[0] if len(state_zip_parts) > 0 else ''
                attorney_data['zip'] = state_zip_parts[1] if len(state_zip_parts) > 1 else ''
                
            except (IndexError, AttributeError) as e:
                attorney_data['city'] = ''
                attorney_data['state'] = ''
                attorney_data['zip'] = ''
    except Exception as e:
        return attorney_raw_data
    
    # Try to create Attorney object directly if we have all required fields
    if all(field in attorney_data and attorney_data[field] for field in ['city', 'state', 'zip', 'role']):
        try:
            from utils.pyschema import Attorney
            attorney_obj = Attorney(**attorney_data)
            return attorney_obj
        except Exception as e:
            pass

    # Return the data dict for Pydantic Union resolution
    return attorney_data
    
    # Try to create Attorney object directly if we have all required fields
    if all(field in attorney_data and attorney_data[field] for field in ['city', 'state', 'zip', 'role']):
        try:
            from utils.pyschema import Attorney
            attorney_obj = Attorney(**attorney_data)
            return attorney_obj
        except Exception as e:
            pass

    # Return the data dict for Pydantic Union resolution
    return attorney_data

def process_party_data(party_raw_data):
    """Process party data and create proper Party objects with enhanced address extraction"""    
    # Build party data dict
    party_data = {k.lower(): v for k, v in party_raw_data.items()}
    
    # Enhanced party address extraction logic
    has_address = False
    
    # Check for State/Zip format first (existing logic)
    if 'State/Zip' in party_raw_data:
        party_data.update({
            'state': party_raw_data['State/Zip'].split("/")[0],
            'zip': party_raw_data['State/Zip'].split("/")[1],  
            'address': party_raw_data.get('Address', "").split("\n")
        })
        has_address = True
        
    if 'City' in party_raw_data:
        party_data.update({
            'city': party_raw_data['City'].strip(),
        })
        has_address = True
    
    # Check for individual address fields
    elif any(field in party_raw_data for field in ['Address:', 'Address', 'City:', 'City']):
        address_lines = []
        
        # Extract address
        if 'Address:' in party_raw_data and party_raw_data['Address:'].strip():
            address_lines = party_raw_data['Address:'].split("\n")
        elif 'Address' in party_raw_data and party_raw_data['Address'].strip():
            address_lines = party_raw_data['Address'].split("\n")

        if address_lines:
            party_data.update({
                'address': address_lines,
            })
            has_address = True
            
    if has_address:
        # Try to create SideAddress object directly if we have all required fields
        if all(field in party_data and party_data[field] for field in ['city', 'state', 'zip']):
            try:
                from utils.pyschema import SideAddress
                party_obj = SideAddress(**party_data)
                return party_obj
            except Exception as e:
                pass
    else:
        pass

    # Return the data dict for Pydantic Union resolution
    return party_data