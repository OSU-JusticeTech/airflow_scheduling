#!/usr/bin/env python3

import argparse
import json
import os
import re
import sys
from pathlib import Path

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None


def parse_address_string(address_str):
    """parse address string into structured components"""
    if not address_str:
        return None
    
    # improved parsing for structured address components
    address_str = address_str.strip()
    full_address = address_str
    
    # extract zip code
    zip_match = re.search(r'\b(\d{5}(?:-\d{4})?)\b', address_str)
    postal_code = zip_match.group(1) if zip_match else None
    
    # extract state (2 letter code before zip)
    state_match = re.search(r'\b([A-Z]{2})\s+\d{5}', address_str)
    state = state_match.group(1) if state_match else None
    
    # extract city (word(s) before state)
    if state:
        city_pattern = rf'([A-Z\s]+?)\s*,?\s*{re.escape(state)}\s'
        city_match = re.search(city_pattern, address_str)
        if city_match:
            city = city_match.group(1).strip().rstrip(',')
        else:
            city = None
    else:
        city = None
    
    # extract street address (everything before city or comma)
    if city:
        street_pattern = rf'(.*?)\s*,?\s*{re.escape(city)}'
        street_match = re.search(street_pattern, address_str)
        if street_match:
            address_line1 = street_match.group(1).strip().rstrip(',')
        else:
            address_line1 = address_str
    else:
        # fallback: take everything before last comma or the whole string
        parts = address_str.split(',')
        address_line1 = parts[0].strip() if parts else address_str
    
    return {
        "address_type": "Physical",
        "address_line1": address_line1,
        "address_line2": full_address,
        "city": city,
        "state": state,
        "country": "USA",
        "postal_code": postal_code
    }


def extract_addresses_from_html(case_folder):
    """extract addresses from printable.html in the case folder"""
    addresses = []
    
    if not case_folder or not os.path.exists(case_folder):
        return addresses
    
    printable_html_path = os.path.join(case_folder, "printable.html")
    if not os.path.exists(printable_html_path):
        print(f"    no printable.html found in {case_folder}")
        return addresses
    
    try:
        if BeautifulSoup is None:
            print("    beautifulsoup not available, skipping html extraction")
            return addresses
        
        with open(printable_html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        html_text = soup.get_text()
        
        # proven patterns that work across different case formats
        address_patterns = [
            r'premises at ([^.\n]+)',
            r'serve defendant[^\n]*at ([^.\n]+)',
            r'at (\d+\s+[A-Z][A-Z\s]+(?:ST|STREET|AVE|AVENUE|DR|DRIVE|RD|ROAD|BLVD|BOULEVARD)(?:\s+(?:APT|APARTMENT|UNIT|STE|SUITE|#)\s*\w+)?\s*,?\s*[A-Z]+\s*,?\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?)',
        ]
        
        for pattern in address_patterns:
            matches = re.findall(pattern, html_text, re.IGNORECASE)
            for match in matches:
                clean_match = match.strip()
                if is_reasonable_address(clean_match):
                    parsed_addr = parse_address_string(clean_match)
                    if parsed_addr:
                        addresses.append(parsed_addr)
                        print(f"    extracted address: {parsed_addr.get('address_line1', '')}")
        
        # remove duplicates while preserving order
        unique_addresses = []
        seen = set()
        for addr in addresses:
            addr_key = f"{addr.get('address_line1', '')}{addr.get('city', '')}{addr.get('postal_code', '')}"
            if addr_key not in seen:
                seen.add(addr_key)
                unique_addresses.append(addr)
        
        return unique_addresses
    
    except Exception as e:
        print(f"    error extracting addresses from html: {e}")
        return addresses


def is_reasonable_address(value):
    """check if the address value looks like a real address"""
    if not value:
        return False
    
    value = value.strip()
    if len(value) < 5 or len(value) > 150:
        return False
    
    # must contain at least one number
    if not re.search(r"\d", value):
        return False
    
    # reject court/legal terms
    if re.search(r"\b(case|court|hearing|plaintiff|defendant|attorney|judge|clerk|municipal|courtroom)\b", value, re.IGNORECASE):
        return False
    
    return True


def extract_judge_from_html(case_folder):
    """extract judge name from printable.html in the case folder"""
    judge_name = None
    
    if not case_folder or not os.path.exists(case_folder):
        return judge_name
    
    printable_html_path = os.path.join(case_folder, "printable.html")
    if not os.path.exists(printable_html_path):
        return judge_name
    
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
    
    except Exception as e:
        print(f"    error extracting judge from html: {e}")
    
    return judge_name


def update_json_with_html_data(json_path, extracted_addresses, extracted_judge):
    """update the json file with extracted addresses and judge"""
    if not os.path.exists(json_path):
        print(f"    json file not found: {json_path}")
        return False
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        updated_addresses = False
        updated_judge = False
        
        # update party addresses (defendants)
        if extracted_addresses:
            parties = data.get("parties", [])
            defendant_count = 0
            
            for party in parties:
                if party.get("party_type", "").lower() == "defendant":
                    if defendant_count < len(extracted_addresses):
                        party["address"] = extracted_addresses[defendant_count]
                        print(f"    updated address for defendant: {party.get('party_name', 'unknown')}")
                        defendant_count += 1
                        updated_addresses = True
        
        # update judge information
        if extracted_judge:
            # update dispositions
            dispositions = data.get("dispositions", [])
            for disposition in dispositions:
                if not disposition.get("judge"):
                    disposition["judge"] = extracted_judge
                    updated_judge = True
            
            # update first_event_judge if it exists
            if "first_event_judge" in data and not data["first_event_judge"]:
                data["first_event_judge"] = extracted_judge
                updated_judge = True
            
            if updated_judge:
                print(f"    updated judge: {extracted_judge}")
        
        # save the updated json
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        updates = []
        if updated_addresses:
            updates.append(f"{defendant_count if extracted_addresses else 0} addresses")
        if updated_judge:
            updates.append("judge")
        
        if updates:
            print(f"    updated: {', '.join(updates)}")
            return True
        else:
            print("    no updates made")
            return True
    
    except Exception as e:
        print(f"    error updating json: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Extract addresses and judges from printable.html and update JSON")
    parser.add_argument("case_folder", help="Path to the case folder containing printable.html")
    args = parser.parse_args()
    
    case_folder = Path(args.case_folder)
    if not case_folder.exists():
        print(f"error: case folder does not exist: {case_folder}")
        return 1
    
    # extract case id from folder name (e.g., CVG-20-00287)
    case_id = case_folder.name
    
    # find the corresponding json file in ocr/texts/
    script_dir = Path(__file__).parent
    json_output_dir = script_dir / "texts"
    json_path = json_output_dir / f"{case_id}.json"
    
    print(f"processing html extraction for {case_id}")
    print(f"  case folder: {case_folder}")
    print(f"  json file: {json_path}")
    
    # extract addresses from html
    addresses = extract_addresses_from_html(str(case_folder))
    
    # extract judge from html
    judge = extract_judge_from_html(str(case_folder))
    
    if not addresses and not judge:
        print("  no addresses or judge extracted")
        return 0
    
    # update json file
    if update_json_with_html_data(str(json_path), addresses, judge):
        print(f"  successfully updated html data for {case_id}")
        return 0
    else:
        print(f"  failed to update html data for {case_id}")
        return 1


if __name__ == "__main__":
    sys.exit(main())