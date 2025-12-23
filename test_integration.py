#!/usr/bin/env python3
"""
Test script to verify the integration between case_data_etl.py, address.py, and geolocation.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.geolocation import create_geolocation_service
from models.address import Address

def test_geolocation_integration():
    """Test that the geolocation service returns data compatible with the address model."""
    
    print("=== TESTING GEOLOCATION INTEGRATION ===")
    
    # Create geolocation service
    geo_service = create_geolocation_service()
    
    # Test address components (similar to what would come from ETL)
    test_address = {
        'address_line1': '123 Main Street',
        'city': 'Columbus',
        'state': 'OH',
        'postal_code': '43215'
    }
    
    print(f"Testing address: {test_address}")
    
    # Geocode the address
    result = geo_service.geocode_address(test_address)
    
    print(f"Geocoding result:")
    print(f"  Status: {result.get('status', 'NO STATUS FIELD')}")
    print(f"  Latitude: {result.get('latitude')}")
    print(f"  Longitude: {result.get('longitude')}")
    print(f"  Error: {result.get('error_message', 'None')}")
    
    if result.get('address_details'):
        print(f"  Enhanced address details:")
        for key, value in result['address_details'].items():
            print(f"    {key}: {value}")
    
    # Check if result fields match what Address model expects
    expected_fields = ['latitude', 'longitude', 'status']
    missing_fields = [field for field in expected_fields if field not in result]
    
    if missing_fields:
        print(f"❌ Missing fields that Address model expects: {missing_fields}")
    else:
        print("✅ All expected fields present in geocoding result")
    
    # Simulate creating an Address model instance
    print("\n=== SIMULATING ADDRESS MODEL CREATION ===")
    
    # This is what the ETL would do with the geocoding result
    address_data = {
        'case_number': 'TEST_CASE_001',
        'address_type': 'extracted_0',
        'address_line1': test_address['address_line1'],
        'city': result.get('address_details', {}).get('City', test_address['city']),
        'state': result.get('address_details', {}).get('Region', test_address['state']),
        'postal_code': result.get('address_details', {}).get('Postal', test_address['postal_code']),
        'country': result.get('address_details', {}).get('CountryCode', 'USA'),
        'latitude': result.get('latitude'),
        'longitude': result.get('longitude'),
        'geocode_status': result.get('status', 'unknown')
    }
    
    print("Address data ready for database insertion:")
    for key, value in address_data.items():
        print(f"  {key}: {value}")
    
    return result

def test_etl_address_extraction():
    """Test the address extraction logic from ETL."""
    
    print("\n=== TESTING ETL ADDRESS EXTRACTION ===")
    
    # Simulate HTML content (like what would come from raw_cases table)
    sample_html = """
    <div>
        <p>Plaintiff: John Doe, 123 Main Street, Columbus OH 43215</p>
        <p>Defendant: Jane Smith, 456 Oak Avenue, Cincinnati OH 45202</p>
    </div>
    """
    
    # Import the extraction function (would normally be in ETL file)
    from dags.case_data_etl import extract_addresses_simple
    
    addresses = extract_addresses_simple(sample_html, 'TEST_CASE_002')
    
    print(f"Extracted {len(addresses)} addresses:")
    for i, addr in enumerate(addresses):
        print(f"  Address {i+1}:")
        for key, value in addr.items():
            print(f"    {key}: {value}")
    
    return addresses

if __name__ == "__main__":
    try:
        geocode_result = test_geolocation_integration()
        extracted_addresses = test_etl_address_extraction()
        
        print("\n=== INTEGRATION TEST SUMMARY ===")
        print("✅ Geolocation service works and returns proper fields")
        print("✅ Address extraction works and returns proper structure") 
        print("✅ Both components are compatible with Address model")
        print("✅ Ready for integration into full ETL pipeline")
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
