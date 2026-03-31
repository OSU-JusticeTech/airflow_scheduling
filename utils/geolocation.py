"""
Geolocation service that integrates with CURA geocoding service.
"""

import requests
from typing import Dict
import logging

logger = logging.getLogger(__name__)

class GeolocationService:
    
    def __init__(self):
        self.cura_geocode_url = "https://cura-gis-web.asc.ohio-state.edu/arcgis/rest/services/geocoding/USA/GeocodeServer/findAddressCandidates"
        
    def geocode_address(self, address_components: Dict[str, str], timeout: int = 30) -> Dict[str, any]:
        return self._cura_geocode(address_components, timeout=timeout)

    def _cura_geocode(self, address_components: Dict[str, str], timeout: int = 30) -> Dict[str, any]:
        try:
            full_address = self._format_full_address(address_components)
            
            # Parameters for CURA geocoding API
            params = {
                'SingleLine': full_address,
                'f': 'json',
                'outFields': 'Addr_type,Match_addr,StAddr,City,Region,Postal,CountryCode',
                'maxLocations': 1,
                'magicKey': '',
                'searchExtent': '',
                'category': ''
            }
            
            logger.info(f"Geocoding with CURA: {full_address}")
            
            # Make request to CURA geocoding service
            response = requests.get(self.cura_geocode_url, params=params, timeout=timeout, verify=False)
            
            data = response.json()
            
            if 'candidates' in data and len(data['candidates']) > 0:
                candidate = data['candidates'][0]
                location = candidate.get('location', {})
                attributes = candidate.get('attributes', {})
                
                if location.get('x') and location.get('y'):
                    return {
                        'latitude': float(location['y']),
                        'longitude': float(location['x']),
                        'status': 'success',
                        'error_message': None,
                        'address_details': {
                            'Address': attributes.get('Match_addr', full_address),
                            'City': attributes.get('City', address_components.get('city', '')),
                            'Region': attributes.get('Region', address_components.get('state', '')),
                            'Postal': attributes.get('Postal', address_components.get('postal_code', '')),
                            'CountryCode': attributes.get('CountryCode', 'USA'),
                            'Match_Score': candidate.get('score', 0),
                            'Address_Type': attributes.get('Addr_type', '')
                        }
                    }
                else:
                    return {
                        'latitude': None,
                        'longitude': None,
                        'status': 'no_results',
                        'error_message': 'No coordinates returned from geocoding service',
                        'address_details': {}
                    }
            else:
                return {
                    'latitude': None,
                    'longitude': None,
                    'status': 'no_results',
                    'error_message': 'No candidates found for address',
                    'address_details': {}
                }
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error during geocoding: {e}")
            return {
                'latitude': None,
                'longitude': None,
                'status': 'network_error',
                'error_message': f'Network error: {str(e)}',
                'address_details': {}
            }
        except Exception as e:
            logger.error(f"Geocoding error: {e}")
            return {
                'latitude': None,
                'longitude': None,
                'status': 'api_error',
                'error_message': f'Geocoding failed: {str(e)}',
                'address_details': {}
            }
    
    def _format_full_address(self, address_components: Dict[str, str]) -> str:
        """
        Format address components into a single string.
        """
        parts = []
        
        if address_components.get('address_line1'):
            parts.append(address_components['address_line1'])
        if address_components.get('address_line2'):
            parts.append(address_components['address_line2'])
        if address_components.get('city'):
            parts.append(address_components['city'])
        if address_components.get('state'):
            parts.append(address_components['state'])
        if address_components.get('postal_code'):
            parts.append(address_components['postal_code'])
            
        return ', '.join(parts)


def create_geolocation_service():
    """
    Factory function to create a geolocation service instance.
    
    Returns:
        GeolocationService instance
    """
    return GeolocationService()

