"""
Geolocation service that integrates with CURA geocoding service.
"""

import time
import requests
from typing import Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class GeolocationService:
    
    def __init__(self):
        """
        Initialize the geolocation service.
        """
        self.cura_geocode_url = "https://cura-gis-web.asc.ohio-state.edu/arcgis/rest/services/geocoding/USA/GeocodeServer/findAddressCandidates"
        
    def geocode_address(self, address_components: Dict[str, str]) -> Dict[str, any]:
        """
        Geocode an address and return coordinates using CURA geocoding service.
        
        Args:
            address_components: Dictionary containing address parts:
                - address_line1: Street address
                - address_line2: Apartment/suite (optional)
                - city: City name
                - state: State
                - postal_code: ZIP code
                
        Returns:
            Dictionary containing:
                - latitude: Float or None
                - longitude: Float or None
                - status: 'success', 'no_results', 'api_error', 'network_error'
                - error_message: String if status is not 'success'
                - address_details: Enhanced address information if available
        """
        
        return self._cura_geocode(address_components)

    def _cura_geocode(self, address_components: Dict[str, str]) -> Dict[str, any]:
        """
        Actual CURA geocoding service implementation.
        """
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
            response = requests.get(self.cura_geocode_url, params=params, timeout=30, verify=False)
            
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

    def geocode_batch(self, address_list: list) -> list:
        """
        Geocode multiple addresses in batch.
        
        Args:
            address_list: List of address component dictionaries
            
        Returns:
            List of geocoding results in same order as input
        """
        results = []
        for i, address_components in enumerate(address_list):
            try:
                result = self.geocode_address(address_components)
                result['batch_index'] = i
                results.append(result)
            except Exception as e:
                logger.error(f"Error geocoding address {i}: {e}")
                results.append({
                    'latitude': None,
                    'longitude': None,
                    'status': 'failed',
                    'error_message': str(e),
                    'batch_index': i,
                    'address_details': {}
                })
        return results


def create_geolocation_service(use_mock=False, geocoding_service="cura"):
    """
    Factory function to create a geolocation service instance.
    
    Args:
        use_mock: Not used anymore - kept for backward compatibility
        geocoding_service: Service type - only "cura" is supported
        
    Returns:
        GeolocationService instance
    """
    return GeolocationService()

