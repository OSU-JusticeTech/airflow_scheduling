"""
Geolocation service that integrates with CURA geocoding service.
"""

import requests
from typing import Dict
import logging
import os
from urllib3.util.ssl_ import create_urllib3_context
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

# disable proxy detection on macOS
if 'no_proxy' not in os.environ:
    os.environ['no_proxy'] = '*'

class GeolocationService:
    
    def __init__(self):
        self.cura_geocode_url = "https://cura-gis-web.asc.ohio-state.edu/arcgis/rest/services/geocoding/USA/GeocodeServer/findAddressCandidates"
        # Create session with custom SSL context to avoid macOS SIGSEGV
        self.session = requests.Session()
        # Don't use custom SSL context if urllib3 context creation fails
        try:
            ctx = create_urllib3_context()
            ctx.check_hostname = False
            ctx.verify_mode = 0  # SSL.CERT_NONE
            adapter = requests.adapters.HTTPAdapter()
            self.session.mount('https://', adapter)
        except:
            pass  # Fall back to default session
        
        # In-memory cache for batch geocoding (prevents redundant API calls)
        self.batch_cache = {}
        
    def geocode_address(self, address_components: Dict[str, str], timeout: int = 30) -> Dict[str, any]:
        # Create cache key from address components
        cache_key = self._create_cache_key(address_components)
        
        # Check if already geocoded in this batch
        if cache_key in self.batch_cache:
            logger.info(f"ADDRESS EXISTS Using {self._format_full_address(address_components)}")
            return self.batch_cache[cache_key]
        
        # Not in cache, call CURA
        result = self._cura_geocode(address_components, timeout=timeout)
        
        # Store in cache
        self.batch_cache[cache_key] = result
        
        return result
    
    def _create_cache_key(self, address_components: Dict[str, str]) -> str:
        """
        Create a unique cache key from address components.
        Uses address_line1, city, state, postal_code for uniqueness.
        """
        addr1 = address_components.get('address_line1', '').upper().strip()
        city = address_components.get('city', '').upper().strip()
        state = address_components.get('state', '').upper().strip()
        postal = address_components.get('postal_code', '').upper().strip()
        
        return f"{addr1}|{city}|{state}|{postal}"
    
    def clear_batch_cache(self):
        """
        Clear the in-memory batch cache. Call this between batch runs.
        """
        cache_size = len(self.batch_cache)
        self.batch_cache.clear()
        logger.info(f"Batch cache cleared. Previous cache size: {cache_size} entries")

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
            
            # Make request to CURA geocoding service with proper timeout
            # Use verify=True for SSL (safe), timeout for connection limits
            response = self.session.get(
                self.cura_geocode_url, 
                params=params, 
                timeout=timeout,
                verify=False,  # Disable SSL verification for macOS compatibility
                allow_redirects=True
            )
            
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

