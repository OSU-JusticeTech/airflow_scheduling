"""
Currently implements a mock service that returns (0,0) coordinates.
Future implementation will integrate with CURA geocoding service.
"""

import time
from typing import Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class GeolocationService:
    
    def __init__(self, use_mock: bool = True):
        """
        Initialize the geolocation service.
        
        Args:
            use_mock: If True, use mock service that returns (0,0). 
                     If False, use actual CURA geocoding service.
        """
        self.use_mock = use_mock
        self.cura_geocode_url = "https://cura-gis-web.asc.ohio-state.edu/arcgis/rest/services/geocoding/USA/GeocodeServer"
        
    def geocode_address(self, address_components: Dict[str, str]) -> Dict[str, any]:
        """
        Geocode an address and return coordinates.
        
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
                - status: 'success', 'failed', 'mock'
                - error_message: String if status is 'failed'
        """
        
        if self.use_mock:
            return self._mock_geocode(address_components)
        else:
            return self._cura_geocode(address_components)

    def _mock_geocode(self, address_components: Dict[str, str]) -> Dict[str, any]:
        """
        Mock geocoding service that returns (0,0) for all addresses.
        """
        # Simulate some processing time
        time.sleep(0.1)
        
        # Log the address being "geocoded"
        full_address = self._format_full_address(address_components)
        logger.info(f"Mock geocoding: {full_address} -> (0.0, 0.0)")
        
        return {
            'latitude': 0.0,
            'longitude': 0.0,
            'status': 'mock',
            'error_message': None
        }
    
    def _cura_geocode(self, address_components: Dict[str, str]) -> Dict[str, any]:
        """
        Future implementation for actual CURA geocoding service.
        Currently returns mock data with a note.
        """
        # TODO: Implement actual CURA geocoding using:
        # curl -k https://cura-gis-web.asc.ohio-state.edu/arcgis/rest/services/geocoding/USA/GeocodeServer
        
        logger.warning("CURA geocoding service not yet implemented. Using mock data.")
        return self._mock_geocode(address_components)
    
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
                    'batch_index': i
                })
        return results

