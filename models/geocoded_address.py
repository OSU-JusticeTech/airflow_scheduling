from sqlalchemy import Column, BigInteger, String, Float, DateTime, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from .base import Base

class GeocodedAddress(Base):
    __tablename__ = "geocoded_addresses"

    address_id = Column(BigInteger, ForeignKey("addresses.address_id"), primary_key=True)
    
    # Geolocation fields
    latitude = Column(Float)
    longitude = Column(Float)
    geocoded_at = Column(DateTime)
    geocode_status = Column(String)  # 'success', 'failed', 'extracted', 'no_results', 'api_error', etc.
    geocode_service = Column(String)  # 'cura', 'google', 'mapbox', etc.
    
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    # Relationships
    address = relationship("Address", back_populates="geocoded_addresses")
