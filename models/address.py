from sqlalchemy import Column, BigInteger, String, Float, DateTime, ForeignKey
from sqlalchemy.sql import func
from .base import Base

class Address(Base):
    __tablename__ = "address"

    address_id = Column(BigInteger, primary_key=True)
    case_number = Column(String)  # Link to raw_cases
    party_name = Column(String)  # Name of the party associated with this address
    address_type = Column(String)
    address_line1 = Column(String)
    address_line2 = Column(String)
    city = Column(String)
    state = Column(String)
    country = Column(String)
    postal_code = Column(String)
    # Geolocation fields
    latitude = Column(Float)
    longitude = Column(Float)
    geocoded_at = Column(DateTime)
    geocode_status = Column(String)  # 'success', 'failed', 'extracted', 'no_results', 'api_error', etc.
    created_at = Column(DateTime, default=func.now())