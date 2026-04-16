from sqlalchemy import Column, BigInteger, String, DateTime, Identity
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from .base import Base

class Address(Base):
    __tablename__ = "addresses"

    address_id = Column(BigInteger, Identity(always=True), primary_key=True)
    address_line1 = Column(String)
    address_line2 = Column(String)
    city = Column(String)
    state = Column(String)
    country = Column(String)
    postal_code = Column(String)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    # Relationships
    parties = relationship("Party", back_populates="address")
    attorneys = relationship("Attorney", back_populates="address")
    geocoded_addresses = relationship("GeocodedAddress", back_populates="address")