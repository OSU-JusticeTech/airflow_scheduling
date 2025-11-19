from sqlalchemy import Column, BigInteger, String, TIMESTAMP, ForeignKey, Identity
from sqlalchemy.orm import relationship
from .base import Base

class Attorney(Base):
    __tablename__ = "attorney"

    attorney_id = Column(BigInteger, Identity(always=True), primary_key=True)
    party_id = Column(BigInteger, ForeignKey("party.party_id"))
    attorney_type = Column(String)
    address_id = Column(BigInteger, ForeignKey("address.address_id"))

    created = Column(TIMESTAMP)
    created_by = Column(String)
    updated = Column(TIMESTAMP)
    updated_by = Column(String)

    party = relationship("Party", back_populates="attorneys")
    address = relationship("Address")
