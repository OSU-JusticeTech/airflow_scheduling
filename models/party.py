from sqlalchemy import Column, BigInteger, String, TIMESTAMP, ForeignKey, Identity
from sqlalchemy.orm import relationship
from .base import Base
from .association_tables import case_party

class Party(Base):
    __tablename__ = "party"

    party_id = Column(BigInteger, Identity(always=True), primary_key=True)
    party_type = Column(String)

    address_id = Column(BigInteger, ForeignKey("address.address_id"))

    created = Column(TIMESTAMP)
    created_by = Column(String)
    updated = Column(TIMESTAMP)
    updated_by = Column(String)

    address = relationship("Address")
    cases = relationship("Case", secondary=case_party, back_populates="parties")
    attorneys = relationship("Attorney", back_populates="party")
