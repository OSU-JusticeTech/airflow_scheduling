from sqlalchemy import Column, BigInteger, String, TIMESTAMP, ForeignKey, Identity
from sqlalchemy.orm import relationship
from .base import Base
from sqlalchemy import func

class Attorney(Base):
    __tablename__ = "attorney"

    attorney_id = Column(BigInteger, Identity(always=True), primary_key=True)
    attorney_name = Column(String)
    party_id = Column(BigInteger, ForeignKey("party.party_id"))
    attorney_type = Column(String)
    address_id = Column(BigInteger, ForeignKey("address.address_id"))

    created = Column(TIMESTAMP(timezone=True), server_default=func.now())
    created_dag_run_id = Column(String)
    
    updated = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    updated_task_id = Column(String)

    party = relationship("Party", back_populates="attorneys")
    address = relationship("Address")
