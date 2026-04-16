from sqlalchemy import Column, BigInteger, String, TIMESTAMP, ForeignKey, Identity
from sqlalchemy.orm import relationship
from .base import Base
from .association_tables import case_party
from sqlalchemy import func

class Party(Base):
    __tablename__ = "party"

    party_id = Column(BigInteger, Identity(always=True), primary_key=True)
    case_id = Column(BigInteger, ForeignKey("cases.case_id"), nullable=False)
    party_name = Column(String)
    party_type = Column(String)
    address_id = Column(BigInteger, ForeignKey("addresses.address_id"))

    created = Column(TIMESTAMP(timezone=True), server_default=func.now())
    created_dag_run_id = Column(String)
    
    updated = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    updated_task_id = Column(String)

    # Relationships
    case = relationship("Case", back_populates="parties")
    address = relationship("Address", back_populates="parties")
    attorneys = relationship("Attorney", back_populates="party")
