from sqlalchemy import Column, BigInteger, String, Text, Date, TIMESTAMP, Identity
from sqlalchemy.orm import relationship
from .base import Base
from .association_tables import case_party

class Case(Base):
    __tablename__ = "cases"

    case_id = Column(BigInteger, Identity(always=True), primary_key=True)
    case_number = Column(String)
    case_title = Column(String)
    case_description = Column(Text)
    case_status = Column(String)
    case_filed_date = Column(Date)
    created = Column(TIMESTAMP)
    created_by = Column(String)
    updated = Column(TIMESTAMP)
    updated_by = Column(String)

    parties = relationship("Party", secondary=case_party, back_populates="cases")
    dispositions = relationship("Disposition", back_populates="case")
    events = relationship("Event", back_populates="case")
    dockets = relationship("Docket", back_populates="case")
