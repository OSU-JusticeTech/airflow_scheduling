from sqlalchemy import Column, BigInteger, String, Date, TIMESTAMP, ForeignKey, Identity
from sqlalchemy.orm import relationship
from .base import Base

class Disposition(Base):
    __tablename__ = "disposition"

    disposition_id = Column(BigInteger, Identity(always=True), primary_key=True)
    case_id = Column(BigInteger, ForeignKey("cases.case_id"))

    disposition_status = Column(String)
    disposition_status_date = Column(Date)
    disposition_code = Column(String)
    disposition_date = Column(Date)
    judge = Column(String)

    created = Column(TIMESTAMP)
    created_by = Column(String)
    updated = Column(TIMESTAMP)
    updated_by = Column(String)

    case = relationship("Case", back_populates="dispositions")
