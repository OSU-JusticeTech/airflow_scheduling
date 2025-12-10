from sqlalchemy import Column, BigInteger, String, Date, TIMESTAMP, ForeignKey, Identity
from sqlalchemy.orm import relationship
from .base import Base
from sqlalchemy import func


class Disposition(Base):
    __tablename__ = "disposition"

    disposition_id = Column(BigInteger, Identity(always=True), primary_key=True)
    case_id = Column(BigInteger, ForeignKey("cases.case_id"))

    disposition_status = Column(String)
    disposition_status_date = Column(Date)
    disposition_code = Column(String)
    disposition_date = Column(Date)
    judge = Column(String)

    created = Column(TIMESTAMP(timezone=True), server_default=func.now())
    created_dag_run_id = Column(String)
    
    updated = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    updated_task_id = Column(String)

    case = relationship("Case", back_populates="dispositions")
