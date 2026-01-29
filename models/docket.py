from sqlalchemy import Column, BigInteger, String, Date, Numeric, Text, TIMESTAMP, ForeignKey, Identity
from sqlalchemy.orm import relationship
from .base import Base
from sqlalchemy import func

class Docket(Base):
    __tablename__ = "docket"

    docket_id = Column(BigInteger, Identity(always=True), primary_key=True)
    case_id = Column(BigInteger, ForeignKey("cases.case_id"))

    docket_date = Column(Date)
    docket_text = Column(Text)
    docket_currency = Column(String)
    docket_amount = Column(Numeric)
    docket_balance = Column(Numeric)

    created = Column(TIMESTAMP(timezone=True), server_default=func.now())
    created_dag_run_id = Column(String)
    
    updated = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    updated_task_id = Column(String)

    case = relationship("Case", back_populates="dockets")
