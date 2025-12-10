from sqlalchemy import Column, BigInteger, String, Text, Date, TIMESTAMP, Identity
from sqlalchemy.orm import relationship
from .base import Base
from .association_tables import case_party
from sqlalchemy import func

class Case(Base):
    __tablename__ = "cases"

    case_id = Column(BigInteger, Identity(always=True), primary_key=True)
    case_number = Column(String)
    case_title = Column(String)
    case_description = Column(Text)
    case_status = Column(String)
    case_filed_date = Column(Date)
    # set default to now()
    created = Column(TIMESTAMP(timezone=True), server_default=func.now())
    created_dag_run_id = Column(String)
    # okay to be null upon first creation, now()
    updated = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())
    updated_task_id = Column(String)
    
    # in progrss -- wip,holds values "new", "valid", "error", "closed"
    pipeline_status = Column(String, default='New', nullable=False) 

    parties = relationship("Party", secondary=case_party, back_populates="cases")
    dispositions = relationship("Disposition", back_populates="case")
    events = relationship("Event", back_populates="case")
    dockets = relationship("Docket", back_populates="case")
