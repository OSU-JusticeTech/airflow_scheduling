from sqlalchemy import Column, BigInteger, String, Date, TIMESTAMP, ForeignKey, Identity
from sqlalchemy.orm import relationship
from .base import Base
from sqlalchemy import func

class Event(Base):
    __tablename__ = "events"

    event_id = Column(BigInteger, Identity(always=True), primary_key=True)
    case_id = Column(BigInteger, ForeignKey("cases.case_id"))

    event_name = Column(String)
    event_date = Column(Date)
    event_start_time = Column(String)
    event_end_time = Column(String)
    event_judge = Column(String)
    event_courtroom = Column(String)
    event_result = Column(String)

    created = Column(TIMESTAMP(timezone=True), server_default=func.now())
    created_dag_run_id = Column(String)
    
    updated = Column(TIMESTAMP(timezone=True), onupdate=func.now())
    updated_task_id = Column(String)

    case = relationship("Case", back_populates="events")
