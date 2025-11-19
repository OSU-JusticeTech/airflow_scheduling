from sqlalchemy import Column, BigInteger, String, Date, TIMESTAMP, ForeignKey, Identity
from sqlalchemy.orm import relationship
from .base import Base

class Event(Base):
    __tablename__ = "events"

    event_id = Column(BigInteger, Identity(always=True), primary_key=True)
    case_id = Column(BigInteger, ForeignKey("cases.case_id"))

    event_date = Column(Date)
    event_start_time = Column(TIMESTAMP)
    event_end_time = Column(TIMESTAMP)
    event_judge = Column(String)
    event_courtroom = Column(String)
    event_result = Column(String)

    created = Column(TIMESTAMP)
    created_by = Column(String)
    updated = Column(TIMESTAMP)
    updated_by = Column(String)

    case = relationship("Case", back_populates="events")
