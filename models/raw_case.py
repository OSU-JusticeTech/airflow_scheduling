from sqlalchemy import Column, BigInteger, String, Text, Date, TIMESTAMP, JSON
from .base import Base

class RawCase(Base):
    __tablename__ = "raw_cases"

    case_id = Column(BigInteger, primary_key=True)
    case_number = Column(String)
    case_title = Column(String)
    case_description = Column(Text)
    case_status = Column(String)
    case_filed_date = Column(Date)
    created = Column(TIMESTAMP)
    created_by = Column(String)
    updated = Column(TIMESTAMP)
    updated_by = Column(String)

    first_party_name = Column(String)
    first_party_type = Column(String)

    first_attorney_name = Column(String)
    first_attorney_type = Column(String)

    first_event_start = Column(TIMESTAMP)
    first_event_end = Column(TIMESTAMP)
    first_event_judge = Column(String)

    parties = Column(JSON)
    attorneys = Column(JSON)
    events = Column(JSON)
    docket = Column(JSON)
    dispositions = Column(JSON)

    raw_html = Column(Text)
    ingested_at = Column(TIMESTAMP)
