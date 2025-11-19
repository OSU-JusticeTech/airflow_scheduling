from sqlalchemy import Column, BigInteger, String
from .base import Base

class Address(Base):
    __tablename__ = "address"

    address_id = Column(BigInteger, primary_key=True)
    address_type = Column(String)
    address_line1 = Column(String)
    address_line2 = Column(String)
    city = Column(String)
    state = Column(String)
    country = Column(String)
    postal_code = Column(String)
