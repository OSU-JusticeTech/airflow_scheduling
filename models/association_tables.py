from sqlalchemy import Table, Column, BigInteger, ForeignKey
from .base import Base

case_party = Table(
    "case_party",
    Base.metadata,
    Column("case_id", BigInteger, ForeignKey("cases.case_id"), primary_key=True),
    Column("party_id", BigInteger, ForeignKey("party.party_id"), primary_key=True)
)
