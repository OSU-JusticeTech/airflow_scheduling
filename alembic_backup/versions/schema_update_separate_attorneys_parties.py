"""Separate attorneys and parties tables with proper case relationships and geocoded addresses

Revision ID: schema_update_2026
Revises: 14d0653c6a30
Create Date: 2026-02-18 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'schema_update_2026'
down_revision: Union[str, None] = '14d0653c6a30'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create new addresses table without geocoding fields
    op.create_table('addresses',
        sa.Column('address_id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column('address_line1', sa.String(), nullable=True),
        sa.Column('address_line2', sa.String(), nullable=True),
        sa.Column('city', sa.String(), nullable=True),
        sa.Column('state', sa.String(), nullable=True),
        sa.Column('country', sa.String(), nullable=True),
        sa.Column('postal_code', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('address_id')
    )

    # Create new geocoded_addresses table for location data
    op.create_table('geocoded_addresses',
        sa.Column('geocoded_address_id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column('address_id', sa.BigInteger(), nullable=False),
        sa.Column('entity_type', sa.String(), nullable=False),  # 'party' or 'attorney'
        sa.Column('entity_id', sa.BigInteger(), nullable=False),  # party_id or attorney_id
        sa.Column('case_id', sa.BigInteger(), nullable=False),
        sa.Column('latitude', sa.Float(), nullable=True),
        sa.Column('longitude', sa.Float(), nullable=True),
        sa.Column('geocoded_at', sa.DateTime(), nullable=True),
        sa.Column('geocode_status', sa.String(), nullable=True),
        sa.Column('geocode_service', sa.String(), nullable=True),  # 'cura', 'google', etc.
        sa.Column('geocode_accuracy', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['address_id'], ['addresses.address_id'], ),
        sa.ForeignKeyConstraint(['case_id'], ['cases.case_id'], ),
        sa.PrimaryKeyConstraint('geocoded_address_id')
    )

    # Update parties table to link directly to cases
    op.add_column('party', sa.Column('case_id', sa.BigInteger(), nullable=True))
    op.create_foreign_key('fk_party_case', 'party', 'cases', ['case_id'], ['case_id'])
    
    # Update party table address relationship to use new addresses table
    op.drop_constraint('party_address_id_fkey', 'party', type_='foreignkey')
    op.create_foreign_key('fk_party_address', 'party', 'addresses', ['address_id'], ['address_id'])

    # Update attorneys table to link directly to cases and use new addresses table
    op.add_column('attorney', sa.Column('case_id', sa.BigInteger(), nullable=True))
    op.create_foreign_key('fk_attorney_case', 'attorney', 'cases', ['case_id'], ['case_id'])
    op.drop_constraint('attorney_address_id_fkey', 'attorney', type_='foreignkey')
    op.create_foreign_key('fk_attorney_address', 'attorney', 'addresses', ['address_id'], ['address_id'])

    # Create indexes for better performance
    op.create_index('idx_geocoded_addresses_entity', 'geocoded_addresses', ['entity_type', 'entity_id'])
    op.create_index('idx_geocoded_addresses_case', 'geocoded_addresses', ['case_id'])
    op.create_index('idx_party_case', 'party', ['case_id'])
    op.create_index('idx_attorney_case', 'attorney', ['case_id'])


def downgrade() -> None:
    # Remove indexes
    op.drop_index('idx_attorney_case', table_name='attorney')
    op.drop_index('idx_party_case', table_name='party')
    op.drop_index('idx_geocoded_addresses_case', table_name='geocoded_addresses')
    op.drop_index('idx_geocoded_addresses_entity', table_name='geocoded_addresses')
    
    # Remove foreign key constraints
    op.drop_constraint('fk_attorney_address', 'attorney', type_='foreignkey')
    op.drop_constraint('fk_attorney_case', 'attorney', type_='foreignkey')
    op.drop_constraint('fk_party_address', 'party', type_='foreignkey')
    op.drop_constraint('fk_party_case', 'party', type_='foreignkey')
    
    # Remove new columns
    op.drop_column('attorney', 'case_id')
    op.drop_column('party', 'case_id')
    
    # Recreate old foreign key constraints
    op.create_foreign_key('party_address_id_fkey', 'party', 'address', ['address_id'], ['address_id'])
    op.create_foreign_key('attorney_address_id_fkey', 'attorney', 'address', ['address_id'], ['address_id'])
    
    # Drop new tables
    op.drop_table('geocoded_addresses')
    op.drop_table('addresses')
