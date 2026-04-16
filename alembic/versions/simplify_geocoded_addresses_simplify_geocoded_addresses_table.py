"""Simplify geocoded_addresses table - remove geocoded_address_id

Revision ID: simplify_geocoded_addresses
Revises: 
Create Date: 2026-03-12 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = 'simplify_geocoded_addresses'
down_revision = 'schema_update_2026'
branch_labels = None
depends_on = None

def upgrade():
    """Remove geocoded_address_id and use address_id as primary key"""
    
    # Create new table with simplified schema
    op.create_table('geocoded_addresses_new',
        sa.Column('address_id', sa.BigInteger(), nullable=False),
        sa.Column('latitude', sa.Float(), nullable=True),
        sa.Column('longitude', sa.Float(), nullable=True),
        sa.Column('geocoded_at', sa.DateTime(), nullable=True),
        sa.Column('geocode_status', sa.String(), nullable=True),
        sa.Column('geocode_service', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['address_id'], ['addresses.address_id'], ),
        sa.PrimaryKeyConstraint('address_id')
    )
    
    # Copy data from old table to new table (if old table exists)
    connection = op.get_bind()
    
    # Check if old table exists
    try:
        result = connection.execute(sa.text("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = 'geocoded_addresses' AND table_schema = 'public'
        """))
        
        if result.scalar() > 0:
            # Copy existing data, keeping only one record per address_id (the most recent)
            connection.execute(sa.text("""
                INSERT INTO geocoded_addresses_new (
                    address_id, latitude, longitude, geocoded_at, geocode_status,
                    geocode_service, created_at, updated_at
                )
                SELECT DISTINCT ON (address_id)
                    address_id, 
                    latitude, longitude, geocoded_at, geocode_status,
                    geocode_service, created_at, updated_at
                FROM geocoded_addresses
                ORDER BY address_id, created_at DESC
            """))
            
            # Drop old table
            op.drop_table('geocoded_addresses')
    
    except Exception:
        # Old table doesn't exist, continue
        pass
    
    # Rename new table to correct name
    op.rename_table('geocoded_addresses_new', 'geocoded_addresses')

def downgrade():
    """Restore geocoded_address_id as primary key"""
    
    # Create table with old schema  
    op.create_table('geocoded_addresses_old',
        sa.Column('geocoded_address_id', sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column('address_id', sa.BigInteger(), nullable=False),
        sa.Column('entity_type', sa.String(), nullable=False),
        sa.Column('entity_id', sa.BigInteger(), nullable=False),
        sa.Column('case_id', sa.BigInteger(), nullable=False),
        sa.Column('latitude', sa.Float(), nullable=True),
        sa.Column('longitude', sa.Float(), nullable=True),
        sa.Column('geocoded_at', sa.DateTime(), nullable=True),
        sa.Column('geocode_status', sa.String(), nullable=True),
        sa.Column('geocode_service', sa.String(), nullable=True),
        sa.Column('geocode_accuracy', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['address_id'], ['addresses.address_id'], ),
        sa.ForeignKeyConstraint(['case_id'], ['cases.case_id'], ),
        sa.PrimaryKeyConstraint('geocoded_address_id')
    )
    
    # Copy data back
    connection = op.get_bind()
    connection.execute(sa.text("""
        INSERT INTO geocoded_addresses_old (
            address_id, entity_type, entity_id, case_id,
            latitude, longitude, geocoded_at, geocode_status,
            geocode_service, geocode_accuracy, created_at, updated_at
        )
        SELECT 
            address_id, 'party' as entity_type, 0 as entity_id, 0 as case_id,
            latitude, longitude, geocoded_at, geocode_status,
            geocode_service, null as geocode_accuracy, created_at, updated_at
        FROM geocoded_addresses
    """))
    
    # Drop current table and rename old one
    op.drop_table('geocoded_addresses')
    op.rename_table('geocoded_addresses_old', 'geocoded_addresses')