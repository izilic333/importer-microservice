"""Initial database

Revision ID: 928eb06f6e8f
Revises: 
Create Date: 2017-09-04 10:07:11.745361

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '928eb06f6e8f'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('cloud_company',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('company_id', sa.Integer(), nullable=True),
    sa.Column('company_name', sa.String(length=75), nullable=True),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_cloud_company_company_id'), 'cloud_company', ['company_id'], unique=True)
    op.create_index(op.f('ix_cloud_company_id'), 'cloud_company', ['id'], unique=False)
    op.create_table('cloud_company_history',
    sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('uuid_generate_v4()'), nullable=False),
    sa.Column('company_id', sa.Integer(), nullable=False),
    sa.Column('import_type', sa.Integer(), nullable=False),
    sa.Column('elastic_hash', sa.String(length=255), nullable=False),
    sa.Column('data_hash', sa.String(length=255), nullable=False),
    sa.Column('import_data', sa.JSON(), nullable=False),
    sa.Column('file_path', sa.String(length=255), nullable=False),
    sa.Column('cloud_inserted', sa.Boolean(), nullable=True),
    sa.Column('full_name', sa.String(length=255), nullable=True),
    sa.Column('user_id', sa.Integer(), nullable=True),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.ForeignKeyConstraint(['company_id'], ['cloud_company.company_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_cloud_company_history_company_id'), 'cloud_company_history', ['company_id'], unique=False)
    op.create_index(op.f('ix_cloud_company_history_created_at'), 'cloud_company_history', ['created_at'], unique=False)
    op.create_index(op.f('ix_cloud_company_history_data_hash'), 'cloud_company_history', ['data_hash'], unique=False)
    op.create_index(op.f('ix_cloud_company_history_elastic_hash'), 'cloud_company_history', ['elastic_hash'], unique=False)
    op.create_index(op.f('ix_cloud_company_history_file_path'), 'cloud_company_history', ['file_path'], unique=False)
    op.create_index(op.f('ix_cloud_company_history_full_name'), 'cloud_company_history', ['full_name'], unique=False)
    op.create_index(op.f('ix_cloud_company_history_id'), 'cloud_company_history', ['id'], unique=True)
    op.create_index(op.f('ix_cloud_company_history_import_type'), 'cloud_company_history', ['import_type'], unique=False)
    op.create_index(op.f('ix_cloud_company_history_user_id'), 'cloud_company_history', ['user_id'], unique=False)
    op.create_table('cloud_company_process_fail_history',
    sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('uuid_generate_v4()'), nullable=False),
    sa.Column('company_id', sa.Integer(), nullable=False),
    sa.Column('import_type', sa.Integer(), nullable=False),
    sa.Column('import_error_type', sa.Integer(), nullable=False),
    sa.Column('elastic_hash', sa.String(length=255), nullable=False),
    sa.Column('data_hash', sa.String(length=255), nullable=False),
    sa.Column('file_path', sa.String(length=255), nullable=False),
    sa.Column('full_name', sa.String(length=255), nullable=True),
    sa.Column('user_id', sa.Integer(), nullable=True),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.ForeignKeyConstraint(['company_id'], ['cloud_company.company_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_cloud_company_process_fail_history_company_id'), 'cloud_company_process_fail_history', ['company_id'], unique=False)
    op.create_index(op.f('ix_cloud_company_process_fail_history_created_at'), 'cloud_company_process_fail_history', ['created_at'], unique=False)
    op.create_index(op.f('ix_cloud_company_process_fail_history_data_hash'), 'cloud_company_process_fail_history', ['data_hash'], unique=False)
    op.create_index(op.f('ix_cloud_company_process_fail_history_elastic_hash'), 'cloud_company_process_fail_history', ['elastic_hash'], unique=True)
    op.create_index(op.f('ix_cloud_company_process_fail_history_file_path'), 'cloud_company_process_fail_history', ['file_path'], unique=False)
    op.create_index(op.f('ix_cloud_company_process_fail_history_full_name'), 'cloud_company_process_fail_history', ['full_name'], unique=False)
    op.create_index(op.f('ix_cloud_company_process_fail_history_id'), 'cloud_company_process_fail_history', ['id'], unique=True)
    op.create_index(op.f('ix_cloud_company_process_fail_history_import_error_type'), 'cloud_company_process_fail_history', ['import_error_type'], unique=False)
    op.create_index(op.f('ix_cloud_company_process_fail_history_import_type'), 'cloud_company_process_fail_history', ['import_type'], unique=False)
    op.create_index(op.f('ix_cloud_company_process_fail_history_user_id'), 'cloud_company_process_fail_history', ['user_id'], unique=False)
    op.create_table('company_export_history',
    sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('uuid_generate_v4()'), nullable=False),
    sa.Column('company_id', sa.Integer(), nullable=False),
    sa.Column('query_hash', sa.String(length=255), nullable=False),
    sa.Column('file_path', sa.String(length=255), nullable=False),
    sa.Column('full_name', sa.String(length=255), nullable=True),
    sa.Column('user_id', sa.Integer(), nullable=True),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.ForeignKeyConstraint(['company_id'], ['cloud_company.company_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_company_export_history_company_id'), 'company_export_history', ['company_id'], unique=False)
    op.create_index(op.f('ix_company_export_history_created_at'), 'company_export_history', ['created_at'], unique=False)
    op.create_index(op.f('ix_company_export_history_file_path'), 'company_export_history', ['file_path'], unique=False)
    op.create_index(op.f('ix_company_export_history_full_name'), 'company_export_history', ['full_name'], unique=False)
    op.create_index(op.f('ix_company_export_history_id'), 'company_export_history', ['id'], unique=True)
    op.create_index(op.f('ix_company_export_history_query_hash'), 'company_export_history', ['query_hash'], unique=False)
    op.create_index(op.f('ix_company_export_history_user_id'), 'company_export_history', ['user_id'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_company_export_history_user_id'), table_name='company_export_history')
    op.drop_index(op.f('ix_company_export_history_query_hash'), table_name='company_export_history')
    op.drop_index(op.f('ix_company_export_history_id'), table_name='company_export_history')
    op.drop_index(op.f('ix_company_export_history_full_name'), table_name='company_export_history')
    op.drop_index(op.f('ix_company_export_history_file_path'), table_name='company_export_history')
    op.drop_index(op.f('ix_company_export_history_created_at'), table_name='company_export_history')
    op.drop_index(op.f('ix_company_export_history_company_id'), table_name='company_export_history')
    op.drop_table('company_export_history')
    op.drop_index(op.f('ix_cloud_company_process_fail_history_user_id'), table_name='cloud_company_process_fail_history')
    op.drop_index(op.f('ix_cloud_company_process_fail_history_import_type'), table_name='cloud_company_process_fail_history')
    op.drop_index(op.f('ix_cloud_company_process_fail_history_import_error_type'), table_name='cloud_company_process_fail_history')
    op.drop_index(op.f('ix_cloud_company_process_fail_history_id'), table_name='cloud_company_process_fail_history')
    op.drop_index(op.f('ix_cloud_company_process_fail_history_full_name'), table_name='cloud_company_process_fail_history')
    op.drop_index(op.f('ix_cloud_company_process_fail_history_file_path'), table_name='cloud_company_process_fail_history')
    op.drop_index(op.f('ix_cloud_company_process_fail_history_elastic_hash'), table_name='cloud_company_process_fail_history')
    op.drop_index(op.f('ix_cloud_company_process_fail_history_data_hash'), table_name='cloud_company_process_fail_history')
    op.drop_index(op.f('ix_cloud_company_process_fail_history_created_at'), table_name='cloud_company_process_fail_history')
    op.drop_index(op.f('ix_cloud_company_process_fail_history_company_id'), table_name='cloud_company_process_fail_history')
    op.drop_table('cloud_company_process_fail_history')
    op.drop_index(op.f('ix_cloud_company_history_user_id'), table_name='cloud_company_history')
    op.drop_index(op.f('ix_cloud_company_history_import_type'), table_name='cloud_company_history')
    op.drop_index(op.f('ix_cloud_company_history_id'), table_name='cloud_company_history')
    op.drop_index(op.f('ix_cloud_company_history_full_name'), table_name='cloud_company_history')
    op.drop_index(op.f('ix_cloud_company_history_file_path'), table_name='cloud_company_history')
    op.drop_index(op.f('ix_cloud_company_history_elastic_hash'), table_name='cloud_company_history')
    op.drop_index(op.f('ix_cloud_company_history_data_hash'), table_name='cloud_company_history')
    op.drop_index(op.f('ix_cloud_company_history_created_at'), table_name='cloud_company_history')
    op.drop_index(op.f('ix_cloud_company_history_company_id'), table_name='cloud_company_history')
    op.drop_table('cloud_company_history')
    op.drop_index(op.f('ix_cloud_company_id'), table_name='cloud_company')
    op.drop_index(op.f('ix_cloud_company_company_id'), table_name='cloud_company')
    op.drop_table('cloud_company')
    # ### end Alembic commands ###