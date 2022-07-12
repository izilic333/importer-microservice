"""Vend import tables

Revision ID: 9507d0a75360
Revises: 60b284f65f89
Create Date: 2018-08-28 14:16:11.527143

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '9507d0a75360'
down_revision = '60b284f65f89'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('vend_device_history',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('device_pid', sa.String(length=255), nullable=False),
    sa.Column('machine_id', sa.String(length=255), nullable=False),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.Column('file_timestamp', sa.DateTime(), nullable=True),
    sa.Column('company_id', sa.Integer(), nullable=False),
    sa.Column('import_filename', sa.String(length=255), nullable=False),
    sa.Column('zip_filename', sa.String(length=255), nullable=False),
    sa.Column('import_type', sa.String(length=255), nullable=False),
    sa.Column('actual_machine', sa.Boolean(), nullable=True),
    sa.Column('data', sa.JSON(), nullable=False),
    sa.ForeignKeyConstraint(['company_id'], ['cloud_company.company_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_vend_device_history_company_id'), 'vend_device_history', ['company_id'], unique=False)
    op.create_index(op.f('ix_vend_device_history_created_at'), 'vend_device_history', ['created_at'], unique=False)
    op.create_index(op.f('ix_vend_device_history_device_pid'), 'vend_device_history', ['device_pid'], unique=False)
    op.create_index(op.f('ix_vend_device_history_file_timestamp'), 'vend_device_history', ['file_timestamp'], unique=False)
    op.create_index(op.f('ix_vend_device_history_id'), 'vend_device_history', ['id'], unique=False)
    op.create_index(op.f('ix_vend_device_history_import_filename'), 'vend_device_history', ['import_filename'], unique=False)
    op.create_index(op.f('ix_vend_device_history_import_type'), 'vend_device_history', ['import_type'], unique=False)
    op.create_index(op.f('ix_vend_device_history_machine_id'), 'vend_device_history', ['machine_id'], unique=False)
    op.create_index(op.f('ix_vend_device_history_zip_filename'), 'vend_device_history', ['zip_filename'], unique=False)
    op.create_table('vend_fail_history',
    sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('uuid_generate_v4()'), nullable=False),
    sa.Column('company_id', sa.Integer(), nullable=False),
    sa.Column('import_type', sa.Integer(), nullable=False),
    sa.Column('import_error_type', sa.Integer(), nullable=False),
    sa.Column('elastic_hash', sa.String(length=255), nullable=False),
    sa.Column('main_elastic_hash', sa.String(length=255), nullable=True),
    sa.Column('data_hash', sa.String(length=255), nullable=False),
    sa.Column('file_path', sa.String(length=255), nullable=False),
    sa.Column('full_name', sa.String(length=255), nullable=True),
    sa.Column('user_id', sa.Integer(), nullable=True),
    sa.Column('active_history', sa.Boolean(), nullable=True),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.ForeignKeyConstraint(['company_id'], ['cloud_company.company_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_vend_fail_history_company_id'), 'vend_fail_history', ['company_id'], unique=False)
    op.create_index(op.f('ix_vend_fail_history_created_at'), 'vend_fail_history', ['created_at'], unique=False)
    op.create_index(op.f('ix_vend_fail_history_data_hash'), 'vend_fail_history', ['data_hash'], unique=False)
    op.create_index(op.f('ix_vend_fail_history_elastic_hash'), 'vend_fail_history', ['elastic_hash'], unique=True)
    op.create_index(op.f('ix_vend_fail_history_full_name'), 'vend_fail_history', ['full_name'], unique=False)
    op.create_index(op.f('ix_vend_fail_history_id'), 'vend_fail_history', ['id'], unique=True)
    op.create_index(op.f('ix_vend_fail_history_import_error_type'), 'vend_fail_history', ['import_error_type'], unique=False)
    op.create_index(op.f('ix_vend_fail_history_import_type'), 'vend_fail_history', ['import_type'], unique=False)
    op.create_index(op.f('ix_vend_fail_history_main_elastic_hash'), 'vend_fail_history', ['main_elastic_hash'], unique=False)
    op.create_index(op.f('ix_vend_fail_history_user_id'), 'vend_fail_history', ['user_id'], unique=False)
    op.create_table('vend_success_history',
    sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('uuid_generate_v4()'), nullable=False),
    sa.Column('company_id', sa.Integer(), nullable=False),
    sa.Column('import_type', sa.Integer(), nullable=False),
    sa.Column('elastic_hash', sa.String(length=255), nullable=False),
    sa.Column('data_hash', sa.String(length=255), nullable=False),
    sa.Column('import_data', sa.JSON(), nullable=False),
    sa.Column('file_path', sa.String(length=255), nullable=False),
    sa.Column('cloud_inserted', sa.Boolean(), nullable=True),
    sa.Column('partial', sa.Boolean(), nullable=True),
    sa.Column('full_name', sa.String(length=255), nullable=True),
    sa.Column('user_id', sa.Integer(), nullable=True),
    sa.Column('statistics', sa.JSON(), nullable=True),
    sa.Column('cloud_results', sa.JSON(), nullable=True),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.ForeignKeyConstraint(['company_id'], ['cloud_company.company_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_vend_success_history_company_id'), 'vend_success_history', ['company_id'], unique=False)
    op.create_index(op.f('ix_vend_success_history_created_at'), 'vend_success_history', ['created_at'], unique=False)
    op.create_index(op.f('ix_vend_success_history_data_hash'), 'vend_success_history', ['data_hash'], unique=False)
    op.create_index(op.f('ix_vend_success_history_elastic_hash'), 'vend_success_history', ['elastic_hash'], unique=False)
    op.create_index(op.f('ix_vend_success_history_id'), 'vend_success_history', ['id'], unique=True)
    op.create_index(op.f('ix_vend_success_history_import_type'), 'vend_success_history', ['import_type'], unique=False)
    op.create_index(op.f('ix_vend_success_history_user_id'), 'vend_success_history', ['user_id'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_vend_success_history_user_id'), table_name='vend_success_history')
    op.drop_index(op.f('ix_vend_success_history_import_type'), table_name='vend_success_history')
    op.drop_index(op.f('ix_vend_success_history_id'), table_name='vend_success_history')
    op.drop_index(op.f('ix_vend_success_history_elastic_hash'), table_name='vend_success_history')
    op.drop_index(op.f('ix_vend_success_history_data_hash'), table_name='vend_success_history')
    op.drop_index(op.f('ix_vend_success_history_created_at'), table_name='vend_success_history')
    op.drop_index(op.f('ix_vend_success_history_company_id'), table_name='vend_success_history')
    op.drop_table('vend_success_history')
    op.drop_index(op.f('ix_vend_fail_history_user_id'), table_name='vend_fail_history')
    op.drop_index(op.f('ix_vend_fail_history_main_elastic_hash'), table_name='vend_fail_history')
    op.drop_index(op.f('ix_vend_fail_history_import_type'), table_name='vend_fail_history')
    op.drop_index(op.f('ix_vend_fail_history_import_error_type'), table_name='vend_fail_history')
    op.drop_index(op.f('ix_vend_fail_history_id'), table_name='vend_fail_history')
    op.drop_index(op.f('ix_vend_fail_history_full_name'), table_name='vend_fail_history')
    op.drop_index(op.f('ix_vend_fail_history_elastic_hash'), table_name='vend_fail_history')
    op.drop_index(op.f('ix_vend_fail_history_data_hash'), table_name='vend_fail_history')
    op.drop_index(op.f('ix_vend_fail_history_created_at'), table_name='vend_fail_history')
    op.drop_index(op.f('ix_vend_fail_history_company_id'), table_name='vend_fail_history')
    op.drop_table('vend_fail_history')
    op.drop_index(op.f('ix_vend_device_history_zip_filename'), table_name='vend_device_history')
    op.drop_index(op.f('ix_vend_device_history_machine_id'), table_name='vend_device_history')
    op.drop_index(op.f('ix_vend_device_history_import_type'), table_name='vend_device_history')
    op.drop_index(op.f('ix_vend_device_history_import_filename'), table_name='vend_device_history')
    op.drop_index(op.f('ix_vend_device_history_id'), table_name='vend_device_history')
    op.drop_index(op.f('ix_vend_device_history_file_timestamp'), table_name='vend_device_history')
    op.drop_index(op.f('ix_vend_device_history_device_pid'), table_name='vend_device_history')
    op.drop_index(op.f('ix_vend_device_history_created_at'), table_name='vend_device_history')
    op.drop_index(op.f('ix_vend_device_history_company_id'), table_name='vend_device_history')
    op.drop_table('vend_device_history')
    # ### end Alembic commands ###