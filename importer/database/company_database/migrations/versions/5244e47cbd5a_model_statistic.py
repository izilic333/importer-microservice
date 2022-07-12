"""model_statistic

Revision ID: 5244e47cbd5a
Revises: a192a933f28e
Create Date: 2017-12-20 12:51:00.501565

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '5244e47cbd5a'
down_revision = 'a192a933f28e'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('company_statistic',
    sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('uuid_generate_v4()'), nullable=False),
    sa.Column('company_id', sa.Integer(), nullable=False),
    sa.Column('api_type', sa.String(length=25), nullable=True),
    sa.Column('api_name', sa.String(length=25), nullable=True),
    sa.Column('api_count', sa.BigInteger(), nullable=True),
    sa.Column('api_elastic_hash', sa.String(length=255), nullable=True),
    sa.Column('elastic_hash_deleted', sa.Boolean(), nullable=True),
    sa.Column('date_statistic', sa.Date(), nullable=True),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
    sa.ForeignKeyConstraint(['company_id'], ['cloud_company.company_id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_company_statistic_api_elastic_hash'), 'company_statistic', ['api_elastic_hash'], unique=False)
    op.create_index(op.f('ix_company_statistic_api_name'), 'company_statistic', ['api_name'], unique=False)
    op.create_index(op.f('ix_company_statistic_api_type'), 'company_statistic', ['api_type'], unique=False)
    op.create_index(op.f('ix_company_statistic_company_id'), 'company_statistic', ['company_id'], unique=False)
    op.create_index(op.f('ix_company_statistic_created_at'), 'company_statistic', ['created_at'], unique=False)
    op.create_index(op.f('ix_company_statistic_date_statistic'), 'company_statistic', ['date_statistic'], unique=False)
    op.create_index(op.f('ix_company_statistic_id'), 'company_statistic', ['id'], unique=True)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_company_statistic_id'), table_name='company_statistic')
    op.drop_index(op.f('ix_company_statistic_date_statistic'), table_name='company_statistic')
    op.drop_index(op.f('ix_company_statistic_created_at'), table_name='company_statistic')
    op.drop_index(op.f('ix_company_statistic_company_id'), table_name='company_statistic')
    op.drop_index(op.f('ix_company_statistic_api_type'), table_name='company_statistic')
    op.drop_index(op.f('ix_company_statistic_api_name'), table_name='company_statistic')
    op.drop_index(op.f('ix_company_statistic_api_elastic_hash'), table_name='company_statistic')
    op.drop_table('company_statistic')
    # ### end Alembic commands ###
