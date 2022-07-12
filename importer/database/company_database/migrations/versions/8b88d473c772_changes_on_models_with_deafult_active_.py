"""changes on models with deafult active field

Revision ID: 8b88d473c772
Revises: 65491adc97ee
Create Date: 2017-09-15 11:12:50.651895

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8b88d473c772'
down_revision = '65491adc97ee'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('cloud_company_history', sa.Column('active_history', sa.Boolean(), nullable=True))
    op.add_column('cloud_company_process_fail_history', sa.Column('active_history', sa.Boolean(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('cloud_company_process_fail_history', 'active_history')
    op.drop_column('cloud_company_history', 'active_history')
    # ### end Alembic commands ###
