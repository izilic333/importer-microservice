"""added statistics

Revision ID: 65491adc97ee
Revises: 9a953e6ff8dd
Create Date: 2017-09-08 10:36:22.540830

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '65491adc97ee'
down_revision = '9a953e6ff8dd'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('cloud_company_history', sa.Column('statistics', sa.JSON(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('cloud_company_history', 'statistics')
    # ### end Alembic commands ###
