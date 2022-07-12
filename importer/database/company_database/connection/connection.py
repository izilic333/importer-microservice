from common.urls.urls import importer_database_connection
from sqlalchemy import create_engine

from database.company_database.models.models import metadata

engine = create_engine(importer_database_connection, convert_unicode=True, echo=False,
                       pool_size=20, max_overflow=100)


metadata.create_all(engine)