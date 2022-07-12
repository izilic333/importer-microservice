import uuid

from sqlalchemy import func, text, Date
from sqlalchemy import (Table, Integer, String, MetaData, ForeignKey, JSON, DateTime,
                        BigInteger, Boolean, Column)

from sqlalchemy.dialects.postgresql import UUID

metadata = MetaData()


def uuid_gen():
    return str(uuid.uuid4())


cloud_company = Table(
    'cloud_company', metadata,
    Column('id', BigInteger, primary_key=True, index=True, autoincrement=True),
    # Company external_id - used for matching with external systems
    Column('company_id', Integer, index=True, unique=True),
    Column('company_name', String(length=255), nullable=True),
    Column('created_at', DateTime, server_default=func.now()),
    Column('updated_at', DateTime, server_default=func.now(), onupdate=func.now())
)

cloud_company_history = Table(
    'cloud_company_history', metadata,
    Column('id', UUID(as_uuid=True), primary_key=True, index=True,
           server_default=text("uuid_generate_v4()"), unique=True),
    Column('company_id', Integer, ForeignKey('cloud_company.company_id'),
           nullable=False, index=True),
    Column('import_type', Integer, nullable=False, index=True),
    Column('elastic_hash', String(length=255), nullable=False, index=True),
    Column('data_hash', String(length=255), nullable=False, index=True),
    Column('import_data', JSON, nullable=False),
    Column('file_path', String(length=255), nullable=False, index=True),
    Column('cloud_inserted', Boolean, default=False),
    Column('active_history', Boolean, default=True),
    Column('partial', Boolean, default=False),
    Column('cloud_results', JSON, nullable=True),
    # User profile for every transaction
    Column('full_name', String(length=255), nullable=True, index=True),
    Column('user_id', Integer, nullable=True, index=True),
    Column('statistics', JSON, nullable=True),

    Column('created_at', DateTime, server_default=func.now(), index=True),
    Column('updated_at', DateTime, server_default=func.now(), onupdate=func.now())
)

cloud_company_process_fail_history = Table(
    'cloud_company_process_fail_history', metadata,
    Column('id', UUID(as_uuid=True), primary_key=True, index=True,
           server_default=text("uuid_generate_v4()"), unique=True),
    Column('company_id', Integer, ForeignKey('cloud_company.company_id'),
           nullable=False, index=True),
    Column('import_type', Integer, nullable=False, index=True),
    Column('import_error_type', Integer, nullable=False, index=True),
    Column('elastic_hash', String(length=255), nullable=False, index=True, unique=True),
    Column('data_hash', String(length=255), nullable=False, index=True),
    Column('file_path', String(length=255), nullable=False, index=True),

    # User profile for every transaction
    Column('full_name', String(length=255), nullable=True, index=True),
    Column('user_id', Integer, nullable=True, index=True),
    Column('active_history', Boolean, default=True),

    Column('created_at', DateTime, server_default=func.now(), index=True),
    Column('updated_at', DateTime, server_default=func.now(), onupdate=func.now())
)

company_export_history = Table(
    'company_export_history', metadata,
    Column('id', UUID(as_uuid=True), primary_key=True, index=True,
           server_default=text("uuid_generate_v4()"), unique=True),
    Column('company_id', Integer, ForeignKey('cloud_company.company_id'),
           nullable=False, index=True),

    Column('query_hash', String(length=255), nullable=False, index=True),
    Column('file_path', String(length=255), nullable=False, index=True),

    # User profile for every transaction
    Column('full_name', String(length=255), nullable=True, index=True),
    Column('user_id', Integer, nullable=False, index=True),

    Column('export_data', JSON, nullable=True),

    Column('export_type', Integer, nullable=True, index=True),
    Column('deleted', Boolean, default=False),

    Column('exported', Boolean, default=False),

    Column('created_at', DateTime, server_default=func.now(), index=True),
    Column('updated_at', DateTime, server_default=func.now(), onupdate=func.now())

)

company_statistic = Table(
    'company_statistic', metadata,
    Column('id', UUID(as_uuid=True), primary_key=True, index=True,
           server_default=text("uuid_generate_v4()"), unique=True),
    Column('company_id', Integer, ForeignKey('cloud_company.company_id'),
           nullable=False, index=True),

    Column('api_type', String(length=25), index=True),  # GET, POST
    Column('api_name', String(length=25), index=True),  # PRODUCTS, LOCATIONS
    Column('api_count', BigInteger),  # 0,1,2,3,4,5

    Column('date_statistic', Date, index=True),  # 2017-05-12

    Column('created_at', DateTime, server_default=func.now(), index=True),
    Column('updated_at', DateTime, server_default=func.now(), onupdate=func.now())

)

company_parameters = Table(
    'company_parameters', metadata,
    Column('id', UUID(as_uuid=True), primary_key=True, index=True,
           server_default=text("uuid_generate_v4()"), unique=True),
    Column('company_id', Integer, ForeignKey('cloud_company.company_id'),
           nullable=False, index=True),
    Column('key', String(), index=True),
    Column('value', String()),
    Column('created_at', DateTime, server_default=func.now(), index=True),
    Column('updated_at', DateTime, server_default=func.now(), onupdate=func.now())
)

# VEND FAIL HISTORY

vend_fail_history = Table(
    'vend_fail_history', metadata,
    Column('id', UUID(as_uuid=True), primary_key=True, index=True, server_default=text("uuid_generate_v4()"),
           unique=True),
    Column('company_id', Integer, ForeignKey('cloud_company.company_id'), nullable=False, index=True),
    Column('import_type', Integer, nullable=False, index=True),
    Column('import_error_type', Integer, nullable=False, index=True),
    Column('elastic_hash', String(length=255), nullable=False, index=True, unique=True),
    Column('main_elastic_hash', String(length=255), nullable=True, index=True),
    Column('data_hash', String(length=255), nullable=False, index=True),
    Column('file_path', String(length=255), nullable=False),

    # User profile for every transaction
    Column('full_name', String(length=255), nullable=True, index=True),
    Column('user_id', Integer, nullable=True, index=True),
    Column('active_history', Boolean, default=True),
    Column('created_at', DateTime, server_default=func.now(), index=True),
    Column('updated_at', DateTime, server_default=func.now(), onupdate=func.now())
)

# VEND SUCCESS HISTORY

vend_success_history = Table(
    'vend_success_history', metadata,
    Column('id', UUID(as_uuid=True), primary_key=True, index=True, server_default=text("uuid_generate_v4()"),
           unique=True),
    Column('company_id', Integer, ForeignKey('cloud_company.company_id'), nullable=False, index=True),
    Column('import_type', Integer, nullable=False, index=True),
    Column('elastic_hash', String(length=255), nullable=False, index=True),
    Column('data_hash', String(length=255), nullable=False, index=True),
    Column('import_data', JSON, nullable=False),
    Column('file_path', String(length=255), nullable=False),
    Column('cloud_inserted', Boolean, default=False),
    Column('partial', Boolean, default=False),

    # User profile for every transaction
    Column('full_name', String(length=255), nullable=True),
    Column('user_id', Integer, nullable=True, index=True),
    Column('statistics', JSON, nullable=True),
    Column('cloud_results', JSON, nullable=True),
    Column('created_at', DateTime, server_default=func.now(), index=True),
    Column('updated_at', DateTime, server_default=func.now(), onupdate=func.now())
)

vend_device_history = Table(
    'vend_device_history', metadata,
    Column('id', BigInteger, primary_key=True, index=True, autoincrement=True),
    Column('device_pid', String(length=255), nullable=False, index=True),
    Column('machine_id', String(length=255), nullable=False, index=True),
    Column('created_at', DateTime, server_default=func.now(), index=True),
    Column('updated_at', DateTime, server_default=func.now(), onupdate=func.now()),
    Column('file_timestamp', DateTime,  index=True),
    Column('company_id', Integer, ForeignKey('cloud_company.company_id'), nullable=False, index=True),
    Column('import_filename', String(length=255), nullable=False, index=True),
    Column('zip_filename', String(length=255), nullable=False, index=True),
    Column('import_type', String(length=255), nullable=False, index=True),
    Column('actual_machine', Boolean, default=True),
    Column('data', JSON, nullable=False),
)