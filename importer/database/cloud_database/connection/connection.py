from sqlalchemy import create_engine, MetaData
from common.urls.urls import cloud_database_connection
from sqlalchemy.ext.automap import generate_relationship, automap_base
from sqlalchemy.orm import interfaces

"""
    Cloud tables only for Importer data.
    This is main include to importer and by this setup importer works with CLOUD tables.
    Relationships generate, and reflect data included.

"""

def _gen_relationship(base, direction, return_fn,
                                attrname, local_cls, referred_cls, **kw):
    if direction is interfaces.ONETOMANY:
        kw['cascade'] = 'all, delete-orphan'
        kw['passive_deletes'] = True
    return generate_relationship(base, direction, return_fn,
                                 attrname, local_cls, referred_cls, **kw)

metadata = MetaData()

cloud_database_engine = create_engine(
    '{}'.format(cloud_database_connection), convert_unicode=True, echo=False
)

metadata.reflect(cloud_database_engine,
                 only=['custom_users', 'auth_user', 'vending_companies',
                       'machines',
                       'machine_locations',
                       'layout_columns',
                       'regions',
                       'machine_types',
                       'machine_columns',
                       'machineclusters',
                       'machineclusters_assigned_machines',
                       'machine_categories',
                       'products',
                       'product_templates',
                       'tax_rates',
                       'administration_vendingcompanyimporter',
                       'administration_importtype',
                       'administration_importerftpconfiguration',
                       'administration_importerftpconfigurationmasterdata',
                       'administration_warehouse',
                       'clients', 'client_types',
                       'user_role_template', 'machine_data', 'administration_machineoperation',
                       'tags', 'machines_tags', 'machine_locations_working_time', 'time_interval',
                       'custom_users_assigned_companies', 'product_categories', 'vends', 'administration_device',
                       'administration_device_machines', 'product_templates', 'layout_columns', 'layout_components',
                       'recipes', 'combo_recipes', 'layout_columns_tags', 'meter_types', 'product_rotation_groups',
                       'packing_name', "product_packing_size", "product_rotation_assignments", "machine_recipes",
                       'product_components', 'product_component_items', 'layout_columns_tags'
                       ]
                 )
Base = automap_base(metadata=metadata)
Base.prepare(generate_relationship=_gen_relationship)
