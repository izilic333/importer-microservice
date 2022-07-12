from database.cloud_database.connection.connection import Base

"""

    Cloud models referent by normalize name.

"""

client = Base.classes.clients
client_type = Base.classes.client_types
company = Base.classes.vending_companies
company_access = Base.classes.administration_companyaccess
custom_user = Base.classes.custom_users
layout_column = Base.classes.layout_columns
location = Base.classes.machine_locations
machine = Base.classes.machines
machine_operation = Base.classes.administration_machineoperation
meter_type = Base.classes.meter_types
machine_type = Base.classes.machine_types
machine_column = Base.classes.machine_columns
machine_cluster = Base.classes.machineclusters
machine_assigned_cluster = Base.classes.machineclusters_assigned_machines
machine_category = Base.classes.machine_categories
region = Base.classes.regions
region_parent = Base.classes.regions
product = Base.classes.products
product_rotation_group = Base.classes.product_rotation_groups
product_template = Base.classes.product_templates
product_category = Base.classes.product_categories
tax_rate = Base.classes.tax_rates
user = Base.classes.auth_user
user_role = Base.classes.user_role_template
machine_data = Base.classes.machine_data
operation = Base.classes.administration_machineoperation
tags = Base.classes.tags
machines_tags = Base.classes.machines_tags
locations_working_time = Base.classes.machine_locations_working_time
time_interval = Base.classes.time_interval
users_assigned_companies = Base.classes.custom_users_assigned_companies
vends = Base.classes.vends
device_machine = Base.classes.administration_device_machines
device_type = Base.classes.administration_devicetype
device = Base.classes.administration_device
importer_company = Base.classes.administration_vendingcompanyimporter
importer_type = Base.classes.administration_importtype
importer_company_ftp = Base.classes.administration_importerftpconfiguration
importer_company_ftp_details = Base.classes.administration_importerftpconfigurationmasterdata
product_templates = Base.classes.product_templates
layout_columns = Base.classes.layout_columns
machine_recipes = Base.classes.machine_recipes
layout_components = Base.classes.layout_components
recipe = Base.classes.recipes
combo_recipe = Base.classes.combo_recipes
layout_columns_tags = Base.classes.layout_columns_tags
packing_name = Base.classes.packing_name
product_packing_size = Base.classes.product_packing_size
product_rotation_groups = Base.classes.product_rotation_groups
warehouse = Base.classes.administration_warehouse
product_rotation_assignments = Base.classes.product_rotation_assignments
product_components = Base.classes.product_components
product_component_items = Base.classes.product_component_items

