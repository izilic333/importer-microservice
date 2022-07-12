import os
import json

# Database setup
databases = json.loads(os.environ['DATABASE_CONNECTION'])
testing = json.loads(os.environ['TEST'])

cloud_database_connection = databases['cloud_database']
if testing['active']:
    importer_database_connection = databases['importer_db_test']
else:
    importer_database_connection = databases['importer_database']

# Elasticsearch config
elasticsearch_connection_url = json.loads(os.environ['ELASTIC_SEARCH'])

# Flask config
flask_config = json.loads(os.environ['FLASK_CONFIG'])

# Rabbitmq connection
rabbit_connection = os.environ['RABBIT_MQ']

# Redis connection
redis_connection = json.loads(os.environ['REDIS_URI'])

# Cloud urls
load_urls = json.loads(os.environ['CLOUD_API_ROUTE'])
url_path = ''

# Import type redis key duration
import_type_redis_key_duration = json.loads(os.environ['IMPORT_TYPE_REDIS_KEY_DURATION'])


# Load status
status_app = json.loads(os.environ['CLOUD_URL'])

if status_app['debug']:
    url_path += status_app['dev']
else:
    url_path += status_app['prod']

# Supported URL-S

machine_url = url_path + load_urls['machine']
region_url = url_path + load_urls['region']
location_url = url_path + load_urls['location']
machine_type_url = url_path + load_urls['machine_type']


# Create initial dirs

load_configuration_dir = json.loads(os.environ['INITIAL_FTP_DIR_CONFIG'])
load_log_basepath = json.loads(os.environ['LOG_PATH'])

MAIN_DIR = load_configuration_dir['main_dir']
WORK_DIR = os.path.join(MAIN_DIR, load_configuration_dir['work'])
ZIP_WORK_DIR = os.path.join(WORK_DIR, load_configuration_dir['zip_working_dir'])
HISTORY_MAIN_DIR = os.path.join(MAIN_DIR, load_configuration_dir['history'])
HISTORY_SUCCESS_DIR = os.path.join(HISTORY_MAIN_DIR, load_configuration_dir['history_success'])
HISTORY_FAIL_DIR = os.path.join(HISTORY_MAIN_DIR, load_configuration_dir['history_fail'])
DOWNLOAD_HISTORY_DIR = os.path.join(HISTORY_MAIN_DIR, load_configuration_dir['download_history'])


MAIN_VEND_DIR = load_configuration_dir['main_vend_dir']
VEND_WORK_DIR = os.path.join(MAIN_VEND_DIR, load_configuration_dir['vend_work'])
VEND_ZIP_WORK_DIR = os.path.join(VEND_WORK_DIR, load_configuration_dir['vends_zip_working_dir'])
VEND_HISTORY_MAIN_DIR = os.path.join(MAIN_VEND_DIR, load_configuration_dir['vend_history'])
VEND_HISTORY_SUCCESS_DIR = os.path.join(VEND_HISTORY_MAIN_DIR, load_configuration_dir['vend_success_history'])
VEND_HISTORY_FAIL_DIR = os.path.join(VEND_HISTORY_MAIN_DIR, load_configuration_dir['vend_fail_history'])
VEND_DOWNLOAD_HISTORY_DIR = os.path.join(VEND_HISTORY_MAIN_DIR, load_configuration_dir['vend_downloads_dir'])


# Pika connection
pika_config = json.loads(os.environ['PIKA_CONNECTION'])
pika_host = pika_config['host']
pika_port = int(pika_config['port'])
pika_username = pika_config['username']
pika_password = pika_config['password']
rabbit_mq_url = pika_config['rabbit_url']

