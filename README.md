# Python microservice: importer

Project description:

This service solves the specific problem of importing data into the database. 
There is a high validation level of imported data and interdependencies between the relationships of individual tables in the database.
This service handle different and complex relatsions betwen DB tables:

    - One-To-One Relationship
    - One-To-Many Relationship
    - Many-To-Many Relationship
    
The microservice communicates with a core Django application defined in the cloud.
The microservice receives specific CSV files and imports certain entities into the database.
There is a substantial interdependence between imported entities concerning mandatory fields and specific imported content.
This microservice solves the problem and needs of a specific company, and customers of that company expressed a desire to import data into the database using CSV file.

This service imports large amounts of data adapted simply for the end-users of this company.

About service:

- handle import for a various entities with high DB relations of each other
    - some different DB tables for import (machines, machine types, locations, regions, products, planograms, clients, users, packings)
- for some complex import types, this service can build predefined structured entities for the database actions like insert, update, delete, in one         transaction using the specific functions on the postgres database.
- this service uses elasticsearch to log all validation actions, which is finally displayed to the end-user as an import result for each of the 
  imported entries.
- The service has its scheduler for importing data into DB so that the end-user can put some CSV data into specific FTP, and this service
  will periodically import this data into DB.
- There is UI configuration for scheduler import per specific import type 

Tech stack:

    - Flask
    - RabbitMQ
    - Elasticsearch
    - SQLAlchemy
    - Redis
    - Postgres
    - PL/pgSQL

Microservice dependencies on another project in cloud:

    - Django
    - Django ORM
    - Django celery

![My Image](/importer/img/django_app.png)


The service has import history and logs all actions into elastic search from the beginning of the process to the end, and end--user can follow import process for specific import type or specific record:

![My Image](/importer/img/import_history.png)

Here is some import actions from elastic search:

![My Image](/importer/img/import_actions.png)


Here is example for some imported data:

![My Image](/importer/img/imported_data.png)

![My Image](/importer/img/imported_data2.png)

The service has scheduler, and periodically download end-user data from FTP and import this data into DB.

![My Image](/importer/img/ftp_import_config.png)

Here is import type config:
![My Image](/importer/img/import_type_config.png)

Here is some app config:
![My Image](/importer/img/app_config.png)




Python version : >=3.5
Virtual env setup :

    virtualenv --python=/usr/bin/python3 --no-site-packages --no-wheel --no-setuptools venv
    source venv/bin/activate
    pip install setuptools wheel
    cd importer
    pip install -r requirements.txt
    
Enable importer in virtualenv if you are working from CLI: 
    
    add2virtualenv importer

-----------------------------------------------------------------------

# Envdir setup

example in `envdir_example` folder

- DATABASE_CONNECTION: database connection string
- REDIS_URI: redis url
- RABBIT_MQ: rabbit mq url
- FLASK_CONFIG: example of json will be provided
- FLASK_SERVER_MODE: production/test
- ELASTIC_SEARCH: host for elastic
- CLOUD_API_ROUTE: URL paths   
- CLOUD_URL: 
- CSV_EMAIL_ERRORS: ";" separated list of emails  
- EMAIL_CONNECTION  
- IMPORT_TYPE_REDIS_KEY_DURATION  
- INITIAL_FTP_DIR_CONFIG  
- LOG_PATH: path to log folders  
- PIKA_CONNECTION
- TEST
------------------------------------------------------------------------

# Create database and run alembic migrations


Activate virtualenv and:
    
    cd importer/database/company_database
    envdir ../../../../.envdir alembic upgrade head

# Start services

Example of starting services :

    cd importer
    
    envdir ../.envdir gunicorn -w 2 -b 127.0.0.1:5000 core.flask.dispatch:app --reload

    envdir ../.envdir python common/rabbit_mq/consumers/consume.py

    envdir ../.envdir python common/custom_q/scheduler_task.py
    
    envdir ../.envdir python common/rabbit_mq/consumers/vend_consume.py
    
    envdir ../.envdir python common/rabbit_mq/consumers/cpi_vend_consume.py
    
    envdir ../.envdir python common/rabbit_mq/consumers/dex_vend_consume.py
     
    envdir ../.envdir python common/rabbit_mq/consumers/vend_download_consume.py
     
    envdir ../.envdir python common/custom_q/vend_scheduler_task.py
     
    envdir ../.envdir python common/rabbit_mq/consumers/csv_validator_consume.py 

------------------------------------------------------------------------

# Packages with URLS

- Flask: [http://flask.pocoo.org/]
- Nameko: [http://nameko.readthedocs.io/en/stable/]
- PyTransmit: [http://pytransmit.readthedocs.io/en/latest/]
- csvvalidator: [https://github.com/alimanfoo/csvvalidator]
- SQLAlchemy: [https://www.sqlalchemy.org/]
- envdir: [https://pypi.python.org/pypi/envdir]
- Flask-APScheduler: [https://github.com/viniciuschiele/flask-apscheduler]
- Flask-Elasticsearch: [https://github.com/chiangf/Flask-Elasticsearch]



# Local elastic search develop purpose
- URL :  [http://www.itzgeek.com/how-tos/linux/ubuntu-how-tos/install-elasticsearch-on-centos-7-ubuntu-14-10-linux-mint-17-1.html]

*Change config files for local development only*:

Path: /etc/elasticsearch/elasticsearch.yml

- network.bind_host: localhost
- cluster.name: importer
- node.name: "x x"
- index.number_of_shards: 1
- index.number_of_replicas: 0
- discovery.zen.ping.multicast.enabled: false
- cluster.routing.allocation.disk.threshold_enabled: false
- cluster.routing.allocation.disk.watermark.low: 500mb
- cluster.routing.allocation.disk.watermark.high: 600mb
- cluster.info.update.interval: 1m

Restart elastic search and test it with.

curl -X GET 'http://localhost:9200'

# *Alembic* config for local databse (initial setup)

- Path: /importer/database/company_database
- envdir ../../../../.envdir alembic revision --autogenerate -m "Create database"
- envdir ../../../../.envdir alembic upgrade head


# Application name: Importer

Virtual env setup
- add2virtualenv importer

-----------------------------------------------------------------------

Python version : 3.5.*
- Envdir for global variables.

Example of starting flask or nameko services.

- envdir ../.envdir nameko run --config settings.yaml run_server
- envdir ../.envdir python core/http_server/app_name.py # Only example

------------------------------------------------------------------------

# Create dirs for exports in path as envdir
1) **email_exports**
2) **exports**

# Envdir setup

- DATABASE_CONNECTION:
- `{
    "importer_database": "postgresql://x:x@localhost/x",
    "cloud_database": "postgresql://x:x@localhost/x",
    "test": true,
    "importer_db_test": "postgresql://x:x@localhost/x"
    }`
- REDIS_URI
- `{
    "host": "localhost",
    "port": 6379
}`
- RABBIT_MQ:
- `amqp://guest:guest@localhost`
- FLASK_CONFIG:
- `{
     "debug": true,
    "amqp_uri": "amqp://guest:guest@localhost",
    "host": "localhost",
    "port": 5000,
    "use_debugger": true,
    "use_reloader": true,
    "token_generator": "-%76lw4fv9oom1y-gy9%az1vu1p1ke%5urrq5m1=%b9#7zilh!"
}`
- FLASK_SERVER_MODE: production/test
- ELASTIC_SEARCH:
- `{
    "host": "localhost",
    "port": 9200,
    "index_vend": "index_vend-"
}`
- EMAIL_CONNECTION: connection for email
- CLOUD_API_ROUTE: api route
- `
{
    "machine": "/api/v2/importer/machines",
    "region": "/api/v2/importer/regions",
    "location": "/api/v2/importer/locations",
    "machine_type": "/api/v2/importer/machine_type"
}
`
- CLOUD_URL: cloud url
- `
{
    "dev": "http://127.0.0.1:8000",
    "prod": "-",
    "debug": true
}
`
- INITIAL_FTP_DIR_CONFIG :Config dir for FTP
-`
{
    "main_dir": "ftp_files",
    "work": "work",
    "zip_working_dir": "zip_working_dir",
    "history": "history_files",
    "history_success": "success",
    "history_fail": "fail",
    "download_history": "download_history",
    "main_vend_dir": "vend_files",
    "vend_work": "work",
    "vends_zip_working_dir": "zip_working_dir",
    "vend_history": "history_files",
    "vend_success_history": "success",
    "vend_fail_history": "fail",
    "vend_downloads_dir": "downloads"
}
`
- PIKA_CONNECTION : Configuration for PIKA
-`
{
    "host": "localhost",
    "port": 5672,
    "username": "guest",
    "password": "guest",
    "rabbit_url": "http://127.0.0.1:15672/api/queues/%2F/{}/get"
}
`
- TEST: Configuration for TEST
-`
{
   "company_id": 7,
   "ftp_home": "",
   "ftp_host":"ftp.selecta.com",
   "test_log": "true",
   "password": "4&N3ZSwk+N5$C",
   "user_id": 306,
   "username": "Televend",
   "active": false
}
`
------------------------------------------------------------------------

# Packages with URLS

- Flask: [http://flask.pocoo.org/]
- Nameko: [http://nameko.readthedocs.io/en/stable/]
- PyTransmit: [http://pytransmit.readthedocs.io/en/latest/]
- csvvalidator: [https://github.com/alimanfoo/csvvalidator]
- SQLAlchemy: [https://www.sqlalchemy.org/]
- envdir: [https://pypi.python.org/pypi/envdir]
- Flask-APScheduler: [https://github.com/viniciuschiele/flask-apscheduler]
- Flask-Elasticsearch: [https://github.com/chiangf/Flask-Elasticsearch]



# Local elastic search develop purpose
- URL: [http://www.itzgeek.com/how-tos/linux/ubuntu-how-tos/install-elasticsearch-on-centos-7-ubuntu-14-10-linux-mint-17-1.html]

Change config files:

Path: /etc/elasticsearch/elasticsearch.yml

- network.bind_host: localhost
- cluster.name: importer
- node.name: "x x"
- index.number_of_shards: 1
- index.number_of_replicas: 0
- discovery.zen.ping.multicast.enabled: false
- cluster.routing.allocation.disk.threshold_enabled: false
- cluster.routing.allocation.disk.watermark.low: 500mb
- cluster.routing.allocation.disk.watermark.high: 600mb
- cluster.info.update.interval: 1m

Path: /etc/default/elasticsearch
- START_DAEMON=true

Restart elastic search and test it with.

curl -X GET 'http://localhost:9200'

# *Alembic* config for local databse (initial setup)

- Path: televendcloud-microservices/importer/database/company_database
- envdir ../../../../.envdir alembic revision --autogenerate -m "Create database"
- envdir ../../../../.envdir alembic upgrade head


# Gunicorn start flask app
envdir ../../../../.envdir gunicorn -w 2 -b 127.0.0.1:5000 dispatch:app --reload

# How to start all services

# PostgresSQL

- 1) Create extension for UUID on database importer: create EXTENSION if not EXISTS "uuid-ossp";

# Importer:

- 1) Run flask to get API for testing,
- 2) Run consumers for Q : 
        Masterdata: envdir ../.envdir python common/rabbit_mq/consumers/consume.py
        CPI: envdir ../.envdir python common/rabbit_mq/consumers/cpi_vend_consume.py
        DEX: envdir ../.envdir python common/rabbit_mq/consumers/dex_vend_consume.py
        Downloader (CPI/DEX): envdir ../.envdir python common/rabbit_mq/consumers/vend_download_consume.py
        Vendon: envdir ../.envdir python common/rabbit_mq/consumers/vend_consume.py
        

- 3) Open pgAdmin so you can see table *cloud_company_process_fail_history* for history log
- 4) Run schedulers : 
        Masterdata: envdir ../../.envdir python common/custom_q/scheduler_task.py
        Vends: envdir ../../.envdir python common/custom_q/vend_scheduler_task.py
- 5) Sync with branch importer
- 6) Install all requirements.txt -> *pip install -r requirements.txt*

# Elastic search for LOCAL DEVELOPMENT, DO NOT USE IN PRODUCTION:

 1) Remove all from history : curl -X DELETE 'http://localhost:9200/_all'
 2) INDEX = import_index_prefix from ELASTIC_SEARCH envdir + company_process 
 2) Trace your log:  http://localhost:9200/INDEX/_search?pretty=true&q=*:*

# Cloud:

- 1) Create your configuration for FTP import,
- 2) Run import consumers in cloud: 
        Masterdata: python manage.py import_consume
        Vends: python manage.py vend_import_consume

# FTP 

- 1) Connection: *ip*: 52.59.249.225 *username*: najbolji  *password*: jQq2QmmpGv8yBAG
- 2) Path: */home/najbolji/test*
- 3) Or make your own local FTP :)
