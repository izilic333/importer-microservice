#!/bin/bash
###
# Used for running tests with the importer test database.
# What it does:
# Stops processes, changes the TEST value, starts processes, runs db migrations,
# runs the tests, stops the processes, changes back TEST value.
###

# show instructions if "-h" argument provided
if [ "$1" == "-h" ]
then
    echo "Usage:"
    echo "./test.sh"
    echo "./test.sh common.validators.csv.test_csv_validator"
    echo "./test.sh common.validators.csv.test_csv_validator.TestCsvFileValidator"
    echo "./test.sh common.validators.csv.test_csv_validator.TestCsvFileValidator.test_process_ftp_files__locations"
    echo "./test.sh no-migrate"
    echo "./test.sh path.to.test_file no-migrate"
    echo "Make sure you have added appropriate envdir TEST file with fields:"
    echo "active, user_id, company_id, username, password"
    echo "user_id and company_id are for cloud data."
    echo "username and password are for FTP access."
    echo "Make sure you have dependencies running, like cloud app, Elasticsearch service,"
    echo "that you are using proper virtual environment,"
    echo "and that you have created the importer_test database (with extension) and have set access fields in envdir DATABASES."
    exit 0
fi

echo Starting testing...

# stop flask, consumer, scheduler
echo -n "Stopping current processes... "
envdir ../../.envdir python test.py stop_processes
echo "done"

# change TEST to active
echo -n "Changing TEST to active... "
activate_test=$(envdir ../../.envdir python test.py test_activate)
echo $activate_test
if [ "${activate_test}" == "success" ]
then
    echo "continuing..."
else
    echo "failed modifying TEST envdir data"
    echo "exiting..."
    exit 1
fi

# if "no-migrate" provided in command arguments, skip doing migrations (saves time)
migrate=true
for arg in "$@"
    do
        if [ $arg == "no-migrate" ]
        then
            migrate=false
            break
        fi
    done
if ! $migrate;
then
    echo "skipping migrations..."
else
    # run migrations on the test database
    echo "Migrating the test db with alembic upgrade..."
    envdir ../../.envdir alembic -c database/company_database/alembic.ini upgrade head
    echo "and syncing database with cloud data..."
    envdir ../../.envdir python database/cloud_database/core/company_query.py
    echo "done"
fi

# start flask, consumer, scheduler. don't show output
echo -n "Starting Flask..."
envdir ../../.envdir gunicorn -b 127.0.0.1:5000 core.flask.dispatch:app &> /dev/null &
echo "done"
echo -n "Starting consumer... "
envdir ../../.envdir python common/rabbit_mq/consumers/consume.py &> /dev/null &
echo "done"
echo -n "Starting scheduler... "
envdir ../../.envdir python common/custom_q/scheduler_task.py &> /dev/null &
echo "done"

# run unittest
echo Running unit tests...
if [ "$1" == "no-migrate" ]
then
    envdir ../../.envdir coverage run -m unittest $2
else
    envdir ../../.envdir coverage run -m unittest $1
fi
echo "done"

# ask for user choices
echo -n "Do you want to generate coverage html? [yes/NO] "
read html
if [ "$html" == "yes" ]
then
    echo "Generating coverage html..."
    envdir ../../.envdir coverage html
    echo "done"
fi

# stop flask, consumer, scheduler
echo "Stopping processes..."
envdir ../../.envdir python test.py stop_processes
echo "done"

# change TEST to inactive
echo -n "Changing TEST to inactive... "
envdir ../../.envdir python test.py test_inactivate
echo "done"

echo "Finished testing."
