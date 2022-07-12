"""Utilities for the test.sh script"""
import json
import os
from pandas import ExcelWriter
import subprocess
import sys


def stop_processes():
    """Stop gunicorn and consumer processes, if running."""
    gunicorn_processes = subprocess.run(["pgrep", "gunicorn"], stdout=subprocess.PIPE)
    consumer_processes = subprocess.run(["pgrep", "-f", "consume.py"], stdout=subprocess.PIPE)
    scheduler_processes = subprocess.run(["pgrep", "-f", "scheduler_task.py"], stdout=subprocess.PIPE)

    gunicorn_pids = gunicorn_processes.stdout.decode('utf-8')
    consumer_pids = consumer_processes.stdout.decode('utf-8')
    scheduler_pids = scheduler_processes.stdout.decode('utf-8')

    gunicorn_pids_list = gunicorn_pids.split('\n')
    consumer_pids_list = consumer_pids.split('\n')
    scheduler_pids_list = scheduler_pids.split('\n')
    # delete last item, which is return value of the subprocess.run command.
    del gunicorn_pids_list[-1]
    del consumer_pids_list[-1]
    del scheduler_pids_list[-1]

    for gunicorn_process in gunicorn_pids_list:
        subprocess.run(["kill", gunicorn_process])
    for consumer_process in consumer_pids_list:
        subprocess.run(["kill", consumer_process])
    for scheduler_process in scheduler_pids_list:
        subprocess.run(["kill", scheduler_process])


def change_test_value(testing_value):
    """Change value to TEST envdir file."""
    with open(os.path.join('../../.envdir', 'TEST'), 'r+') as config_file:
        testing = json.load(config_file)

        testing['active'] = testing_value

        # get the cursor to the beginning of file
        config_file.seek(0)
        json.dump(testing, config_file, indent=3)
        # remove the old content
        config_file.truncate()
    print('success')


def main():
    if 'stop_processes' in args:
        stop_processes()
    elif 'test_activate' in args:
        change_test_value(True)
    elif 'test_inactivate' in args:
        change_test_value(False)


def log_test(error_type, test_path):
    print(error_type, test_path)
    #ExcelWriter('tests_log.xlsx')



def test_suite(import_type=None):
    #suite = unittest.TestSuite()
    #runner = unittest.TestRunner()
    import_type = ''
    #tests = get_enum_tests(import_type)
    #for test in tests:
    #    suite.addTest(test)
    #runner.run(suite())


if __name__ == '__main__':
    args = sys.argv
    main()
