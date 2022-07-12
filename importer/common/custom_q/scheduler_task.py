

from datetime import datetime
from time import sleep
from apscheduler.schedulers.blocking import BlockingScheduler as Scheduler
from common.mixin.validation_const import ImportType

from database.cloud_database.core.company_query import CloudLocalDatabaseSync
from database.company_database.core.query_export import ExportEmailHistory

from common.mixin.mixin import generate_hash_for_json
from common.validators.csv.csv_validator import CsvFileValidatorRemote
from common.logging.setup import logger
from common.email.send_email import send_email_on_general_error
from common.email.csv_error_email_list import get_list_of_emails_in_case_of_csv_error

"""

    Scheduler script for creating new imports and sync cloud with local database.
    Main thing is that script will delete or create jobs of import by parsing CLOUD DB changes.

"""

def run_sync_with_cloud():
    CloudLocalDatabaseSync.query_cloud_company_initial_insert()


def sync_importer_with_cloud():
    CloudLocalDatabaseSync.update_main_importer_status_on_cloud()


def export_emails():
    logger.info('<<< Cron export query started.')
    cron_export = CloudLocalDatabaseSync.setup_cron_job()
    logger.info('<<< Cron export query  results: {}'.format(cron_export))

    # Call external function for this
    logger.info('<<< Calling function for export emails.')
    ExportEmailHistory.export_daily(cron_export)


def execute_csv(data):
    logger.info('<<< Scheduler start import, data: {}'.format(data))
    max_retries = 3

    error_list = []

    while max_retries > 0:
        try:
            cfv = CsvFileValidatorRemote(data)
            cfv.get_remote_file()
            cfv.process_ftp_files()
            return
        except Exception as e:
            logger.error(str(e))
            error_list.append(str(e))
            max_retries -= 1
            sleep(10)

    logger.error('Errors during import. Reached max retries: {}'.format(str(error_list)))

    csv_email_errors = get_list_of_emails_in_case_of_csv_error()
    if csv_email_errors:
        import threading

        threading.Thread(
            target=send_email_on_general_error,
            args=(csv_email_errors, error_list), daemon=True
        ).start()


def scheduler_cloud_jobs(sched):
    """

    :param sched: packet of schedule
    :return: work in background job
    """

    def get_job(name):
        """

        :param name:  name of job
        :return: name of job if exists
        """
        split_name_input = name.split('$')[0]
        length_jobs = []

        for x in sched.get_jobs():
            job_name_full = x.name
            if '$' in job_name_full:
                length_jobs.append(job_name_full.split('$')[0])

        results = set(length_jobs)
        if split_name_input in results:
            return True
        else:
            return False

    def delete_all():
        for x in sched.get_jobs():
            job_name_full = x.name
            if '$' in job_name_full:
                sched.remove_job(x.id)

    def delete_from_q(name):
        """

        :param name: job name
        :return: delete job from scheduler
        """
        for x in sched.get_jobs():
            job_name_full = x.name
            if '$' in job_name_full:
                if job_name_full.split('$')[0] == name:
                    sched.remove_job(x.id)

    def check_job_status(name, data):
        """

        :param name: job name
        :param data: job data for FTP import JSON
        :return: it will only unschedule_job
        """
        if '$' in name:
            split_name_input = name.split('$')[0]
            for x in sched.get_jobs():
                job_name_full = x.name
                if '$' in job_name_full:

                    data_cron = generate_hash_for_json(data)
                    data_input = generate_hash_for_json(x.args[0])

                    job_split_name = job_name_full.split('$')
                    name_job = job_split_name[0]

                    if data_cron != data_input and split_name_input == name_job:
                        logger.info(
                            '<<< Interval task changed from --> {} >>> To: {}'
                            .format(x.args[0], data)
                        )
                        sched.remove_job(x.id)
                        # Detects if user has entered the hours or set the job trigger every x minutes!
                        if int(cron_job['cron_hour']) >= 1:
                            sched.add_job(
                                execute_csv,
                                'cron',
                                hour=str('*/') + str(cron_job['cron_hour']),
                                minute=int(cron_job['cron_min']),
                                start_date=datetime.now(),
                                args=[data],
                                name=name
                            )
                        else:
                            sched.add_job(
                                execute_csv,
                                'cron',
                                minute=str('*/') + str(data['cron_min']),
                                start_date=datetime.now(),
                                args=[data],
                                name=name
                            )

    query_data = CloudLocalDatabaseSync.setup_cron_job()

    if query_data:
        total_in_q = []
        if len(sched.get_jobs()):
            for x in sched.get_jobs():
                if '$' in x.name:
                    total_in_q.append(x.name.split('$')[0])

        total_in_cloud = []

        is_vend_import = {x.value['id']: x.value['vend'] for x in ImportType}

        for cron_job in query_data:

            if is_vend_import[cron_job['category_import']]:
                continue

            name_job = (
                '{}_{}_{}${}'.format(
                    cron_job['category_import'],
                    cron_job['company'],
                    cron_job['id'],
                    str(cron_job['cron_hour'])+str(cron_job['cron_min'])
                )
            )
            total_in_cloud.append('{}_{}_{}'.format(
                    cron_job['category_import'],
                    cron_job['company'],
                    cron_job['id']
                ))
            if get_job(name_job):
                logger.info('>>> Job is already in Q: {}'.format(name_job))
                check_job_status(name_job, cron_job)
            else:
                logger.info('<<< Adding new interval task. {}'.format(cron_job))
                try:
                    # Detects if user has entered the hours or set the job trigger every x minutes!
                    if int(cron_job['cron_hour']) >= 1:
                        sched.add_job(
                            execute_csv,
                            'cron',
                            hour=str('*/') + str(cron_job['cron_hour']),
                            minute=int(cron_job['cron_min']),
                            args=[cron_job],
                            name=name_job
                        )
                    else:
                        sched.add_job(
                            execute_csv,
                            'cron',
                            minute=str('*/') + str(cron_job['cron_min']),
                            args=[cron_job],
                            name=name_job
                        )
                except Exception as e:
                    logger.error('Problems on adding con job! {}'.format(e))

        if len(set(total_in_q)) > 0 and set(total_in_q) != len(total_in_cloud):
            diff_in = list(set(total_in_q) - set(total_in_cloud))
            for x in diff_in:
                delete_from_q(x)

    else:
        delete_all()


def main():
    # Start the scheduler
    scheduler = Scheduler()

    # Schedules job_function to be run once each second
    scheduler.add_job(run_sync_with_cloud, 'interval', seconds=5)
    scheduler.add_job(sync_importer_with_cloud, 'interval', seconds=120)
    scheduler.add_job(scheduler_cloud_jobs, 'interval', seconds=5, args=[scheduler])

    scheduler.start()


if __name__ == '__main__':
    main()
