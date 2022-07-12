from apscheduler.schedulers.blocking import BlockingScheduler as Scheduler
from common.mixin.validation_const import ImportType, return_import_type_name
from database.cloud_database.core.company_query import CloudLocalDatabaseSync
from database.company_database.core.company_parameters import CompanyParameters
from common.mixin.mixin import generate_hash_for_json
from common.logging.setup import logger
from common.validators.vend_importer_validator.cpi_vend_processing import GetRemoteVendData
from common.apis.vendon.vendon_api_handler import VendonVendsApiJob


def execute_vend_processing(data):
    processing = GetRemoteVendData(data)
    processing.get_remote_file()
    import_type_name = return_import_type_name(int(data.get('category_import')))
    if import_type_name == 'CPI_VENDS':
        processing.processing_ftp_files(processor_type=import_type_name)
    if import_type_name == 'DEX_VENDS':
        processing.processing_ftp_files(processor_type=import_type_name)


def convert_hours_min_to_seconds(hour, minute):
    """

    :param hour: 1-12
    :param minute: 1-60
    :return: it will return total calculate in seconds
    """
    total = (int(hour) * 60 * 60)+(int(minute) * 60)
    return total


def scheduler_vend_cloud_jobs(sched):
    """
    This is main function for  schedule/unschedule jobs based on import configuration from cloud!

    :param sched: packet of schedule
    :return: work in background job
    """

    def get_job(name):
        """
        Check if exists job 'category_import_company$hour&minute'
        then return True otherwise return False

        :param name:  name of job
        :return: True if exist job otherwise return False
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
        This function decides whether a change has been made on cloud import configuration!
        If import configuration is changed it removes a job, preventing it from being run any more and
        schedules this new job to be completed on specified intervals!

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
                            '<<< Interval task changed from --> {} >>> To: {}'.format(x.args[0], data)
                        )
                        sched.remove_job(x.id)
                        sched.add_job(
                            execute_vend_processing,
                            'interval',
                            seconds=convert_hours_min_to_seconds(
                                data['cron_hour'], data['cron_min']
                            ),
                            args=[data],
                            name=name
                        )

    query_data = CloudLocalDatabaseSync.setup_cron_job()

    if query_data:
        total_in_q = []

        # check if exists job with name: category_import_company$hour&minute
        if len(sched.get_jobs()):
            for x in sched.get_jobs():
                if '$' in x.name:
                    total_in_q.append(x.name.split('$')[0])

        total_in_cloud = []

        # Get importer Vend configuration and make job like: category_import_company$hour&minute
        mapping_import_type_ids = [{'category_import': x.value['id'], 'vend': x.value['vend']} for x in ImportType]

        for cron_job in query_data:
            for ids in mapping_import_type_ids:
                if ids.get('category_import') == cron_job['category_import']:
                    if ids.get('vend'):
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
                        # If name_job exists check status, otherwise schedule new job!

                        if get_job(name_job):
                            check_job_status(name_job, cron_job)
                        else:
                            logger.info('<<< Adding new interval task. {}'.format(cron_job))
                            time_execute = convert_hours_min_to_seconds(
                                cron_job['cron_hour'], cron_job['cron_min']
                            )
                            sched.add_job(
                                execute_vend_processing,
                                'interval',
                                seconds=time_execute,
                                args=[cron_job],
                                name=name_job
                            )

        if len(set(total_in_q)) > 0 and set(total_in_q) != len(total_in_cloud):
            diff_in = list(set(total_in_q) - set(total_in_cloud))
            if diff_in:
                for x in diff_in:
                    delete_from_q(x)

    else:
        delete_all()


def execute_vendon_api(company_id, data):
    try:
        vends_fetcher_job = VendonVendsApiJob(company_id, data['vendon_user_id'])
        start_time, end_time = vends_fetcher_job.get_scheduled_timestamps()
        job_successful = vends_fetcher_job.fetch_vends(start_time, end_time)
        if job_successful:
            CompanyParameters.set_parameter(company_id, "vendon_last_fetched_timestamp_vends", end_time)

        return job_successful
    except Exception as e:
        logger.error("Vendon API job error "+str(e))
        return False

def scheduler_api_jobs(sched):

    def delete_from_q(name):
        """

        :param name: job name
        :return: delete job from scheduler
        """
        for x in sched.get_jobs():
            job_name_full = x.name
            if job_name_full == name:
                sched.remove_job(x.id)

    jobs_info = CompanyParameters.get_all_parameters()

    current_q_vendon = [job.name for job in sched.get_jobs() if job.name[:10] == 'vendon_api']
    configured_jobs = []

    for company_job_info in jobs_info:
        im = jobs_info[company_job_info]['vendon_interval_minutes_vends']
        if not im:
            continue
        interval_minutes = int(im)
        job_name = "vendon_api_{}-{}".format(company_job_info, interval_minutes)
        configured_jobs.append(job_name)

        if job_name in current_q_vendon:
            continue

        try:

            logger.info('<<< Adding new interval task. {}'.format(job_name))
            sched.add_job(
                execute_vendon_api,
                'interval',
                minutes=interval_minutes,
                args=[company_job_info, jobs_info[company_job_info]],
                name=job_name
            )
        except Exception as e:
            logger.error('Problems on adding cron job: {}'.format(e))

    for to_delete in list(set(current_q_vendon) - set(configured_jobs)):
        logger.info('<<< Removing interval task. {}'.format(to_delete))
        delete_from_q(to_delete)

    logger.info('All jobs in scheduled: {}'.format(sched.print_jobs()))

    return


def main():
    # Start the scheduler
    scheduler = Scheduler()

    # Schedules job_function to be run once each second
    scheduler.add_job(scheduler_api_jobs, 'interval', seconds=60, args=[scheduler])
    scheduler.add_job(scheduler_vend_cloud_jobs, 'interval', seconds=60, args=[scheduler])

    scheduler.start()


if __name__ == '__main__':
    main()
