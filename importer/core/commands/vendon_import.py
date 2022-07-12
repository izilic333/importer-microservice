import click
from database.company_database.core.company_parameters import CompanyParameters
from common.apis.vendon.vendon_api_handler import VendonVendsApiJob
from common.logging.setup import logger


@click.command()
@click.option('--from_timestamp',  help='Import start timestamp', prompt=True)
@click.option('--to_timestamp',  help='Import end timestamp', prompt=True)
@click.option('--company_id',  help='company id', prompt=True)
@click.option('--machine_ext_id',  help='Machine external id in cloud')
def vendon_import(from_timestamp, to_timestamp, company_id, machine_ext_id):
    """Simple program that greets NAME for a total of COUNT times."""
    company_info = CompanyParameters.get_all_parameters()

    try:
        vends_fetcher_job = VendonVendsApiJob(company_id, company_info[int(company_id)]['vendon_user_id'])
        job_successful = vends_fetcher_job.fetch_vends(from_timestamp, to_timestamp, machine_ext_id, True)
        if not job_successful:
            print("Error during import (hash: {}) -> {}".format(vends_fetcher_job.elastic_hash if vends_fetcher_job.elastic_hash else "Hash error", vends_fetcher_job.error_message))
        return job_successful
    except Exception as e:
        logger.error("Vendon API job error "+str(e))
        logger.exception(e)
        return False


if __name__ == '__main__':
    vendon_import()
