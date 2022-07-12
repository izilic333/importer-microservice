import os
import re

EMAIL_REGEX = re.compile(r"^([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)?$")


def get_list_of_emails_in_case_of_csv_error():
    """
    in CSV_EMAIL_ERRORS file in .envdir jou need to put list of emails separated by ;
    # example: ivan.zilic9@gmail.com
    :return: string of emails  separated by ; or an empty string
    """
    csv_emails = os.environ.get('CSV_EMAIL_ERRORS')
    emails = []
    if csv_emails:
        for email in csv_emails.split(';'):
            if email and EMAIL_REGEX.fullmatch(email):
                emails.append(email)

    return ";".join(emails)
