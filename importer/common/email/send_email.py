import mimetypes
import smtplib
import os
import socket


from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from common.mixin.validation_const import return_import_type_name


def connection():
    server = smtplib.SMTP("email-smtp.eu-west-1.amazonaws.com", 587)
    server.starttls()
    server.login("AKIAJBXAHVMLFXRSAVMQ", "AtMgXVnCU/N8y9kGes04nEM+guz2SXtujXDvFDfsrlKE")

    return server


def send_email_error_on_file_parse(file_path, email, import_type, date_exp, delete=True):
    """

    :param file_path: local file path
    :param email: email
    :param import_type: MACHINES, LOCATIONS ..
    :param date_exp: export date 2015-06-18
    :param delete: delete local file
    :return: it will return success of false send email
    """

    conn = connection()

    array_of_emails = []

    convert_import_type = (
        return_import_type_name(import_type)
        if return_import_type_name(import_type) else import_type.name
    )

    if ';' in email:
        split_e = email.split(';')
        for x in split_e:
            if x:
                array_of_emails.append(x)

    msg = MIMEMultipart('mixed')

    msg['Subject'] = 'Importer status of %s' % convert_import_type
    msg['From'] = 'Ivan <ivan.zilic9@gmail.com>'
    msg['To'] = ", ".join(array_of_emails) if len(array_of_emails) else email

    # Body
    body = MIMEMultipart('alternative')
    body.attach(MIMEText("Cloud export {} for date {}".format(convert_import_type, date_exp)))
    msg.attach(body)

    ctype, encoding = mimetypes.guess_type(file_path)

    file_ex = file_path.split('/')
    ext_l = len(file_ex)

    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"

    maintype, subtype = ctype.split("/", ext_l-1)

    # Read file
    fp = open(file_path, "rb")
    attachment = MIMEBase(maintype, subtype)
    attachment.set_payload(fp.read())
    fp.close()

    encoders.encode_base64(attachment)

    attachment.add_header(
        "Content-Disposition", "attachment", filename=file_ex[ext_l-1]
    )
    msg.attach(attachment)

    conn.sendmail('<ivan.zilic9@gmail.com>',
                  array_of_emails if len(array_of_emails) else [email],
                  msg.as_string())

    # Delete file from OS
    if delete:
        os.remove(file_path)

    conn.quit()

def send_email_on_import_error(email, import_type, error_msg, company_id):

    conn = connection()

    array_of_emails = []

    convert_import_type = (
        return_import_type_name(import_type)
        if return_import_type_name(import_type) else import_type.name
    )

    if ';' in email:
        split_e = email.split(';')
        for x in split_e:
            if x:
                array_of_emails.append(x)

    msg = MIMEMultipart('mixed')

    msg['Subject'] = 'Importer errors on %s' % convert_import_type
    msg['From'] = 'Ivan <ivan.zilic9@gmail.com>'
    msg['To'] = ", ".join(array_of_emails) if len(array_of_emails) else email

    # Body
    body = MIMEMultipart('alternative')
    body.attach(MIMEText("Televendcloud errors for import of {} for company id {}-> {}".format(convert_import_type, company_id, error_msg)))
    msg.attach(body)

    conn.sendmail('<ivan.zilic9@gmail.com>',
                  array_of_emails if len(array_of_emails) else [email],
                  msg.as_string())

    conn.quit()

def send_email_on_general_error(email, error_msg):
    conn = connection()

    array_of_emails = []


    if ';' in email:
        split_e = email.split(';')
        for x in split_e:
            if x:
                array_of_emails.append(x)

    msg = MIMEMultipart('mixed')

    msg['Subject'] = 'Importer errors on {}'.format(socket.gethostname())
    msg['From'] = 'Ivan <ivan.zilic9@gmail.com>'
    msg['To'] = ", ".join(array_of_emails) if len(array_of_emails) else email

    # Body
    body = MIMEMultipart('alternative')
    body.attach(MIMEText("Cloud errors for import-> {}".format(error_msg)))
    msg.attach(body)

    conn.sendmail('<ivan.zilic9@gmail.com>',
                  array_of_emails if len(array_of_emails) else [email],
                  msg.as_string())

    conn.quit()

