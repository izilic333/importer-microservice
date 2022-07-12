import csv
import json
import os
import pandas as pd
import shutil
import zipfile
import zlib

from common.mixin.validation_const import (
    clientParser, machineParser, machineTypeParser,
    locationParser, regionsParser, productParser, packingParser
)
from common.mixin.validator_import import (
    WORKING_DIR, HISTORY_FAIL_DIR, STORE_DIR, ZIP_WORKING_DIR,
    HISTORY_SUCCESS_DIR, HISTORY_FILES_DIR
)


TEST_DATA = json.loads(os.environ['TEST'])
FTP_USERNAME = TEST_DATA['username']
FTP_HOME = TEST_DATA['ftp_home']


# example content used in testing
machine_content = {
    'all_fields': [
        (
        'Necta Fontana Trevi', 'nectatrev1', '1', '5',
        'comboa', '45', '12345678', '2017-09-05',
        'AW000FF558', '#tag', 'Macchina combo - Fontana Trevi šđčćž', 15,
        1, 1, 1, '1', '1',
        '1:23', '1:23', '1:23', '1:23',
        '1:23', '', '', 'W001', 'MC001'
        )
    ],
    'required': [
        ('Necta Fontana Trevi', 'nectatrev1', 0, 'trevi', 'comboa'),
        ('Necta Fontana Levi', 'nectalev1', 0, 'levi', 'combob')
    ]
}

location_content = {
    'all_fields': [
        (
        'Fontana di Trevi', 'trevi', 0, 'Piazza di Trev1 1, Rome',
        'municipio1','','','+39XXXXXXXXX',
        'trevi@ditta1.com','Fontana di Trevi','1111100','08:00-15:00#17:00-21:00'
        )
    ],
    'required': [
        ('Fontana di Trevi', 'trevi', 0) #, 'Piazza di Trev1 1, Rome')
    ]
}

machine_type_content = {
    'all_fields': [
        ('Combo A', 'comboa', 0)
    ],
    'required': [
        ('Combo A', 'comboa', 0)
    ]
}

region_content = {
    'all_fields': [
        ('Roma', 'rome', 0, 'italia')
    ],
    'required': [
        ('Roma', 'rome', 0)
    ]
}

product_content = {
    'all_fields': [
        (
        'BELVITA CHOCOLAT 50GR X30', 'B448', 1,
        1.36, 5.55, 13,
        'ABC123', 0.23, 1,
        'P456', 'Belvita Dark Chocolate Bar', 120,
        100, 8, 0, 1
        )
    ],
    'required': [
        ('BELVITA CHOCOLAT 50GR X30', 'B448', 1, 1.3)
    ]
}

#client_name; client_id; parent_client_id;client_type_id;client_action;
client_content = {
    'all_fields': [
        ('Audi', 'audi',
         0, 'car_company', 'TIP1', 0)
    ],
    'required': [
        ('Audi', 'audi', 0)
    ]
}

def write_csv(
    filepath,
    import_type,
    delimiter,
    validity,
    fields='required',
    content=None
):
    """
    Write a .CSV file, based on inputed parameters.

    Keyword arguments:
    filepath -- path to file to write to
    import_type -- one of ['machines', 'locations', 'regions'...]
    delimiter -- delimiter to use for separation of fields
    validity -- control if file should pass validation or not, is one of
        ['valid', 'missing_header', 'missing_field', 'wrong_header', 'empty_required_field']
    fields -- one of ['required', all_fields'] as defined in the parser for
        particular import_type
    content -- if None, predefined example content is used, else if not None
        allows passing custom example data
    """
    headers = None
    headers_modified = None
    content_modified = None

    if import_type == 'machines':
        headers = machineParser[fields]
        if content == None:
            content = machine_content[fields]

    elif import_type in [4, 'locations']:
        headers = locationParser[fields]
        # missing required field from locationParser?
        #if fields == 'required':
        #    headers.append('location_address')

        if content == None:
            content = location_content[fields]

    elif import_type in [13, 'machine_types']:
        headers = machineTypeParser[fields]
        if content == None:
            content = machine_type_content[fields]

    elif import_type in [5, 'regions']:
        headers = regionsParser[fields]
        if content == None:
            content = region_content[fields]

    elif import_type in [6, 'products']:
        headers = productParser[fields]
        if content == None:
            content = product_content[fields]

    elif import_type in [10, 'clients']:
        headers = clientParser[fields]
        if content == None:
            content = client_content[fields]

    elif import_type in [20, 'packings']:
        headers = packingParser[fields]
        if content == None:
            content = client_content[fields]

    # configure validity of the file
    if validity == 'missing_header':
        # make a copy of the list, so as to not modify the original list
        headers_modified = headers[:]
        headers_modified[-1] = ''
    elif validity == 'missing_field':
        content_modified = content[:]
        # convert type from tuple to list, modify and convert back to tuple
        content_modified[0] = list(content_modified[0])
        content_modified[0][-1] = ''
        content_modified[0] = tuple(content_modified[0])
    elif validity == 'wrong_header':
        headers_modified = headers[:]
        headers_modified[6] = 'wrong_header'
    elif validity == 'empty_required_field':
        content_modified = content[:]
        content_modified[0] = list(content_modified[0])
        content_modified[0][1] = '' # first field is required, make it blank
        content_modified[0] = tuple(content_modified[0])

    headers_final = headers_modified if headers_modified else headers
    content_final = content_modified if content_modified else content

    df = pd.DataFrame.from_records(content_final, columns=headers_final)
    df.to_csv(filepath, sep=delimiter, index=False)


def create_file_path(
    base_path,
    file_name,
    extension,
):
    """Creates a full file path string."""
    full_file_name = file_name + extension
    file_path = os.path.join(base_path, full_file_name)
    return file_path


def create_file(
    base_path='/home',
    file_name='empty_file',
    extension='.txt',
    import_type=None,
    delimiter=';',
    fields='required',
    validity='valid',
    content=None
):
    """
    Create file with specific extension in the FTP user's directory.

    Keyword arguments:
    base_path -- where we want file to be created
    file_name -- how we want file to be called
    extension -- specify extension
    import_type -- define which import type should this file be for
    delimiter -- delimiter
    fields -- one of ['required', 'all_fields']
    validity -- one of ['valid', 'missing_header', 'missing_field', 'wrong_header',
        'empty_required_field']
    """
    filepath = create_file_path(
        base_path=base_path,
        file_name=file_name,
        extension=extension
    )

    if extension == '.csv':
        write_csv(
            filepath=filepath,
            import_type=import_type,
            delimiter=delimiter,
            fields=fields,
            validity=validity,
            content=content
        )
    else:
        with open(filepath, 'w') as file_open:
            file_open.write('Some text here\n')
    return file_name, filepath


def create_zip(zip_path, file_paths, parent_folder=''):
    compression = zipfile.ZIP_DEFLATED
    with zipfile.ZipFile(zip_path, mode='w') as zf:
        for file_path in file_paths:
            zf.write(
                file_path,
                os.path.join(parent_folder, os.path.basename(file_path)),
                compress_type=compression
            )
    return zip_path


def clear_test_dirs():
    """
    Clear used directory of any files, so that tests are executed on a
    clean slate.
    """
    target_dirs = [
        WORKING_DIR,
        ZIP_WORKING_DIR,
        HISTORY_FILES_DIR,
        HISTORY_SUCCESS_DIR,
        HISTORY_FAIL_DIR,
        STORE_DIR,
        FTP_HOME
    ]
    for directory in target_dirs:
        for existing_file in os.listdir(directory):
            file_path = os.path.join(directory, existing_file)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                else:
                    # remove folder, except target_dirs folders
                    if file_path not in target_dirs:
                        shutil.rmtree(file_path)
            except Exception as e:
                print(e)
    return
