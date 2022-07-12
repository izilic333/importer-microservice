import json


def convert_string_to_json(string):
    try:
        return json.loads(string)
    except Exception as ex1:
        return string
