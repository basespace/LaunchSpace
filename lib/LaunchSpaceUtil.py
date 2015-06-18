import json

# this thing will raise some kind of exception if the json file is wrong
# an IOException if the file is missing
# a ValueError if it's invalid json
def validate_json_file(json_file):
    with open(json_file) as fh:
        json_text = fh.read()
        json.loads(json_text)
    return json_text

