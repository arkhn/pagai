from flask import jsonify

import glob
import re
import json
import yaml


def fhir_resource_path(fhir_resource, parent_folder):
    """
    Search for a file within a given folder
    """
    # this is highly inefficient, since we need to scan the entire folder.
    # It might be interesting to cache the folder structure

    # glob is case insensitive
    pattern = r'.*/{}(?:\.yml|\.json)?'.format(fhir_resource)
    matching_files = [
        file
        for file in glob.glob(f'../{parent_folder}/**', recursive=True)
        if re.match(pattern, file, flags=re.IGNORECASE)
    ]
    if not matching_files:
        return None
    else:
        return matching_files[0]


def file_response(content, extension):
    """
    Return the data corresponding to a filename, according to its format
    """
    if extension == 'json':
        return jsonify(json.loads(content))
    elif extension == 'yml':
        return jsonify(yaml.load(content))

    return jsonify({
        'message': 'Unknown extension.'
    })
