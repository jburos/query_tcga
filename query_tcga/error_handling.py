from __future__ import absolute_import
from .log_with import log_with

@log_with()
def raise_error_parsing_result(response):
    try:
        raise ValueError('Error parsing returned object: {}'.format(response.json()['warnings']))
    except:
        raise ValueError('Server responded with: {}'.format(response.json()))
    return None
