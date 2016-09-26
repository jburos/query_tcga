from __future__ import absolute_import
from .log_with import log_with
import pandas as pd
import os 

@log_with()
def compute_start_given_page(page, size):
    """ compute start / from position given page & size
    """
    return (page*size+1)

@log_with()
def convert_to_list(x):
    """ Convert x to a list if not already a list

    Examples
    -----------

    >>> _convert_to_list('Clinical')
    ['Clinical']
    >>> _convert_to_list(['Clinical'])
    ['Clinical']
    >>> _convert_to_list(('Clinical','Biospecimen'))
    ['Clinical', 'Biospecimen']

    """
    if isinstance(x, pd.Series):
        return(list(x))
    elif not(x):
        return(None)
    elif isinstance(x, list):
        return(x)
    elif isinstance(x, str):
        return([x])
    else:
        return(list(x))

@log_with()
def convert_to_file_id(file_paths):
    ## merge in file_source to get meta-data for the file
    return [os.path.split(os.path.dirname(f))[1] for f in convert_to_list(file_paths)]
