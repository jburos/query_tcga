from __future__ import absolute_import
import pandas as pd
import logging
from .log_with import log_with
from .config import get_setting_value 
from . import parameters as _params
from .cache import requests_get
from . import helpers

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

#### ---- utilities for interacting with the GDC api ---- 

@log_with()
def get_data(endpoint_name, arg=None,
              project_name=None, fields=None, size=get_setting_value('DEFAULT_SIZE'), page=0,
              data_category=None, query_args={}, verify=False, *args, **kwargs):
    """ Get single result from querying GDC api endpoint

    >>> file = get_data(endpoint='files', data_category='Clinical', query_args=dict(file_id=df['case_uuid'][0]))
    <Response [200]>
    """
    endpoint = get_setting_value('GDC_API_ENDPOINT').format(endpoint=endpoint_name)
    if arg:
        endpoint = endpoint+'/{}'.format(arg)
    else:
        ## prep extra-params, including `from` param, as dict
        extra_params = {}
        if page>0:
            from_param = helpers.compute_start_given_page(page=page, size=size)
            extra_params.update({
              'from': from_param,
              })
        if fields:
            extra_params.update({'fields': ','.join(helpers.convert_to_list(fields))})
        if dict(**kwargs):
            ## TODO check on whether this handles redundant param spec 
            ## correctly
            extra_params.update(dict(**kwargs))
        params = _params.construct_parameters(project_name=project_name,
                                             size=size,
                                             data_category=data_category,
                                             query_args=query_args,
                                             verify=verify,
                                             **extra_params
                                             )

    # requests URL-encodes automatically
    log.info('submitting request for {endpoint} with params {params}'.format(endpoint=endpoint, params=params))
    response = requests_get(endpoint, params=params)
    log.info('url requested was: {}'.format(response.url))
    response.raise_for_status()
    return response


@log_with()
def _get_case_data(arg=None,
              project_name=None, fields=None, size=get_setting_value('DEFAULT_SIZE'), page=1,
              data_category=None, query_args={}, verify=False, **kwargs):
    """ Get single case json matching project_name & categories

    >>> _get_case_data(project_name='TCGA-BLCA', data_category=['Clinical'], size=5)
    <Response [200]>
    """
    return get_data(endpoint='cases', arg=arg, project_name=project_name, fields=fields, size=size,
                    page=page, data_category=data_category, query_args=query_args,
                    verify=verify, **kwargs)


@log_with()
def _get_sample_data():
    return True


@log_with()
def get_fileinfo(file_id, fields=get_setting_value('DEFAULT_FILE_FIELDS'), format=None):
    query_args = {'files.file_id': file_id}
    response = get_data(endpoint_name='files', query_args=query_args, fields=fields, format=format)
    if format == 'json':
        return response.json()['data']['hits']
    else:
        return response


def get_fileinfo_data(file_id,
                      fields=get_setting_value('DEFAULT_FILE_FIELDS'),
                      chunk_size=get_setting_value('DEFAULT_CHUNK_SIZE')
                      ):
    file_id = helpers.convert_to_list(file_id)
    if len(file_id)>chunk_size:
        chunks = [file_id[x:x+chunk_size] for x in range(0, len(file_id), chunk_size)]
        data = [get_fileinfo_data(chunk, fields=fields) for chunk in chunks]
        return pd.concat(data)
    res = get_fileinfo(file_id=file_id, fields=fields, format='json')
    df = list()
    for hit in res:
        hit_data = dict()
        for (k, v) in hit.items():
            if k == 'cases':
                for (subkey, subval) in hit['cases'][0].items():
                  hit_data[subkey] = subval
            elif k == 'analysis':
                for (subkey, subval) in hit['analysis'].items():
                  hit_data[subkey] = subval
            else:
                hit_data[k] = v
        df.append(hit_data)
    df = pd.DataFrame(df)
    return df


@log_with()
def _describe_samples(case_ids,
                      data_category,
                      query_args={},
                      **kwargs):
    """ Helper function to describe sample files
    """
    for case_id in helpers.convert_to_list(case_ids):
        sample_df = list()
        samples = get_data(endpoint='cases',
                               fields='sample_ids',
                               query_args=dict(case_id=case_id, **query_args),
                               **kwargs
                               )
        sample_ids = list()
        [sample_ids.extend(hit['sample_ids']) for hit in samples.json()['data']['hits']]
        sample_data = get_data(endpoint='samples',
                                   query_args=dict(sample_id=sample_ids),
                                   )
        sample_df.append(sample_data)
    return sample_df
