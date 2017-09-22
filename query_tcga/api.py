from __future__ import absolute_import
import pandas as pd
import logging
from .log_with import log_with
from .config import get_setting_value 
from . import parameters as _params
from .cache import requests_get
from . import helpers

log = logging.getLogger(__name__)

#### ---- utilities for interacting with the GDC api ---- 

@log_with()
def get_data(endpoint_name, arg=None,
              project_name=None, fields=None, size=None,
              page=0, data_category=None, query_args={}, verify=False, *args, **kwargs):
    """ Get single result from querying GDC api endpoint

    >>> file = get_data(endpoint='files', data_category='Clinical', query_args=dict(file_id=df['case_uuid'][0]))
    <Response [200]>
    """
    if size is None:
        size = int(get_setting_value('DEFAULT_SIZE'))
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
def get_fileinfo(file_id, fields=None, format=None, chunk_size=None):
    if fields is None:
        fields = get_setting_value('DEFAULT_FILE_FIELDS')
    query_args = {'files.file_id': file_id}
    response = get_data(endpoint_name='files', query_args=query_args, fields=fields, format=format, size=chunk_size)
    if format == 'json':
        return response.json()['data']['hits']
    else:
        return response


def get_fileinfo_data(file_id,
                      fields=None,
                      chunk_size=None
                      ):
    if fields is None:
        fields = get_setting_value('DEFAULT_FILE_FIELDS')
    if chunk_size is None:
        chunk_size = int(get_setting_value('DEFAULT_CHUNK_SIZE'))
    file_id = helpers.convert_to_list(file_id)
    file_id = [x for x in file_id if x != '']
    if len(file_id) == 0:
        # logger.warning('No files left to process')
        return pd.DataFrame()
    if len(file_id)>chunk_size:
        chunks = [file_id[x:x+chunk_size] for x in range(0, len(file_id), chunk_size)]
        data = [get_fileinfo_data(chunk, fields=fields, chunk_size=chunk_size) for chunk in chunks]
        return pd.concat(data)
    res = get_fileinfo(file_id=file_id, fields=fields, format='json', chunk_size=chunk_size)
    assert len(res) == len(file_id), "Not enough results from fileinfo returned"
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
    assert len(df) == len(file_id), "Some fileinfo details not queried"
    df = pd.DataFrame(df)
    return df

@log_with()
def _describe_samples(case_ids,
                      query_args={},

                      **kwargs):
    """ Helper function to describe sample files
    """
    sample_df = list()
    for case_id in helpers.convert_to_list(case_ids):
        samples = get_data(endpoint_name='cases',
                               fields=['files.cases.samples.sample_id',
                                       'files.cases.samples.sample_type',
                                       'files.cases.samples.sample_type_id',
                                       'files.cases.samples.composition',
                                       'files.cases.samples.created_datetime',
'files.cases.samples.current_weight',
'files.cases.samples.days_to_collection',
'files.cases.samples.days_to_sample_procurement',
'files.cases.samples.freezing_method',
'files.cases.samples.initial_weight',
'files.cases.samples.intermediate_dimension',
'files.cases.samples.is_ffpe',
'files.cases.samples.longest_dimension',
'files.cases.samples.oct_embedded',
'files.cases.samples.pathology_report_uuid',
'files.cases.samples.preservation_method',
'files.cases.samples.sample_id',
'files.cases.samples.sample_type',
'files.cases.samples.sample_type_id',
'files.cases.samples.shortest_dimension',
'files.cases.samples.state',
'files.cases.samples.submitter_id',
'files.cases.samples.time_between_clamping_and_freezing',
'files.cases.samples.time_between_excision_and_freezing',
'files.cases.samples.tissue_type',
'files.cases.samples.tumor_code',
'files.cases.samples.tumor_code_id',
'files.cases.samples.tumor_descriptor',
'files.cases.samples.updated_datetime'],
                               query_args=dict(case_id=case_id, **query_args),
                               **kwargs
                               )
        sample_data = _convert_sample_result_to_df(samples.json()['data']['hits'])
        sample_df.append(sample_data)
    return pd.concat(sample_df).drop_duplicates()

def _convert_sample_result_to_df(res):
    sample_df_list = list()
    for hit in res:
        for result_file in hit['files']:
            for result_case in result_file['cases']:
                hit_data = pd.DataFrame(result_case['samples'])
                sample_df_list.append(hit_data)
    return pd.concat(sample_df_list)
            
