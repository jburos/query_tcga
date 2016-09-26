from __future__ import absolute_import
import json
from .log_with import log_with
from query_tcga import defaults 
from query_tcga.defaults import GDC_API_ENDPOINT
from query_tcga import error_handling as _errors
from query_tcga.cache import requests_get
from query_tcga import helpers # import _convert_to_list

#### ---- tools for constructing parameters ---- 
@log_with()
def _construct_filter_element(field, value, op='in', verify=False):
    """ construct dict element representing an atomic unit of a filter.
        if endpoint_name is None, assume endpoint is given as part of the field
            (and parse out endpoint_name for field checking)
    """
    field_name = field
    endpoint_name = field.split('.')[0]
    field_value = helpers.convert_to_list(value)
    if verify:
        _verify_field_values(data_list=field_value, field_name=field_name, endpoint_name=endpoint_name)
    filt = {"op": op,
                "content": {
                    "field": field_name,
                    "value": field_value
                }
        }
    return filt

@log_with()
def _construct_filter_parameters(project_name=None, data_category=None, query_args={}, verify=False):
    """ construct filter-json given project name & files requested

    Examples
    -----------

    >>> _construct_filter_parameters(project_name='TCGA-BLCA', data_category='Clinical')
    {'content': [
        {'content': {'field': 'cases.project.project_id', 'value': ['TCGA-BLCA']}, 'op': 'in'},
        {'content': {'field': 'files.data_category', 'value': ['Clinical']}, 'op': 'in'}
        ],
        'op': 'and'}

    """
    content_filters = list()
    if project_name:
        filt_project = _construct_filter_element(field='cases.project.project_id',
                                                 value=project_name, verify=verify
                                                 )
        content_filters.append(filt_project)
    if data_category:
        filt_category = _construct_filter_element(field='files.data_category',
                                                 value=data_category, verify=verify
                                                 )
        content_filters.append(filt_category)
    if query_args:
        if 'data_category' in query_args.keys() or 'files.data_category' in query_args.keys():
            raise ValueError('Flexible filtering by data_category not yet implemented.')
        for field in query_args:
            next_filter = _construct_filter_element(field=field,
                                                     value=query_args[field]
                                                     )
            content_filters.append(next_filter)
    filt = {"op": "and",
            "content": content_filters
            }
    return filt


@log_with()
def construct_parameters(project_name=None, data_category=None, query_args={}, verify=False, **kwargs):
    """ Construct query parameters given project name & list of data categories

    >>> _construct_parameters(project_name='TCGA-BLCA', endpoint_name='files', data_category='Clinical', size=5)
    {'filters':
        '{"content": [{"content": {"value": ["TCGA-BLCA"], "field": "cases.project.project_id"}, "op": "in"}, {"content": {"value": ["Clinical"], "field": "files.data_category"}, "op": "in"}], "op": "and"}',
     'size': 5}
    """
    params = {}
    if any([project_name, data_category, query_args]):
        filt = _construct_filter_parameters(project_name=project_name,
                                            data_category=data_category,
                                            query_args=query_args,
                                            verify=verify)
        params.update({'filters': json.dumps(filt)})

    if dict(**kwargs):
        [params.update({k: v}) for (k, v) in kwargs.items()]

    return params

#### ---- tools for field validation ----

@log_with()
def _list_valid_fields(endpoint_name):
    """ List allowable fields for this endpoint

    >>> res = _list_valid_fields(endpoint_name='files')
    >>> len(res)
    221
    >>> res.sort()
    >>> res[0:3]
    ['files.access', 'files.acl', 'files.analysis.analysis_id']

    """
    _verify_data_list(data_list=[endpoint_name], allowed_values=defaults.VALID_ENDPOINTS)
    endpoint = GDC_API_ENDPOINT.format(endpoint=endpoint_name)+'/_mapping'
    response = requests_get(endpoint)
    response.raise_for_status()
    field_names = response.json()['_mapping'].keys()
    return field_names


@log_with()
def _list_valid_options(field_name,
                        endpoint_name,
                        project_name=None,
                        strip_endpoint_from_field_name=True):
    """ List valid options (values) for a field.

    Note that field names are listed without prefix (as 'data_category') when given as a facet. This function
      masks that complexity by stripping out the endpoint from the field name by default. (the default behavior
      can be turned off using parameter `strip_endpoint_from_field_name=False`)

    >>> _list_valid_options('data_category', endpoint_name='files')
    ['Simple Nucleotide Variation',
     ...
     'Clinical']

    >>> _list_valid_options('files.data_category', endpoint_name='files')
    ['Simple Nucleotide Variation',
      ...
      'Clinical']

    >>> _list_valid_options('files.data_category', endpoint_name='files', strip_endpoint_from_field_name=False)
    ValueError: ... 'warnings': {'facets': 'unrecognized values: [files.data_category]'}}


    """
    # according to https://gdc-docs.nci.nih.gov/API/Users_Guide/Search_and_Retrieval/#filters-specifying-the-query
    # this is the best way to query the endpoint for values
    endpoint = GDC_API_ENDPOINT.format(endpoint=endpoint_name)
    if strip_endpoint_from_field_name:
        field_name = field_name.replace('{}.'.format(endpoint_name), '')
    params = construct_parameters(project_name=project_name, facets=field_name, size=0)
    response = requests_get(endpoint, params=params)
    response.raise_for_status()
    try:
        items = [item['key'] for item in response.json()['data']['aggregations'][field_name]['buckets']]
    except:
        _errors.raise_error_parsing_result(response)
    return items


@log_with()
def _verify_field_name(field_name, endpoint_name):
    """ Verify that field exists for this endpoint

    >>> _verify_field_name(field_name='files.data_category', endpoint_name='files')
    True

    >>> _verify_field_name(field_name='data_category', endpoint_name='files')
    ValueError: Field given was not valid: data_category.
     Some close matches:
            files.data_category
            ...
            files.downstream_analyses.output_files.data_category
    """
    try:
        found = _verify_data_list(field_name, allowed_values=_list_valid_fields(endpoint_name=endpoint_name))
    except ValueError:
        possible_matches = _search_for_field(field_name, endpoint_name=endpoint_name)
        raise ValueError('Field given was not valid: {given}. \n Some close matches: \n\t{matches}'.format(given=field_name,
             matches='\n\t'.join(possible_matches)))
    return found


@log_with()
def _search_for_field(search_string, endpoint_name):
    fields = _list_valid_fields(endpoint_name=endpoint_name)
    return [field for field in fields if field.find(search_string)>0]


@log_with()
def _verify_data_list(data_list, allowed_values, message='At least one value given was invalid'):
    """ Verify that each element in a given list is among the allowed_values.

    >>> _verify_data_list(['TCGA-BLCA'], allowed_values=['Clinical'])
    ValueError: At least one value given was invalid: TCGA-BLCA
    >>> _verify_data_list(['Clinical'], allowed_values=['Clinical', 'Biospecimen'])
    True
    >>> _verify_data_list(['Clinical'], allowed_values=_list_valid_options('data_category'))
    True
    """
    data_list = helpers.convert_to_list(data_list)
    if not(all(el in allowed_values for el in data_list)):
        ## identify invalid categories for informative error message
        bad_values = list()
        [bad_values.append(el) for el in data_list if not(el in allowed_values)]
        raise ValueError('{message}: {bad_values}'.format(bad_values=', '.join(bad_values), message=message))
    return True

@log_with()
def _verify_field_values(data_list, field_name, endpoint_name, project_name=None):
    """ Verify that each element in a given list is among the allowed_values
        for that field/endpoint (& optionally for that project).

    >>> _verify_field_values(['Clinical'], field_name='files.data_category', endpoint_name='files')
    True

    >>> _verify_field_values(['Clinical'], field_name='data_category', endpoint_name='files')
    ValueError: Field given was not valid: data_category.
     Some close matches:
            files.data_category
            ...
            files.downstream_analyses.output_files.data_category
    """
    _verify_field_name(field_name=field_name, endpoint_name=endpoint_name)
    valid_options = _list_valid_options(field_name=field_name, endpoint_name=endpoint_name, project_name=project_name)
    return _verify_data_list(data_list=data_list, allowed_values=valid_options)



