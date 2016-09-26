from query_tcga import parameters
from query_tcga import helpers
import pytest
import shutil
import pandas as pd


def test_construct_filter_parameters():
   res = parameters._construct_filter_parameters(
   			project_name='TCGA-BLCA',
   			data_category='Clinical')
   expects = {'content': [
        {'content': {'field': 'cases.project.project_id', 'value': ['TCGA-BLCA']}, 'op': 'in'},
        {'content': {'field': 'files.data_category', 'value': ['Clinical']}, 'op': 'in'}
        ],
        'op': 'and'}
   assert res == expects


def test_convert_to_list():
    assert helpers.convert_to_list('Clinical') == ['Clinical']
    assert helpers.convert_to_list(['Clinical']) == ['Clinical']
    assert helpers.convert_to_list(('Clinical','Biospecimen')) == ['Clinical', 'Biospecimen']


def test_construct_parameters():
    ## minimal testing because dictionary (which is converted to string) 
    ## isn't always in same order. *could* sort the dict, but prob isn't necessary
    res = parameters.construct_parameters(project_name='TCGA-BLCA', size=5, endpoint_name='files')
    assert list(res.keys()).sort() == ['filters','size'].sort()


def test_construct_parameters_with_args():
    ## minimal testing because dictionary (which is converted to string) 
    ## isn't always in same order. *could* sort the dict, but prob isn't necessary
    res = parameters.construct_parameters(query_args={'file_id': 'XXXX'}, size=5, endpoint_name='files')
    assert list(res.keys()).sort() == ['filters','size'].sort()


def test_list_valid_fields():
    expected = ['files.access', 'files.acl', 'files.analysis.analysis_id']
    res = list(parameters._list_valid_fields(endpoint_name='files'))
    res.sort()
    assert res[0:3] == expected
    assert len(res) >= 200


def test_list_valid_options():
    expected = ['Simple Nucleotide Variation',
     'Transcriptome Profiling',
     'Raw Sequencing Data',
     'Copy Number Variation',
     'Biospecimen',
     'Clinical']
    res = parameters._list_valid_options('data_category', endpoint_name='files')
    assert isinstance(res, list)
    assert set(res) == set(expected)
    assert set(parameters._list_valid_options('files.data_category', endpoint_name='files')) == set(expected)
    # confirm raises an error when strip_endpoint_from_field_name == False
    with pytest.raises(ValueError):
        parameters._list_valid_options('files.data_category', endpoint_name='files', strip_endpoint_from_field_name=False)


def test_verify_field_name():
    assert parameters._verify_field_name(field_name='files.data_category', endpoint_name='files') == True
    with pytest.raises(ValueError):
        parameters._verify_field_name(field_name='data_category', endpoint_name='files')


def test_verify_field_values():
    assert parameters._verify_field_values(['Clinical'],
    									 field_name='files.data_category',
    									 endpoint_name='files') == True  
    with pytest.raises(ValueError):
        parameters._verify_field_values(['Clinical'], field_name='data_category', endpoint_name='files')


def test_verify_data_list():
    assert parameters._verify_data_list(['Clinical'],
                                allowed_values=['Clinical', 'Biospecimen']) == True
    valid_options = parameters._list_valid_options('data_category', endpoint_name='files')
    assert parameters._verify_data_list(['Clinical'],
                                allowed_values=valid_options) == True
    with pytest.raises(ValueError):
        parameters._verify_data_list(['TCGA-BLCA'], allowed_values=['Clinical'])
