from query_tcga import api
from query_tcga import query_tcga as qt
from query_tcga import config
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG)

TEST_SAMPLE_FILE_ID='0001801b-54b0-4551-8d7a-d66fb59429bf'
TEST_PROJECT='TCGA-BLCA'
TEST_CLIN_FILE_ID='6082fbb4-1f13-4e58-a0fb-33574354b74b'

TEST_DATA_DIR='test/test_data'
config.set_value(
     GDC_DATA_DIR=TEST_DATA_DIR,
     GDC_TOKEN_PATH='/Users/jacquelineburos/Downloads/gdc-user-token.2016-09-26T12-23-27-04-00.txt'
     )


def test_get_data_sample_file():
    res = api.get_data(endpoint_name='files', query_args={'files.file_id': TEST_SAMPLE_FILE_ID})
    assert len(res.json()['data']['hits']) == 1


def test_get_data_sample_file_with_fields():
    res = api.get_data(endpoint_name='files',
                        query_args={'files.file_id': TEST_SAMPLE_FILE_ID},
                        fields=config.get_setting_value('DEFAULT_FILE_FIELDS'),
                        )
    assert len(res.json()['data']['hits']) == 1


def test_get_data_clin_file_with_fields():
    res = api.get_data(endpoint_name='files',
                        query_args={'files.file_id': TEST_CLIN_FILE_ID},
                        fields=config.get_setting_value('DEFAULT_FILE_FIELDS'),
                        )
    assert len(res.json()['data']['hits']) == 1


def test_get_fileinfo_data_sample_file():
    res = api.get_fileinfo_data(file_id = TEST_SAMPLE_FILE_ID)
    assert isinstance(res, pd.DataFrame)
    assert len(res.index)==1


def test_get_fileinfo_data_clin_file():
    res = api.get_fileinfo_data(file_id = TEST_CLIN_FILE_ID)
    assert isinstance(res, pd.DataFrame)
    assert len(res.index)==1


def test_get_fileinfo_data_multiple_files():
    res = api.get_fileinfo_data(file_id = [TEST_SAMPLE_FILE_ID, TEST_CLIN_FILE_ID])
    assert isinstance(res, pd.DataFrame)
    assert len(res.index)==2

def test_get_fileinfo_data_file_details():
    clin = qt.get_clinical_data(project_name=TEST_PROJECT, data_dir=TEST_DATA_DIR, n=1)
    res = api.get_fileinfo_data(file_id = clin['_source_file_uuid'][0])
    assert isinstance(res, pd.DataFrame)
    assert len(res.index)==1

    
