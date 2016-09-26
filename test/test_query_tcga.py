
from query_tcga import query_tcga as qt
import pytest
import shutil
import pandas as pd
import requests
from query_tcga import error_handling as errors
from query_tcga.log_with import log_with
import logging


logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

DATA_DIR = 'tests/test_data'

manifest = pytest.mark.NAME

def test_connection():
    requests.get('http://yahoo.com')

def test_get_num_pages():
    num_pages = qt._get_num_pages(project_name='TCGA-BLCA', data_category=['Clinical'], size=5, endpoint_name='files')
    assert isinstance(num_pages, int)


def test_get_num_pages_using_n_lessthan():
    num_pages = qt._get_num_pages(project_name='TCGA-BLCA', data_category=['Clinical'], n=5, size=6, endpoint_name='files')
    assert num_pages == 1


def test_get_num_pages_using_n_equal():
    num_pages = qt._get_num_pages(project_name='TCGA-BLCA', data_category=['Clinical'], n=5, size=5, endpoint_name='files')
    assert num_pages == 1


def test_get_num_pages_using_n_greaterthan():
    num_pages = qt._get_num_pages(project_name='TCGA-BLCA', data_category=['Clinical'], n=6, size=5, endpoint_name='files')
    assert num_pages == 2


def test_get_num_pages_null_n():
    num_pages = qt._get_num_pages(project_name='TCGA-BLCA', data_category=['Clinical'], size=5, endpoint_name='files')
    assert num_pages > 20


def test_get_manifest_once():
    response = qt._get_manifest_once(project_name='TCGA-BLCA', data_category=['Clinical'], size=5)
    assert response.status_code == 200


def test_get_manifest_using_pages():
    res = qt.get_manifest(project_name='TCGA-BLCA', data_category=['Clinical'], pages=2, size=2)
    assert len(res.splitlines()) == 5 ## 4 records + header
    assert res.splitlines()[0] == 'id\tfilename\tmd5\tsize\tstate'


def test_get_manifest_using_n():
    res = qt.get_manifest(project_name='TCGA-BLCA', data_category=['Clinical'], n=4)
    assert len(res.splitlines()) == 5 ## 4 records + header
    assert res.splitlines()[0] == 'id\tfilename\tmd5\tsize\tstate'


## TODO fix/use tempdir setup
## doesn't work now b/c doesn't have a path
# http://doc.pytest.org/en/latest/_modules/_pytest/tmpdir.html
@log_with()
@manifest
def test_download_files_using_page():
    _rmdir_if_exists(DATA_DIR)
    res = qt.download_files(project_name='TCGA-BLCA', data_category='Clinical',
         pages=1, size=5, data_dir=DATA_DIR)
    assert isinstance(res, list)
    assert len(res) == 5


@manifest
def test_download_files_using_n():
    res = qt.download_files(project_name='TCGA-BLCA', data_category='Clinical',
         n=5, data_dir=DATA_DIR)
    assert isinstance(res, list)
    assert len(res) == 5


def _rmdir_if_exists(*args, **kwargs):
    try:
        shutil.rmtree(*args, **kwargs)
    except errors.FileNotFoundError:
        pass

@manifest
def test_download_clinical_files():
    _rmdir_if_exists(DATA_DIR)
    res = qt.download_clinical_files(project_name='TCGA-BLCA', n=5, data_dir=DATA_DIR)
    assert isinstance(res, list)
    assert len(res) == 5


@manifest
def test_get_clinical_data():
    res = qt.get_clinical_data(project_name='TCGA-BLCA', n=5, data_dir=DATA_DIR)
    assert isinstance(res, pd.DataFrame)
    assert len(res.index) == 5
    assert '_source_type' in res.columns
    assert '_source_desc' in res.columns
    assert 'patient_id' in res.columns


@manifest
def test_list_failed_downloads():
    _rmdir_if_exists(DATA_DIR)
    manifest_contents = qt.get_manifest(project_name='TCGA-BLCA', data_category='Clinical', n=5)
    failed = qt._list_failed_downloads(manifest_contents=manifest_contents, data_dir=DATA_DIR)
    assert len(failed) == 5
    qt.download_clinical_files(project_name='TCGA-BLCA', n=5, data_dir=DATA_DIR)
    new_failed = qt._list_failed_downloads(manifest_contents=manifest_contents, data_dir=DATA_DIR)
    assert isinstance(new_failed, list)
    assert len(new_failed) == 0


def test_download_from_manifest():
    manifest_contents = qt.get_manifest(project_name='TCGA-BLCA', data_category='Clinical', n=5)
    downloaded = qt.download_from_manifest(manifest_contents=manifest_contents, data_dir=DATA_DIR)
    assert isinstance(downloaded, list)
    assert len(manifest_contents.splitlines()) == len(downloaded)+1

