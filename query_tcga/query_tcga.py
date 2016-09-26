from __future__ import absolute_import, unicode_literals
import os
import subprocess
import pandas as pd
import io
import numpy as np
import tempfile
import bs4
import logging
import sys

py27_version = (2,7)
cur_version = sys.version_info

if cur_version <= py27_version:
    sys.path.append('~/projects/tcga-blca/query_tcga')
    from log_with import log_with
    import defaults 
    from defaults import GDC_API_ENDPOINT
    import parameters as _params
    import cache
    from cache import requests_get
    import helpers # import _compute_start_given_page, _convert
    import api
    from super_list import L
else:
    from .log_with import log_with
    from . import defaults 
    from .defaults import GDC_API_ENDPOINT
    from . import parameters as _params
    from . import cache
    from .cache import requests_get
    from . import helpers # import _compute_start_given_page, _convert
    from . import api
    from .super_list import L

## cache recquets depending on value of 
cache.setup_cache()
## -- DO -- :
## 1. generate manifest / list of files to download
## 2. use gdc-client to download files to cwd
## 3. verify downloaded files
## 4. transform files to format needed by Cohorts (not done)




@log_with()
def _get_num_pages(project_name, endpoint_name, size=defaults.DEFAULT_SIZE,
                 n=None, data_category=None, query_args={}, verify=False):
    """ Get total number of pages for given criteria

    >>> _get_num_pages('TCGA-BLCA', data_category=['Clinical'], size=5)
    83

    """
    if n and size >= n:
        return 1
    elif n and size:
        return np.floor(n / size)+1
    else:
        endpoint = GDC_API_ENDPOINT.format(endpoint='files')
        params = _params.construct_parameters(project_name=project_name,
                                               endpoint_name=endpoint_name,
                                               size=size,
                                               data_category=data_category,
                                               query_args=query_args,
                                               verify=verify,
                                               )
        response = requests_get(endpoint, params=params)
        response.raise_for_status()
        pages = response.json()['data']['pagination']['pages']
    return pages


@log_with()
def _get_manifest_once(project_name, size=defaults.DEFAULT_SIZE, page=0,
                       data_category=None, query_args={}, verify=False):
    """ Single get for manifest of files matching project_name & categories

    >>> _get_manifest_once('TCGA-BLCA', query_args=dict(data_category=['Clinical']), size=5)
    <Response [200]>
    """
    endpoint = GDC_API_ENDPOINT.format(endpoint='files')
    from_param = helpers.compute_start_given_page(page=page, size=size)
    params = _params.construct_parameters(project_name=project_name,
                                       size=size,
                                       data_category=data_category,
                                       query_args=query_args,
                                       verify=verify,
                                       **{'return_type': 'manifest',
                                       'from': from_param,  ## wrapper to avoid reserved word
                                       'sort': 'file_name:asc'})
    # requests URL-encodes automatically
    response = requests_get(endpoint, params=params)
    response.raise_for_status()
    return response


@log_with()
def get_manifest(project_name=None, n=None, data_category=None, query_args={}, verify=False,
                 size=defaults.DEFAULT_SIZE, pages=None):
    """ Get manifest containing files to be downloaded. 

        By default returns a manifest for all files, up to n files. Otherwise users 
        can filter by combinations of project_name, data_category, and/or query_args.

    >>> get_manifest(project_name='TCGA-BLCA', query_args=dict(data_category=['Clinical']), pages=2, size=2)
    'id\tfilename\tmd5\tsize\tstate\n...'
    """
    output = io.StringIO()
    ## manifest doesn't have 'pagination' json, so iterate through result manually
    ## determine number of pages
    if not(pages):
        pages = _get_num_pages(project_name=project_name, endpoint_name='files',
                               data_category=data_category, n=n,
                               size=size, query_args=query_args, verify=verify)
    if n and pages == 1:
        size = n+1

    ## loop through number of pages
    for page in np.arange(pages):
        response = _get_manifest_once(project_name=project_name,
                                     data_category=data_category,
                                     page=page,
                                     size=size,
                                     query_args=query_args,
                                     verify=verify)
        response_text = response.text.splitlines()
        if page>0:
            del response_text[0]
            output.write('\n')
        output.write('\n'.join(response_text))

    ## truncate to n results
    manifest = output.getvalue()
    if n:
        manifest = output.getvalue().splitlines()
        manifest = '\n'.join(manifest[0:n+1])
    return manifest


@log_with()
def get_manifest_data(*args, **kwargs):
    """ Get manifest containing files to be downloaded, as a Pandas DataFrame.
        See `get_manifest` for more details.
    """
    manifest_contents = get_manifest(*args, **kwargs)
    if manifest_contents != '':
        return pd.read_csv(io.StringIO(manifest_contents), sep='\t')
    else:
        return None


@log_with()
def download_manifest(data_dir=defaults.GDC_DATA_DIR, filename='manifest.txt', only_updates=False, *args, **kwargs):
    """ Get manifest containing files to be downloaded, and write to disk.
        File will be written to GDC_DATA_DIR by default.
        See `get_manifest` for more details.
    """
    manifest = get_manifest(*args, **kwargs)
    manifest_file = open(os.path.join(data_dir, filename), 'rw')
    return _write_manifest_to_disk(manifest_contents=manifest,
                                   data_dir=data_dir,
                                   manifest_file=manifest_file,
                                   only_updates=only_updates)


#### ---- download files ----

@log_with()
def _mkdir_if_not_exists(dir):
    if not(os.path.exists(dir)):
        sub_dir = ''
        for dir_name in os.path.split(dir):
            sub_dir = os.path.join(sub_dir, dir_name)
            if not(os.path.exists(sub_dir)):
                os.mkdir(sub_dir)


def _filter_manifest_updates(manifest_contents, data_dir, only_updates=True):
    """ Filter manifest contents, keeping only files
        that have not been downloaded
    """ 
    _mkdir_if_not_exists(data_dir)
    if not(only_updates):
        return(manifest_contents)
    logging.debug('manifest_contents: {}'.format(manifest_contents))
    # identify files that have not yet downloaded
    failed_downloads = _list_failed_downloads(manifest_contents=manifest_contents,
                                              data_dir=data_dir)
    failed_files = [os.path.basename(f) for f in failed_downloads]
    # filter failed files from manifest
    manifest_contents = [row 
                    for row in manifest_contents.splitlines() 
                    if row.split('\t')[1] in failed_files 
                    or row.split('\t')[0] == 'id']
    manifest_contents = '\n'.join(manifest_contents)
    return manifest_contents


def _write_manifest_to_disk(manifest_contents, manifest_file):
    """ Write manifest content to disk, filtering for new updates by default
    """
    manifest_file.write(manifest_contents.encode())
    return True
    #logging.info('Manifest file written to: {}'.format(str(manifest_file)))


def _truncate_manifest_contents(manifest_contents, n):
    ## TODO truncate manifest contents
    return manifest_contents

@log_with()
def download_from_manifest(manifest_file=None, manifest_contents=None,
                            n=None,
                            data_dir=defaults.GDC_DATA_DIR,
                            only_updates=True,
                            size=defaults.DEFAULT_SIZE,
                            pages=None):

    ## prep manifest contents per params
    if manifest_file:
        manifest_contents = _read_manifest(manifest_file)
    elif isinstance(manifest_contents, pd.DataFrame):
        raise ValueError('Downloading from manifest-data isn\'t currently supported.'+
                    'Please pass in a manifest file or content')
    elif not manifest_contents:
        raise ValueError('Either manifest_file or manifest_content is required.')

    if n:
        manifest_contents = _truncate_manifest_contents(manifest_contents=manifest_contents, n=n)

    all_manifest_contents = manifest_contents
    if only_updates:
        manifest_contents = _filter_manifest_updates(manifest_contents, data_dir=data_dir)

    ## prepare to write manifest data to file
    ## and execute gdc-client
    manifest_file = tempfile.NamedTemporaryFile()
    try:
        # write manifest contents to disk
        _write_manifest_to_disk(manifest_contents=manifest_contents,
                                manifest_file=manifest_file)
        manifest_file.flush()
        # call gdc-client to download contents
        # {gdc_client} download -m {manifest_file} -t {auth_token}
        exe_bash = [defaults.GDC_CLIENT_PATH, 'download', '-m', manifest_file.name, '-t', defaults.GDC_TOKEN_PATH]
        if subprocess.check_call(exe_bash, cwd=data_dir):
            subprocess.call(exe_bash, cwd=data_dir)
        # verify that all files in original manifest have been downloaded
        downloaded = _verify_download(manifest_contents=all_manifest_contents, data_dir=data_dir)
    finally:
        manifest_file.close()
    return downloaded


@log_with()
def download_files(project_name, data_category, n=None, 
                   data_dir=defaults.GDC_DATA_DIR, query_args={},
                   only_updates=True, verify=False,
                   size=defaults.DEFAULT_SIZE,
                   pages=None):
    """ Download files for this project to the current working directory
        1. Query API to get manifest file containing all files matching criteria
        2. Use gdc-client to download files to current working directory
        3. Verify that files downloaded as expected

    Parameters
    --------------
      verify (boolean, optional): if True, verify each name-value pair in the query_args dict

    >>> download_files(project_name='TCGA-BLCA', data_category='Clinical', n=5)
    100% [##############################] Time: 0:00:00
    ...
    100% [#################] Time: 0:00:00 394.49 kB/s

    """
    # get all manifest data
    manifest_contents = get_manifest(project_name=project_name,
                                    data_category=data_category,
                                    n=n,
                                    size=size,
                                    pages=pages,
                                    query_args=query_args,
                                    verify=verify)
    if manifest_contents.strip() == '':
        raise ValueError('No files to download')

    ## filter manifest contents to those which are updates
    new_manifest_contents = _filter_manifest_updates(manifest_contents=manifest_contents,
                                                     data_dir=data_dir,
                                                     only_updates=only_updates)
    ## exit if no files need to be updated
    if new_manifest_contents.strip() == '' or len(new_manifest_contents)==0:
        return _verify_download(manifest_contents=manifest_contents, data_dir=data_dir)

    ## prepare to write manifest data to file
    ## and execute gdc-client
    manifest_file = tempfile.NamedTemporaryFile()
    try:
        # write manifest contents to disk
        _write_manifest_to_disk(manifest_contents=new_manifest_contents,
                                manifest_file=manifest_file)
        manifest_file.flush()
        # call gdc-client to download contents
        # {gdc_client} download -m {manifest_file} -t {auth_token}
        exe_bash = [defaults.GDC_CLIENT_PATH, 'download', '-m', manifest_file.name, '-t', defaults.GDC_TOKEN_PATH]
        if subprocess.check_call(exe_bash, cwd=data_dir):
            subprocess.call(exe_bash, cwd=data_dir)
        # verify that all files in original manifest have been downloaded
        downloaded = L(_verify_download(manifest_contents=manifest_contents, data_dir=data_dir))
    finally:
        manifest_file.close()
    fileinfo = api.get_fileinfo_data(file_id=helpers.convert_to_file_id(downloaded))
    downloaded.fileinfo = fileinfo ## set attribute on returned list
    return downloaded


@log_with()
def download_clinical_files(project_name, n=None, data_dir=defaults.GDC_DATA_DIR, **kwargs):
    """ Download clinical files for this project to the data_dir
        1. Query API to get manifest file containing all files matching criteria
        2. Use gdc-client to download files to current working directory
        3. Verify that files downloaded as expected

    Parameters
    -----------
      project_name (string, required): Name of project, ie 'TCGA-BLCA', 'TCGA-BRCA', etc
      n (int, optional): number of files to download (default: None - downloads all)
      data_dir (string, optional): directory in which to save downloaded files. defaults to 'data/gdc'
      query_args (dict, optional): fields to use when filtering result (other than project & data_category)

    Other parameters (mostly useful for testing)
    -----------
      verify (boolean, optional): if True, verify each name-value pair in the query_args dict
      size (int, optional): how many records to list per page (default 50)
      pages (int, optional): how many pages of records to download (default: all, by specifying value of None)

    """
    ## update kwargs with parameters listed explicitly above (for readability)
    return download_files(project_name=project_name, data_category=['Clinical'], n=n, data_dir=data_dir, **kwargs)

#### ---- verify downloaded files ----

@log_with()
def _read_manifest_data(manifest_file):
    """ Read file contents into pandas dataframe
    """
    manifest_data = pd.read_table(manifest_file, sep='\t')
    return manifest_data


@log_with()
def _verify_download_single_file(row, data_dir):
    """ Verify that the file indicated in the manifest exists in data_dir
    """
    file_name = os.path.join(data_dir, row['id'], row['filename'])
    if not(os.path.exists(file_name)):
        logging.warn('Some files were not found following download: {}'.format(file_name))
        return file_name, False
    else:
        return file_name, True


@log_with()
def _read_manifest(manifest_file=None, manifest_contents=None):
    """ Read in a variety of inputs of manifest (string, pd.DataFrame or file).
        Return a pandas.DataFrame
    """ 
    if manifest_file and not(manifest_contents):
        manifest_data = _read_manifest_data(manifest_file)
    elif not(isinstance(manifest_contents, pd.DataFrame)):
        manifest_data = pd.read_csv(io.StringIO(manifest_contents), sep='\t')
    elif isinstance(manifest_contents, pd.DataFrame):
        manifest_data = manifest_contents
    else:
        raise ValueError('We received neither a manifest_file (path) nor manifest_data (content).')
    return manifest_data



@log_with()
def _characterize_downloads(data_dir, manifest_file=None, manifest_contents=None):
    manifest_data = _read_manifest(manifest_file=manifest_file, manifest_contents=manifest_contents)
    failed_downloads = list()
    downloads = list()
    for i, row in manifest_data.iterrows():
        file_name, success = _verify_download_single_file(row=row, data_dir=data_dir)
        if success:
            downloads.append(file_name)
        else:
            failed_downloads.append(file_name)
    return {'failed': failed_downloads, 'success': downloads}


@log_with()
def _list_failed_downloads(data_dir, manifest_file=None, manifest_contents=None):
    """
    """
    res = _characterize_downloads(manifest_file=manifest_file,
                                  manifest_contents=manifest_contents,
                                  data_dir=data_dir)
    return res['failed']


@log_with()
def _verify_download(data_dir, manifest_file=None, manifest_contents=None):
    """
    """
    res = _characterize_downloads(manifest_file=manifest_file,
                                  manifest_contents=manifest_contents,
                                  data_dir=data_dir)
    return res['success']


#### ---- transform downloaded files to Cohorts-friendly format ----

@log_with()
def _read_xml_bs(xml_file_path):
    with open(xml_file_path) as fd:
        soup = bs4.BeautifulSoup(fd.read(), 'xml')
    return soup


@log_with()
def _parse_clin_data_from_tag(tag, name_prefix=None, preferred_only=True):
    data = dict()

    if not(isinstance(tag, bs4.element.Tag)):
        return data

    if tag.is_empty_element:
        return data

    ## get field_name for tag data
    if tag.has_attr('preferred_name'):
        field_name = tag.get('preferred_name')
    elif not(preferred_only):
        field_name = tag.name
    elif name_prefix and not(preferred_only):
        field_name = '-'.join([name_prefix, field_name])
    else:
        field_name = None

    ## extract text from this tag, if it exists
    if tag.text:
        field_value = tag.text.strip()
    else:
        field_value = None

    ## update data with this tag's name & value
    ## only capture data if field_name & field_value defined
    if field_name and field_value and field_value != '':
        data[field_name] = field_value

    ## if tag has children, process those
    if len(tag)>1:
        for sub_tag in tag:
            sub_tag_data = _parse_clin_data_from_tag(sub_tag, name_prefix=field_name)
            data.update(sub_tag_data)

    return data


@log_with()
def _parse_clin_data_soup(soup, **kwargs):
    patient_node = soup.findChild('patient')
    data = dict()
    for tag in patient_node:
        if isinstance(tag, bs4.element.Tag):
            tag_data = _parse_clin_data_from_tag(tag, **kwargs)
            data.update(tag_data)
    return data


@log_with()
def get_clinical_data_from_file(xml_file, fileinfo=None, **kwargs):
    soup = _read_xml_bs(xml_file)
    data = _parse_clin_data_soup(soup, **kwargs)
    file_id = helpers.convert_to_file_id(xml_file)
    data['_source_type'] = 'XML'
    data['_source_desc'] = xml_file
    data['patient_id'] = soup.findChild('patient_id').text
    data['_source_file_uuid'] = file_id
    ## get file meta-data (for case_id):
    if fileinfo is not None:
        data['case_id'] = fileinfo.loc[fileinfo['file_id']==file_id[0], 'case_id'].values[0]
    else:
        data['case_id'] = api.get_fileinfo_data(file_id=file_id)['case_id'][0]
    return data


def _convert_to_categorical(series, max_groups=5):
    """ Convert the series to categorical, if number of distinct groups <= `max_groups`
    """
    if type(series) is not chr:
        return series
    if len(series.value_counts(dropna=True)) <= max_groups:
        return series.astype('category')
    else:
        return series


@log_with()
def get_clinical_data(project_name, **kwargs):
    xml_files = download_clinical_files(project_name=project_name, **kwargs)
    data = list()
    for xml_file in xml_files:
        data.append(get_clinical_data_from_file(xml_file, fileinfo=xml_files.fileinfo))
    df = pd.DataFrame(data)
    ## convert numerical fields to numeric
    df = df.apply(pd.to_numeric, errors='ignore')
    ## convert rest of fields to categories, if n_groups <= 5
    df = df.apply(_convert_to_categorical)
    return df

