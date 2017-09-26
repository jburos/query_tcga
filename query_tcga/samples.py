from __future__ import absolute_import
from . import query_tcga as qt
import varcode
import pandas as pd
from . import helpers, api
import os

#### ---- download other files ----

@qt.log_with()
def download_wxs_files(project_name, query_args={}, dry_run=False, experimental_strategy=["WXS", "RNA-Seq"], **kwargs):
    """ Download sequencing files for this project to the current working directory
        1. Query API to get manifest file containing all files matching criteria
        2. Use gdc-client to download files to current working directory
        3. Verify that files downloaded as expected

    Parameters
    -----------
      project_name (string, required): Name of project, ie 'TCGA-BLCA', 'TCGA-BRCA', etc
      data_dir (string, optional): directory in which to save downloaded files. defaults to 'data/gdc'
      query_args (dict, optional): other filters to apply (other than experimental_strategy & project)
      experimental_strategy (list, optional): filter by experimental stragery. 
        Examples include ["WXS", "RNA-Seq", "Genotyping Array", "miRNA-Seq"]

    Other parameters (mostly useful for testing)
    -----------
      verify (boolean, optional): if True, verify each name-value pair in the query_args dict
      page_size (int, optional): how many records to list per page (default 50)
      max_pages (int, optional): how many pages of records to download (default: all, by specifying value of None)

    """
    if experimental_strategy:
        query_args.update(dict(experimental_strategy=experimental_strategy))
    if dry_run:
        files = qt.get_manifest_data(project_name=project_name, data_category=['Raw Sequencing Data'],
                 query_args=query_args, **kwargs)
    else:
        files = qt.download_files(project_name=project_name, data_category=['Raw Sequencing Data'],
                 query_args=query_args, **kwargs)
        files = _summarize_wxs_files(files)
    return files

@qt.log_with()
def _summarize_wxs_files(bam_files):
    bam_file_summary = bam_files.fileinfo
    # check if these fileinfo data have already been augmented
    if 'filepath' in bam_file_summary.columns:
        return bam_files
    # get sample info (ie sample_type, etc) for each file
    bam_sample_info = pd.DataFrame([a[0] for a in bam_file_summary.samples.tolist()])
    bam_sample_info.reset_index(inplace=True)
    bam_file_summary.reset_index(inplace=True)
    bam_file_summary = pd.concat([bam_file_summary, bam_sample_info], axis=1)
    bam_file_summary.drop(['index', 'index.1'], axis=1, inplace=True)
    bam_file_summary.rename(columns = {'submitter_id.1': 'sample_submitter_id'}, inplace=True)
    # create field `filepath` containing path to file downloaded
    bam_file_paths = pd.DataFrame(data=dict(filepath = bam_files))
    bam_file_paths['file_name'] = bam_file_paths.filepath.apply(os.path.basename)
    bam_file_summary = pd.merge(bam_file_summary, bam_file_paths, on='file_name', how='right')
    bam_files.fileinfo = bam_file_summary
    return bam_files

@qt.log_with()
def download_vcf_files(project_name, data_format='VCF', workflow_type=None, data_type=['Raw Simple Somatic Mutation', 'Annotated Somatic Mutation'],
                       query_args={}, dry_run=False, **kwargs):
    """ Download VCF files for this project to the DATA_DIR directory
        1. Query API to get manifest file containing all files matching criteria
        2. Use gdc-client to download files to current working directory
        3. Verify that files downloaded as expected

    Parameters
    -----------
      project_name (string, required): Name of project, ie 'TCGA-BLCA', 'TCGA-BRCA', etc
      data_dir (string, optional): directory in which to save downloaded files. defaults to 'data/gdc'
      query_args (dict, optional): other filters to apply (e.g. experimental_strategy=["WXS", "RNA-Seq", "Genotyping Array", "miRNA-Seq"])

    Other parameters (mostly useful for testing)
    -----------
      verify (boolean, optional): if True, verify each name-value pair in the query_args dict
      size (int, optional): how many records to list per page (default 50)
      pages (int, optional): how many pages of records to download (default: all, by specifying value of None)

    """
    if data_format:
        query_args.update({'files.data_format': data_format})
    if data_type:
        query_args.update({'files.data_type': data_type})
    if workflow_type:
        query_args.update({'files.analysis.workflow_type': workflow_type})
    if (dry_run):
        files = qt.get_manifest_data(
             project_name=project_name,
             data_category=['Simple Nucleotide Variation'],
             query_args=query_args,
             **kwargs)
    else :
        files = qt.download_files(
             project_name=project_name,
             data_category=['Simple Nucleotide Variation'],
             query_args=query_args,
             **kwargs)
        files.fileinfo = summarize_vcf_files(files)
    return files


def _summarize_single_vcf_file(filepath):
    """ Summarize meta-data from a single VCF file
    """
    try:
        vcf = varcode.vcf.load_vcf(filepath, max_variants=1)
    except ValueError as e:
        reference_name = None
    else:
        reference_name = vcf[0].reference_name
    summary = dict(filepath=filepath, reference_name=reference_name, file_id=helpers.convert_to_file_id(filepath)[0])
    return summary


def summarize_vcf_files(files):
    """ Sumarize meta-data from each of the VCF files listed in `files`.
    """
    dflist = list()
    [dflist.append(_summarize_single_vcf_file(file)) for file in files]
    file_summary = pd.DataFrame(dflist)
    if hasattr(files, 'fileinfo'):
        fileinfo = files.fileinfo
    else:
        fileinfo = api.get_fileinfo_data(files)
    summary = pd.merge(file_summary, fileinfo, on='file_id')
    return summary


def summarize_wxs_files(files):
    if hasattr(files, 'fileinfo'):
        fileinfo = files.fileinfo
    else:
        fileinfo = api.get_fileinfo_data(files)
    return fileinfo

