from __future__ import absolute_import
import cohorts
from . import query_tcga as qt
from . import samples
from . import helpers
from .config import get_setting_value
from . import config
import numpy as np
import pandas as pd
import logging
import os

def _get_file_path(project_data_dir, file_type):
    return os.path.join(project_data_dir, '{}.csv'.format(file_type))

def _try_get_file(project_data_dir=None, file_type='generic'):
    if project_data_dir:
        file_path = _get_file_path(project_data_dir=project_data_dir, file_type=file_type)
        if os.path.exists(file_path):
            dataframe = pd.read_csv(file_path, sep='|')
            return dataframe
    return None

def _try_save_file(dataframe, project_data_dir=None, file_type='generic'):
    if project_data_dir:
        file_path = _get_file_path(project_data_dir=project_data_dir, file_type=file_type)
        dataframe.to_csv(file_path, sep='|', index=False)


def _load_clinical_data(project_name, data_dir, project_data_dir=None, **kwargs):
    clinical_data = _try_get_file(project_data_dir, file_type='clinical')
    if clinical_data is None:
        clinical_data = qt.get_clinical_data(project_name=project_name, data_dir=data_dir, **kwargs)
        _try_save_file(clinical_data, project_data_dir=project_data_dir, file_type='clinical')
    return clinical_data


def _load_vcf_fileinfo(project_name, data_dir, project_data_dir=None, **kwargs):
    vcf_fileinfo = _try_get_file(project_data_dir, file_type='vcf_fileinfo')
    if vcf_fileinfo is None:
        vcf_files = samples.download_vcf_files(project_name=project_name, data_dir=data_dir, **kwargs)
        vcf_fileinfo = vcf_files.fileinfo
        _try_save_file(vcf_fileinfo, project_data_dir=project_data_dir, file_type='vcf_fileinfo')
    return vcf_fileinfo


def _prep_vcf_fileinfo(project_name, data_dir, project_data_dir=None, **kwargs):
    all_vcf_fileinfo = _load_vcf_fileinfo(project_name=project_name, project_data_dir=project_data_dir, data_dir=data_dir, **kwargs)
    vcf_fileinfo = all_vcf_fileinfo.loc[:,['submitter_id','filepath']]
    vcf_fileinfo.rename(columns = {'filepath': 'snv_vcf_paths'}, inplace=True)
    vcf_fileinfo['patient_id'] = vcf_fileinfo['submitter_id'].apply(lambda x: x.split('-')[2])
    vcf_fileinfo['snv_vcf_paths'] = vcf_fileinfo['snv_vcf_paths'].apply(helpers.convert_to_list)
    vcf_fileinfo_agg = vcf_fileinfo.groupby('patient_id').agg({'snv_vcf_paths': 'sum'}).reset_index()
    return vcf_fileinfo_agg


def build_cohort_patient(row, benefit_days, **kwargs):
    patient_id = row['case_id']
    deceased = row['vital_status'] != 'Alive'
    progressed = row['treatment_outcome_at_tcga_followup'] != 'Complete Response'
    censor_time = float(row['last_contact_days_to'])
    deceased_time = float(row['death_days_to'])
    progressed_time = float(row['new_tumor_event_dx_days_to'])

    row['progressed_time'] = progressed_time
    row['deceased_time'] = deceased_time
    row['censor_time'] = censor_time
    row['progressed'] = progressed
    row['deceased'] = deceased
    row['age'] = -1*row['birth_days_to']/365.25

    censor_time = censor_time or 0
    if np.isnan(censor_time):
        censor_time = max(progressed_time, deceased_time, censor_time)

    if censor_time > progressed_time:
        censor_time = progressed_time
    if censor_time > deceased_time:
        censor_time = deceased_time

    os = deceased_time if deceased else censor_time
    pfs = progressed_time if progressed else os

    if np.isnan(os):
        os = censor_time

    if np.isnan(pfs):
        pfs = os

    row['pfs'] = pfs
    row['os'] = os
    row['censor_time'] = censor_time

    pfs = min(pfs, os) ## force progressed time to be < os 

    benefit = pfs <= benefit_days

    assert(not np.isnan(pfs))
    assert(not np.isnan(os))
    assert pfs <= os, 'PFS {pfs} is not <= OS {os} for Patient {patid}'.format(pfs=pfs, os=os, patid=patient_id)

    # capture snv_vcf_paths, if they exist
    if 'snv_vcf_paths' in row.keys() and isinstance(row['snv_vcf_paths'], list):
        snv_vcf_paths = helpers.convert_to_list(row['snv_vcf_paths'])
    else:
        snv_vcf_paths = None

    patient = cohorts.Patient(
        id=str(patient_id),
        deceased=deceased,
        progressed=progressed,
        os=os,
        pfs=pfs,
        benefit=benefit,
        additional_data=row,
        snv_vcf_paths=snv_vcf_paths,
        **kwargs
    )
    return(patient)


def _merge_filepath_with_fileinfo(files):
    """ Given list of filepaths & fileinfo, add a field "filepath" to fileinfo & return it
    """ 
    filepath_data = pd.DataFrame(dict(file_id=helpers.convert_to_file_id(files), file_path=list(files)))
    fileinfo = files.fileinfo
    return pd.merge(fileinfo, filepath_data, on='file_id')



def prep_patients(project_name, data_dir=get_setting_value('GDC_DATA_DIR'), benefit_days=365.25,
                 include_vcfs=True, project_data_dir='data', cache_dir='data-cache', **kwargs):
    """ Given a project_name, return a list of cohorts.Patient objects
    """
    ## try to load config file, if it exists
    if os.path.exists('config.ini'):
        config.load_config('config.ini')

    clinical_data = _load_clinical_data(project_name=project_name, project_data_dir=project_data_dir, data_dir=data_dir, **kwargs)

    # merge clinical & vcf data
    if include_vcfs:
        vcf_fileinfo = _prep_vcf_fileinfo(project_name=project_name, project_data_dir=project_data_dir, data_dir=data_dir, **kwargs)
        clinical_data = clinical_data.merge(vcf_fileinfo, on='patient_id', how='left')
        assert clinical_data['snv_vcf_paths'].count()>0
        clinical_data.dropna(subset=['snv_vcf_paths'], inplace=True, axis=0)
    
    assert clinical_data.duplicated('patient_id').any() == False, 'Duplicates by patient_id'

    patients = []
    for (i, row) in clinical_data.iterrows():
        patients.append(build_cohort_patient(row, benefit_days=benefit_days))
    return patients


def prep_cohort(patients, cache_dir='data-cache', **kwargs):
    """ Given a list of patients, create a `cohorts.Cohort`
    """
    cohort = cohorts.Cohort(
            patients=patients,
            cache_dir=cache_dir,
            **kwargs
        )
    return(cohort)


