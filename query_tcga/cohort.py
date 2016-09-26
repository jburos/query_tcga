from __future__ import absolute_import
import cohorts
from . import query_tcga as qt
from . import samples
from . import helpers
import numpy as np
import pandas as pd

def prep_patient_data(row, snv_vcf_paths=None, **kwargs):
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

    benefit = pfs <= 365.25

    assert(not np.isnan(pfs))
    assert(not np.isnan(os))
    assert pfs <= os, 'PFS {pfs} is not <= OS {os} for Patient {patid}'.format(pfs=pfs, os=os, patid=patient_id)

    patient = cohorts.Patient(
        id=str(patient_id),
        deceased=deceased,
        progressed=progressed,
        os=os,
        pfs=pfs,
        benefit=benefit,
        additional_data=row,
        snv_vcf_paths=snv_vcf_paths,
    )
    return(patient)


def _merge_filepath_with_fileinfo(files):
    """ Given list of filepaths & fileinfo, add a field "filepath" to fileinfo & return it
    """ 
    filepath_data = pd.DataFrame(dict(file_id=helpers.convert_to_file_id(files), file_path=list(files)))
    fileinfo = files.fileinfo
    return pd.merge(fileinfo, filepath_data, on='file_id')




def prep_cohort_patients(project_name, include_vcfs=True, n=None, **kwargs):
    """ Given a project_name, return a list of cohorts.Patient objects
    """
    clin = qt.get_clinical_data(project_name=project_name, n=n, **kwargs)
    vcf_fileinfo = None
    if not n and include_vcfs:
        all_vcf_files = samples.download_vcf_files(project_name=project_name, **kwargs)
        vcf_fileinfo = _merge_filepath_with_fileinfo(all_vcf_files)
    patients = []
    for (i, row) in clin.iterrows():
        if include_vcfs:
            if vcf_fileinfo is None:
                vcf_files = samples.download_vcf_files(query_args={'cases.case_id': row['case_id']}, project_name=project_name)
            else:
                vcf_files = vcf_fileinfo.loc[vcf_fileinfo['case_id']==row['case_id'], 'file_path']
            patients.append(prep_patient_data(row, snv_vcf_paths=vcf_files))
        else:
            patients.append(prep_patient_data(row))
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


