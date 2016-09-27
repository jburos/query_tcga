# whether to use requests-cache 
USE_CACHE = False
## location of token authorizing download
GDC_TOKEN_PATH = '/Users/jacquelineburos/Downloads/gdc-user-token.2016-09-12T16-39-34-04-00.txt'
## path to gdc-client
GDC_CLIENT_PATH = '/usr/local/bin/gdc-client'
## API endpoint base URL (contains version, etc)
GDC_API_ENDPOINT = 'https://gdc-api.nci.nih.gov/{endpoint}'
## location to download files to (gdc-client executes in this dir)
GDC_DATA_DIR='data/gdc'
# not used but helpful to see
VALID_CATEGORIES = [
 "Simple Nucleotide Variation",
 "Copy Number Variation",
 "Biospecimen",
 "Raw Sequencing Data",
 "Transcriptome Profiling",
 "Biospecimen",
 "Clinical",
]
# these are used since you cannot query them
VALID_ENDPOINTS = ['files', 'projects', 'cases', 'annotations']
# number of records per page, by default
DEFAULT_SIZE = 10
# fields to pull for 'file-metadata' table
DEFAULT_FILE_FIELDS=['file_id','file_name','cases.submitter_id','cases.case_id','data_category','data_type','cases.samples.tumor_descriptor','cases.samples.tissue_type','cases.samples.sample_type','cases.samples.submitter_id','cases.samples.sample_id', 'analysis.analysis_id', 'files.analysis.workflow_type']
DEFAULT_CHUNK_SIZE=30
