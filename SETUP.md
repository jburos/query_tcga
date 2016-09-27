# tcga-blca

Example using [Cohorts](http://github.com/hammerlab/cohorts) to manage TCGA-BLCA for analysis

1. Query GDC for clinical and sample datasets for TCGA-BLCA data (query code to be merged into [pygdc](http://github.com/arahuja/pygdc))
2. Set up a Cohort using [Cohorts](http://github.com/hammerlab/cohorts) to manage these data

# Setup 

There are two items which you will want to configure before using this code. 
   - [gdc-client](https://github.com/NCI-GDC/gdc-client). 
       - Install per the [instructions](https://gdc-docs.nci.nih.gov/Data_Transfer_Tool/Users_Guide/Getting_Started/#downloading-the-gdc-data-transfer-tool)
       - Edit the variable `GDC_CLIENT_PATH` in `query_tcga/defaults.py`
   - auth-token, because many files are controlled-access. 
      1. Gain [authorization](https://gdc-docs.nci.nih.gov/API/Users_Guide/Authentication_and_Authorization/)
      2. Download the [authentication token](https://gdc-portal.nci.nih.gov/)
      3. Edit the variable `GDC_TOKEN_PATH` in `query_tcga/defaults.py`

Once you have those two items set up, you can run one or both of the `refresh_*.py` scripts to fetch data from the GDC portal.

# Testing

Test cases are written in [pytest](http://docs.pytest.org/en/latest/). 

For example, you can run test cases as follows:

```
python -m pytest tests --exitfirst -v
```
