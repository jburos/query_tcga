query_tcga
===============================

author: Jacki Novik

Overview
--------

Helper functions to download TCGA data from GDC

Installation / Usage
--------------------

To install, clone the repo & edit the default values:

    $ git clone https://github.com/jburos/query_tcga.git
    $ vim query_tcga/query_tcga/defaults.py
    $ python setup.py install
    
Or, install from git via pip:
    
    $ pip install git+git://github.com/jburos/query_tcga

Setup
-----

There are a few steps you will have to follow before using this code. 
   - Install [gdc-client](https://github.com/NCI-GDC/gdc-client). 
       - Install per the [instructions](https://gdc-docs.nci.nih.gov/Data_Transfer_Tool/Users_Guide/Getting_Started/#downloading-the-gdc-data-transfer-tool)
       - Edit the variable `GDC_CLIENT_PATH` by one of the methods described below
   - Log into GDC, request access to TCGA & download an auth-token 
      1. Gain [authorization](https://gdc-docs.nci.nih.gov/API/Users_Guide/Authentication_and_Authorization/)
      2. Download the [authentication token](https://gdc-portal.nci.nih.gov/)
      3. Edit the variable `GDC_TOKEN_PATH` by one of the methods described below

Once you have these items set up, you can use this package to download and/or parse clinical, WXS, VCF and other files from the GDC portal by project. 


Configuration
-------------

Because many of the files available via the GDC require authentication, some configuration settings are required in order to use this package.

Only one setting is required to be set by the user: `GDC_TOKEN_PATH`. The other setting (`GDC_CLIENT_PATH`) is strongly recommended.

These and all settings can be configured in one of two ways:

1. Via a `config.ini` file, loaded in via `config.load_config()`
2. In Python code, updating value via `config.set_value()`

If you choose to use a config file, options should be included under `[main]`, like so:

**Example `config.ini` file **
```
[main]
GDC_TOKEN_PATH = /full/path/to/your/gdc_token_file.txt
GDC_CLIENT_PATH = /usr/local/bin/gdc-client
```

This can be loaded into a python script as:

```
from query_tcga import config

config.load_config('config.ini')
```

Example
-------

See the companion repo ([tcga-blca](http://github.com/jburos/tcga-blca)) for example usage.


Contributing
------------

TBD


Testing
--------

Test cases for query_tcga are written in [pytest](http://docs.pytest.org/en/latest/). 

For example, you can run test cases as follows:

```
python -m pytest tests --exitfirst -v
```
