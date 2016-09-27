#from ConfigParser import SafeConfigParser
from . import defaults
import os
import logging
import configparser

## empty class to hold "current" settings
class Settings:
    pass

## module-wide collection of settings
__DEFAULTS = Settings()
__DEFAULTS.USE_CACHE = defaults.USE_CACHE
__DEFAULTS.GDC_CLIENT_PATH = defaults.GDC_CLIENT_PATH
__DEFAULTS.GDC_TOKEN_PATH = None
__DEFAULTS.GDC_API_ENDPOINT = defaults.GDC_API_ENDPOINT
__DEFAULTS.GDC_DATA_DIR = defaults.GDC_DATA_DIR
__DEFAULTS.VALID_ENDPOINTS = defaults.VALID_ENDPOINTS
__DEFAULTS.DEFAULT_SIZE = defaults.DEFAULT_SIZE
__DEFAULTS.DEFAULT_FILE_FIELDS = defaults.DEFAULT_FILE_FIELDS
__DEFAULTS.DEFAULT_CHUNK_SIZE = defaults.DEFAULT_CHUNK_SIZE


REQUIRED_SETTINGS = ['GDC_TOKEN_PATH']

def restore_default_settings():
    """ Restore settings to default values. 
    """
    global __DEFAULTS
    __DEFAULTS.USE_CACHE = defaults.USE_CACHE
    __DEFAULTS.GDC_CLIENT_PATH = defaults.GDC_CLIENT_PATH
    __DEFAULTS.GDC_TOKEN_PATH = None
    __DEFAULTS.GDC_API_ENDPOINT = defaults.GDC_API_ENDPOINT
    __DEFAULTS.GDC_DATA_DIR = defaults.GDC_DATA_DIR
    __DEFAULTS.VALID_ENDPOINTS = defaults.VALID_ENDPOINTS
    __DEFAULTS.DEFAULT_SIZE = defaults.DEFAULT_SIZE
    __DEFAULTS.DEFAULT_FILE_FIELDS = defaults.DEFAULT_FILE_FIELDS
    __DEFAULTS.DEFAULT_CHUNK_SIZE = defaults.DEFAULT_CHUNK_SIZE
    logging.info('Settings reverted to their default values.')


def load_config(config_file='~/.query_tcga.ini'):
    """ Load config file into default settings
    """
    if not os.path.exists(config_file):
        logging.warning('Config file does not exist: {}. Using default settings.'.format(config_file))
        return
    ## get user-level config in *.ini format
    config = configparser.ConfigParser()
    config.read(config_file)
    if not config.has_section('main'):
        raise ValueError('Config file {} has no section "main"'.format(config_file))
    for (key, val) in config.items('main'):
        _set_value(key.upper(), val)
    return


def _set_value(setting_name, value):
    global __DEFAULTS
    setattr(__DEFAULTS, setting_name, value)
    logging.info('Setting {name} = {value}'.format(name=setting_name, value=value))


def set_value(**kwargs):
    args = dict(**kwargs)
    for key in args:
        _set_value(key, args[key])


def get_setting_value(setting_name):
    val = getattr(__DEFAULTS, setting_name)
    if not setting_name in REQUIRED_SETTINGS:
        return val
    if val is not None:
        return val
    else:
        raise ValueError('Setting {setting_name} was not provided & is required.'.format(setting_name=setting_name))


#def write_config(file = 'config.ini', config = default_settings):
#    with open(file, 'w') as f:
#        config.write(f)
