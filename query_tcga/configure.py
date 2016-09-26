from simple_settings import settings
from . import defaults

## load default settings if not already defined
default_settings = dict(
	USE_CACHE=defaults.USE_CACHE, 
	#GDC_TOKEN_PATH=defaults.GDC_TOKEN_PATH,
	GDC_CLIENT_PATH=defaults.GDC_CLIENT_PATH,
	GDC_API_ENDPOINT=defaults.GDC_API_ENDPOINT,
	GDC_DATA_DIR=defaults.GDC_DATA_DIR,
	# these are used since you cannot query them
	VALID_ENDPOINTS=defaults.VALID_ENDPOINTS,
	# number of records per page, by default
	DEFAULT_SIZE=defaults.DEFAULT_SIZE,
	# fields to pull for 'file-metadata' table
	DEFAULT_FILE_FIELDS=defaults.DEFAULT_FILE_FIELDS,
	DEFAULT_CHUNK_SIZE=defaults.DEFAULT_CHUNK_SIZE,
	)

def get_setting_value(setting_name, default_settings = default_settings, user_settings = settings.as_dict()):
	## use user-level setting if defined 
	if setting_name in user_settings:
		return user_settings[setting_name]
	## otherwise, use default value if defined
	elif setting_name in default_settings:
		return default_settings[setting_name]
	else:
		raise ValueError('Setting {setting_name} was not provided & is required.'.format(setting_name=setting_name))
