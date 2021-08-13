import os
from confidential_config import *

SECRET_API_KEY = os.urandom((32))
PRODUCTION_MACHINE = False
USE_PRODUCTION_DATABASES = True

LOGFILE = 'rest_data.log'
if PRODUCTION_MACHINE:
    LOGPATH = '/var/log/wq_rest'
else:
    LOGPATH = './'

FULL_LOG_PATH = os.path.join(LOGPATH, LOGFILE)
if USE_PRODUCTION_DATABASES:
    SHELLBASE_CONNECTION_STRING = "{database_type}://{db_user}:{db_password}@{db_host}/{db_name}".format(
        database_type=DATABASE_TYPE,
        db_user=DATABASE_USER,
        db_password=DATABASE_PASSWORD,
        db_host=DATABASE_HOST_ADDRESS,
        db_name=DATABASE_NAME
    )
else:
    SHELLBASE_CONNECTION_STRING = "{database_type}://{db_user}:{db_password}@{db_host}/{db_name}".format(
        database_type=DEBUG_DATABASE_TYPE,
        db_user=DEBUG_DATABASE_USER,
        db_password=DEBUG_DATABASE_PASSWORD,
        db_host=DEBUG_DATABASE_HOST_ADDRESS,
        db_name=DEBUG_DATABASE_NAME
    )
