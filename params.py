import os
import datetime

USERNAME = "fake"
PASSWORD = "fake"

LOGIN_URL= "https://www.eso.org/sso/login"
LOGOUT_URL = "http://www.eso.org/UserPortal/authenticatedArea/logout.eso"
ARCHIVE_URL = "http://archive.eso.org/wdb/wdb/eso/eso_archive_main/query"
RETRIEVAL_URL = "http://archive.eso.org/cms/eso-data/eso-data-direct-retrieval.html"

SLEEP_TIME = 0.5 #Minutes to wait before making the request and downloading the files

LOGFILE = os.path.join(os.path.dirname(__file__),'esoportal.log')

STAGING_DIR = os.path.join(os.path.dirname(__file__),'staging/')
SORTED_DIR = '/data1/GROND/'
SORTED_DATA_LIFETIME = 43829.1 #Minutes, (43829.1 minutes = 1 month)

INSTRUMENT="((ins_id like 'GROND%'))" #This will go directly into the querystring. Keep the format.
ARCNAME_REGEX = 'GROND\.\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\d'
PROGRAM_ID = '092.A-9099(A)'
START_DATE = (datetime.date.today()-datetime.timedelta(days=1)).strftime("%d %m %Y") #string: "d m Y" format
END_DATE = datetime.date.today().strftime("%d %m %Y")
