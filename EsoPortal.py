'''
Vladimir Sudilovsky <vsudilovsky@gmail.com>

Relevant parameters must be defined in params.py.
params.py must be importable by EsoPortal.py

Example usage:

  import EsoPortal
  
  conn = EsoPortal.EsoPortal()
  conn.login()
  conn.queryArchive()
  conn.createRequest()
  conn.retrieveData()
  conn.verifyData()
  conn.logout()
'''

import requests
from bs4 import BeautifulSoup
import pyfits
import mechanize

import os,sys
from time import sleep
import mimetools
from urllib import urlencode
import datetime
import logging
from logging.handlers import RotatingFileHandler
import re
import subprocess

from params import (USERNAME,PASSWORD,LOGIN_URL,LOGOUT_URL,LOGFILE,
                    INSTRUMENT, STAGING_DIR, SORTED_DIR, SORTED_DATA_LIFETIME,
                    ARCHIVE_URL,ARCNAME_REGEX, RETRIEVAL_URL, METADATA_PICKLE,
                    SLEEP_TIME, PROGRAM_ID, START_DATE, END_DATE, MAX_PROCESSES)

class EsoPortal:
  def __init__(self):
    self.session = requests.session()
    logfmt = '%(levelname)s [%(asctime)s]:\t  %(message)s'
    datefmt= '%m/%d/%Y %I:%M:%S %p'
    formatter = logging.Formatter(fmt=logfmt,datefmt=datefmt)
    self.logger = logging.getLogger('__main__')
    logging.root.setLevel(logging.DEBUG)
    rfh = RotatingFileHandler(filename=LOGFILE,maxBytes=1048576,backupCount=3,mode='a') #1MB file
    rfh.setFormatter(formatter)
    ch = logging.StreamHandler() #console handler
    ch.setFormatter(formatter)
    self.logger.addHandler(ch)
    self.logger.addHandler(rfh)

  def login(self):
    login_data = {
      'username': USERNAME,
      'password': PASSWORD,
      'submit': 'login',
      'service': "https://www.eso.org:443/UserPortal/security_check",
      "_eventId": "submit",
    }
    self.logger.info("Attempting login")
    r = self.session.get(LOGIN_URL)
    soup = BeautifulSoup(r.content) 
    csrf_tag = soup.find_all('input',attrs={"name":"lt"})[0]
    csrftoken = csrf_tag.attrs['value']
    login_data.update({"lt":csrftoken})
    self.session.post(LOGIN_URL, data=login_data, headers=dict(Referer=LOGIN_URL))
    #TODO: Test for success/failure

  def logout(self):
    self.session.get(LOGOUT_URL)
    self.logger.info("Logged out")

  def queryArchive(self):
    queryparams = {
        'add':INSTRUMENT,
        'max_rows_returned':10000,
        'stime':START_DATE,
        'etime':END_DATE,
        #'starttime':(datetime.datetime.now()-datetime.timedelta(hours=2)).hour,
        #'endtime':datetime.datetime.now().hour,
        'wdbo':'ascii',
        'prog_id': PROGRAM_ID,
        }
    url = '%s?%s' % (ARCHIVE_URL,urlencode(queryparams))
    r = self.session.get(url)
    self.currentData = []
    for line in r.content.split('\n'):
      arcname = re.search(ARCNAME_REGEX,line)
      if arcname:
        self.currentData.append(arcname.group())
    self.logger.info("Query returned %s files" % len(self.currentData))

  def createRequest(self):
    arcfiles = self.currentData
    #This should work, but it doesn't. I would love for someone to tell me why.
    ########################################
    # post_params = {
    #   #'list_of_datasets':'\n'.join(arcfiles),
    #   'list_of_datasets':'GROND.2013-10-06T05:51:16.222\n',
    #   'mode':  'datasets_list',
    #   'archive':'SAF',
    #   'userfile': '',
    #   'count':'count characters',
    #   'file_columns': '1',
    # }
    # boundary = mimetools.choose_boundary()
    # headers = {'Content-Type':'multipart/form-data; boundary=%s' % boundary}
    # prepped = requests.Request('POST',  # or any other method, 'POST', 'PUT', etc.
    #                   RETRIEVAL_URL,
    #                   data=post_params,
    #                   headers=headers,
    #                   # ...
    #                   ).prepare()
    # buffer = ''
    # for k,v in post_params.iteritems():
    #   buffer += '--%s\r\n' % boundary
    #   buffer += 'Content-Disposition: form-data; name="%s"' % k
    #   buffer += '\r\n\r\n' + v + '\r\n'
    # buffer += '--%s--\r\n\r\n' % boundary  
    # prepped.body=buffer  
    # r = self.session.send(prepped)

      #We will go the 'mechanize' route instead
    self.br = mechanize.Browser()
    self.br.set_handle_robots(False)   # ignore robots
    self.br.set_cookiejar(self.session.cookies)
    self.br.open(RETRIEVAL_URL)
    self.br.form = list(self.br.forms())[1]
    self.br.form.controls[0].value = '\n'.join(arcfiles)
      ##Python mechanize is broken, fixing it: 
      ##http://stackoverflow.com/questions/2394420/python-mechanize-ignores-form-input-in-the-html
    r = self.br.submit()
    r.set_data(r.get_data().replace("<br/>", "<br />"))
    self.br.set_response(r)

      #Now we are on the 'confirmation' page
    self.br.form = list(self.br.forms())[1]
    r = self.br.submit() #Naming is optional, we can submit immediatly

      #Next page is populated with AJAX calls, so mechanize becomes unweidly.
      #However, we can just download the wget script for the direct access URLs after
      #some delay to ensure that the files are present.
    self.script_url = os.path.join(self.br.geturl(),'script')
    sleep(SLEEP_TIME*60.)
    r = self.session.get(self.script_url)
    if r.status_code != 200:
      msg = "Unable to download script [%s]. Is SLEEP_TIME set high enough?" % script_url
      self.logger.error(msg)
      sys.exit(msg)
    self.script = r.content

  def retrieveData(self):
    lines = [line for line in self.script.split('\n') if line ]
    lines = [line for line in lines if not line.startswith('#')]
    procs = []
    self.logger.info("Starting downloads with %s processes" % MAX_PROCESSES)
    for line in lines:
      line = '%s -nc -P %s' % (line,STAGING_DIR)
      while len(procs) >= MAX_PROCESSES:
        [p.poll() for p in procs]
        procs = [p for p in procs if not type(p.returncode)!=int]
        sleep(1)
        self.logger.debug("Running wget procs: %s" %len(procs))

      P = subprocess.Popen(line,shell=True)
      procs.append(P)

  def verifyData(self):
    files = [f.replace('.fits.Z','') for f in os.listdir(os.path.abspath(STAGING_DIR))]
    diff = set(self.currentData).difference(files)
    if diff:
      self.logger.warning("%s files seem to be missing." % len(diff))
      return False
    self.logger.info("All files have downloaded.")
    return True

def main():
  print __doc__

if __name__=="__main__":
  main()