import os,sys
import subprocess
import logging
import pyfits
import re

from params import (STAGING_DIR,SORTED_DIR)

logfmt = '%(levelname)s [%(asctime)s]:\t  %(message)s'
datefmt= '%m/%d/%Y %I:%M:%S %p'
formatter = logging.Formatter(fmt=logfmt,datefmt=datefmt)
logger = logging.getLogger('__main__')
logging.root.setLevel(logging.DEBUG)
ch = logging.StreamHandler() #console handler
ch.setFormatter(formatter)
logger.addHandler(ch)

def sort():
  logger.info("Sorting data based on FITS keywords")
  initial_report = False
  for f in [os.path.join(STAGING_DIR,i) for i in os.listdir(os.path.abspath(STAGING_DIR))]:
    try:
      hdulist = pyfits.open(f)
    except:
      logger.warning("Could not open %s" % f)
      continue
    hdr = hdulist[0].header

    target = hdr['TARGETID']
    date = hdr['DATE-OBS']
    date = date[:date.find('T')]
    ob = 'OB%s_%s' % (hdr['OBSRUNID'],hdr['OBSEQNUM'])
    origfile = hdr['ORIGFILE']

    cmd = []
    cmd.append('mv')
    cmd.append('--no-clobber')
    cmd.append(f)
    cmd.append(os.path.join(SORTED_DIR,date,'raw',target,origfile))

    if not os.path.isdir(os.path.join(SORTED_DIR,date,'raw',target)):
      P = subprocess.Popen('mkdir -p %s' % os.path.join(SORTED_DIR,date,'raw',target),shell=True)
      P.wait()

    P = subprocess.Popen(cmd)
    if not initial_report:
      logger.info('Example command: %s' % ' '.join(cmd))
      initial_report = True


def unzip():
  zippedFiles = [f for f in os.listdir(os.path.abspath(STAGING_DIR)) if re.search('\.fits\.Z',f)]
  if not zippedFiles:
    logger.warning("No zipped files in %s" % STAGING_DIR)
    return
  logger.info('Unzipping all *fits.Z files in %s' % STAGING_DIR)
  P = subprocess.Popen('gzip -d %s/*fits.Z' % STAGING_DIR)
  P.wait()
  logger.info('Finished unzipping')

def main():
  unzip()
  sort()

if __name__=="__main__":
  main()