import os,sys
import subprocess
import logging
import pyfits
import re
import datetime

from params import (STAGING_DIR,SORTED_DIR)

logfmt = '%(levelname)s [%(asctime)s]:\t  %(message)s'
datefmt= '%m/%d/%Y %I:%M:%S %p'
formatter = logging.Formatter(fmt=logfmt,datefmt=datefmt)
logger = logging.getLogger('__main__')
logging.root.setLevel(logging.DEBUG)
ch = logging.StreamHandler() #console handler
ch.setFormatter(formatter)
logger.addHandler(ch)


def rewriteFitsHeader(f):
  updateData = {
    'TARGETID':[
      #('OriginalName','ChangedName'),
      ('ZillaMonster','CALIB'),
      ('Zilla_Monster','CALIB'),
    ],
  }

  hdulist = pyfits.open(f,mode='update')
  hdr = hdulist[0].header
  for k in updateData:
    if k not in hdr:
      continue
    for L in updateData[k]:
      if hdr[k] == L[0]:
        hdr.update(k,L[1])
  hdulist.flush()
  hdulist.close()
def sort():
  logger.info("Sorting data based on FITS keywords")
  initial_report = False
  for f in [os.path.join(STAGING_DIR,i) for i in os.listdir(os.path.abspath(STAGING_DIR))]:
    try:
      rewriteFitsHeader(f)
      hdulist = pyfits.open(f)
    except:
      logger.warning("Could not open %s" % f)
      continue
    hdr = hdulist[0].header
    
    if 'TARGETID' in hdr:
      target = hdr['TARGETID']
    else:
      target = 'UNKNOWN'
    date = datetime.datetime.strptime(hdr['DATE-OBS'],'%Y-%m-%dT%H:%M:%S.%f')
    if date.hour < 16: #We want the date folder to coorespond to the beginning of the night.
      date -= datetime.timedelta(days=1)
    date = date.strftime('%Y-%m-%d')
    if 'OBSRUNID' in hdr:
      ob = 'OB%s_%s' % (hdr['OBSRUNID'],hdr['OBSEQNUM'])
    else:
      ob = 'UNKNOWN'
    origfile = hdr['ORIGFILE']

    cmd = []
    cmd.append('mv')
   #cmd.append('--no-clobber')
    cmd.append(f)
    cmd.append(os.path.join(SORTED_DIR,date,'raw',target,ob,origfile))

    if not os.path.isdir(os.path.join(SORTED_DIR,date,'raw',target,ob)):
      P = subprocess.Popen('mkdir -p %s' % os.path.join(SORTED_DIR,date,'raw',target,ob),shell=True)
      P.wait()

    P = subprocess.Popen(cmd)
    #P.wait()
    #P.poll()
    #if P.returncode == 0:
    #  os.remove(f)
    if not initial_report:
      logger.info('Example command: %s' % ' '.join(cmd))
      initial_report = True


def unzip():
  zippedFiles = [f for f in os.listdir(os.path.abspath(STAGING_DIR)) if re.search('\.fits\.Z',f)]
  if not zippedFiles:
    logger.warning("No zipped files in %s" % STAGING_DIR)
    return
  logger.info('Unzipping all *fits.Z files in %s' % STAGING_DIR)
  P = subprocess.Popen('gzip -d %s/*fits.Z' % STAGING_DIR,shell=True)
  P.wait()
  logger.info('Finished unzipping')

def main():
  unzip()
  sort()

if __name__=="__main__":
  main()
