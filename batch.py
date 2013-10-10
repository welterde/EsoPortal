from EsoPortal import EsoPortal
import argparse

def main():
  parser = argparse.ArgumentParser(description='Enter batch filename with format "PROGRAM_ID USERNAME PASSWORD"')
  parser.add_argument('file',nargs=1,help='Enter batch filename with format "PROGRAM_ID USERNAME PASSWORD"')
  args = parser.parse_args()

  with open(args.file[0],'r') as fp:
    lines = fp.readlines()
    
  lines = lines.split('\n')  
  lines = [i for i in lines if i]
  lines = [i for i in lines if not i.startswith('#')]
  for line in lines:
    pid,u,p = line
    conn = EsoPortal()
    conn.login(u=u,p=p)
    conn.queryArchive(pid=pid)
    conn.createRequest()
    conn.retrieveData()
    conn.verifyData()
    conn.logout()
   
   
if __name__=="__main__":
  main()