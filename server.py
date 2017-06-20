#!/usr/bin/env python2

import Queue as queue
import zmq
import msgpack
import threading
import logging
import subprocess
import datetime
import time
import sys
from EsoPortal import EsoPortal


class Job(object):
    def __init__(self, day, user=None, prog_id=None):
        self.state = 'pending'
        self.created = time.time()
        self.day = day
        self.user = user
        self.prog_id = prog_id
        self.message = ''
        self.log = logging.getLogger('job')

    def run(self, auth_db, prog_db):
        self.state = 'prepare'
        conn = EsoPortal()
        if self.prog_id is not None:
            user = prog_db[self.prog_id]
        else:
            user = self.user
        if user is not None:
            users = [user]
        else:
            users = auth_db.keys()

        for user in users:
            if user not in auth_db:
                self.state = 'failed'
                self.message = 'unknown user'
                return
            try:
                if not conn.login(u=user, p=auth_db[user]):
                    self.log.error('Failed to login')
                    continue
            except:
                self.log.exception('Failed to login')
                continue
            if self.prog_id is not None:
                progs = self.prog_id
            else:
                progs = filter(lambda a: prog_db[a] == user, prog_db.keys())
            for prog_id in progs:
                self.state = 'running'
                try:
                    conn.queryArchive(pid=prog_id,
                                      sdate=datetime.date.fromordinal(self.day).strftime("%d %m %Y"),
                                      edate=datetime.date.fromordinal(self.day+1).strftime("%d %m %Y"))
                    conn.createRequest()
                    conn.retrieveData()
                    conn.verifyData()
                except:
                    self.log.exception('Failed to request data')
                    self.state = 'failed'
                    self.message('Failed to request data')
            conn.logout()
        self.state = 'sorting'
        subprocess.check_call(['python2', 'sortData.py'])
        self.state = 'success'
    


class Runner(threading.Thread):
    def __init__(self, auth_db, prog_db):
        self.q = queue.Queue()
        self.requests = {}
        self.auth_db = auth_db
        self.prog_db = prog_db
        self.log = logging.getLogger('run')
        threading.Thread.__init__(self, name='run')

    def run(self):
        while True:
            job_id = self.q.get()
            if self.requests[job_id].state != 'pending':
                continue
            try:
                self.requests[job_id].run(self.auth_db, self.prog_db)
            except:
                self.log.exception('Job error')
                self.requests[job_id].state = 'failed'


def format_table(lines):
    ret = ''
    for line in lines:
        ret += '\t'.join(line)
        ret += '\n'
    return ret

def format_job(job_id, job):
    ret = [str(job_id), job.state, datetime.datetime.fromordinal(job.day).strftime('%Y-%m-%d')]
    if job.user is not None:
        ret += [job.user]
    else:
        ret += ['*']
    if job.prog_id is not None:
        ret += [job.prog_id]
    else:
        ret += ['*']
    return ret
    
            
class Server(object):
    def __init__(self, runner):
        self.zmq = zmq.Context()
        self.sock = self.zmq.socket(zmq.REP)
        self.sock.bind('ipc:///tmp/esoportal.sock')
        self.runner = runner
        self.log = logging.getLogger('srv')
        self.req_ctr = 0

    def run(self):
        while True:
            msg = self.sock.recv()
            try:
                msg = msgpack.unpackb(msg)
                cmd = msg['cmd']
                if cmd == 'req':
                    self.cmd_req(msg)
                elif cmd == 'list':
                    self.cmd_list(msg)
                elif cmd == 'status':
                    self.cmd_status(msg)
                else:
                    self.sock.send('-ERROR: Invalid command "%s"' % cmd)
            except:
                self.log.exception('Invalid command')

    def cmd_req(self, msg):
        job_id = self.req_ctr
        self.req_ctr += 1
        if 'user' not in msg:
            msg['user'] = None
        if 'prog_id' not in msg:
            msg['prog_id'] = None
        job = Job(day=msg['day'], user=msg['user'],
                  prog_id=msg['prog_id'])
        self.runner.requests[job_id] = job
        self.runner.q.put(job_id)
        self.sock.send('-OK: Job created (ID=%s)' % job_id)

    def cmd_list(self, msg):
        ret = '-OK: %d jobs\n' % len(self.runner.requests)
        def build_line(job_id):
            job = self.runner.requests[job_id]
            return format_job(job_id, job)
        tab = [['ID', 'State', 'Day', 'User', 'PID']]
        tab += map(build_line, self.runner.requests.keys())
        ret += format_table(tab)
        #for job_id in self.runner.requests.keys():
        # TODO: list jobs
        self.sock.send(ret)
            

    def cmd_status(self, msg):
        job_id = msg['job_id']
        if job_id not in self.runner.requests:
            self.sock.send('-ERROR: Invalid job id "%s"' % job_id)
            return
        job = self.runner.requests[job_id]
        self.sock.send('-OK: Job in %s status' % job.state)
    

def main():
    log = logging.getLogger('main')
    if len(sys.argv) != 2:
        print 'Usage: python server.py <batchfile>'
        sys.exit(1)
    log.info('Loading authdb from %s', sys.argv[1])
    with open(sys.argv[1], 'r') as f:
        lines = f.readlines()
    lines = filter(lambda a: a,
                   filter(lambda a: not a.startswith('#'),
                          lines))
    auth_db = {}
    prog_db = {}
    for pid,user,password in map(lambda a: a.strip().split(), lines):
        if user not in auth_db:
            auth_db[user] = password
        if pid not in prog_db:
            prog_db[pid] = user

    runner = Runner(auth_db, prog_db)
    runner.start()
    server = Server(runner)
    server.run()
    

            
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
