#!/usr/bin/env python2
import zmq
import msgpack
import argparse
import datetime


def send_cmd(msg):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.connect('ipc:///tmp/esoportal.sock')
    sock.send(msgpack.packb(msg))
    msg = sock.recv()
    print msg

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='cmd', help='sub-command help')
    parser_r = subparsers.add_parser('request', help='Request to download data')
    parser_l = subparsers.add_parser('list', help='List requests in the queue')
    parser_r.add_argument('day')

    args = parser.parse_args()

    if args.cmd == 'request':
        if args.day == 'today':
            day = (datetime.date.today()).toordinal()
        else:
            day = datetime.datetime.strptime(args.day, '%Y-%m-%d').toordinal()
        req = {
            'cmd': 'req',
            'day': day
        }
        send_cmd(req)
    elif args.cmd == 'list':
        req = {'cmd': 'list'}
        send_cmd(req)
        
