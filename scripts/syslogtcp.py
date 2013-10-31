#!/usr/bin/env python

import os, sys, argparse, socket, ssl
from loggerglue.logger import Logger
from loggerglue.emitter import TCPSyslogEmitter
from loggerglue.rfc5424 import SDElement
from loggerglue import constants

def stringtoprival(facility, severity):
    return getattr(constants, "LOG_" + facility) + getattr(constants, "LOG_" + severity)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Send RFC5424-compliant syslog message via TCP')

  parser.add_argument('-H','--host',
    dest='host',
    metavar='HOST', 
    default='localhost',
    help='syslog host')
  parser.add_argument('-P','--port',
    dest='port',
    metavar='PORT', 
    type=int,
    default=514,
    help='syslog port')
  parser.add_argument('-x','--tls',
    action='store_true',
    dest='tls',
    help='use TLS')
  parser.add_argument('--hostname',
    metavar='HOSTNAME', 
    default=socket.getfqdn(),
    help='message hostname')
  parser.add_argument('-f','--facility',
    dest='facility',
    choices=('KERN', 'USER', 'MAIL', 'DAEMON', 'AUTH', 'SYSLOG', 'LPR', 'NEWS', 'UUCP', 'CRON', 'AUTHPRIV', 'LOCAL0', 'LOCAL1', 'LOCAL2', 'LOCAL3', 'LOCAL4', 'LOCAL5', 'LOCAL6', 'LOCAL7'),
    metavar='facility', 
    default='INFO',
    help='message facility')
  parser.add_argument('-s','--severity',
    dest='severity',
    choices=('DEBUG','INFO','NOTICE','WARNING','ERROR','CRITICAL','ALERT','EMERG'),
    metavar='SEVERITY', 
    default='USER',
    help='message severity')
  parser.add_argument('--appname',
    metavar='APPNAME', 
    default=None,
    help='message appname')
  parser.add_argument('--procid',
    metavar='PROCID', 
    default=None,
    help='message process ID')
  parser.add_argument('--msgid',
    metavar='MSGID', 
    default=None,
    help='message ID')
  parser.add_argument('-m','--message',
    dest='message',
    metavar='MESSAGE', 
    default=None,
    help='message text')

  args = parser.parse_args()

  print "syslog host: %s:%i" % (args.host,args.port)
  print "hostname: " + args.hostname
  print "tls: " + str(args.tls)
  print "facility: " + args.facility
  print "severity: " + args.severity
  if (args.appname):
    print "appname: " + args.appname
  if (args.procid):
    print "procid: " + args.procid
  if (args.msgid):
    print "msgid: " + args.msgid
  if (args.message):
    print "message: " + args.message

  ssl_args = dict(cert_reqs=ssl.CERT_NONE) if args.tls else dict()

  syslog = Logger(
    emitter=TCPSyslogEmitter(address=(args.host, args.port), **ssl_args),
    hostname=args.hostname,
    app_name=args.appname,
    procid=args.procid
    )

  syslog.log(
    prival=stringtoprival(args.facility, args.severity),
    msg=args.message,
    msgid=args.msgid,
    structured_data=None,
    )
  syslog.close()
