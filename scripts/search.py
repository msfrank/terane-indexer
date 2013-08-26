#!/usr/bin/env python

import argparse, os, sys, time, json, pprint, restkit.resource, logging

class TeraneServer(restkit.resource.Resource):

  headers = { 'Content-Type': 'application/json' }

  def __init__(self, url, **kwargs):
    self.url = url
    super(TeraneServer, self).__init__(url, **kwargs)

  def create_query(self, args):
    query = ' '.join(args.query)
    body = { 'query': query, 'store': args.store }
    if args.fields != None:
      body['fields'] = args.fields
    if args.sortby != None:
      body['sortBy'] = args.sortby
    if args.limit != None:
      body['limit'] = args.limit
    if args.reverse == True:
      body['reverse'] = args.reverse
    payload = json.dumps(body)
    path = '/1/queries'
    logging.debug("POST %s%s\n  Headers: %s\n  Payload: %s", self.url, path, TeraneServer.headers, payload)
    resp = self.post(path, headers=TeraneServer.headers, payload=payload)
    body = json.loads(resp.body_string())
    logging.debug("%s\n  Headers: %s\n  Payload: %s", resp.status, dict(resp.headers), body)
    return body

  def retrieve_events(self, id):
    path = "/1/queries/" + id + "/events"
    logging.debug("GET %s%s\n  Headers: %s\n", self.url, path, TeraneServer.headers)
    resp = self.get(path, headers=TeraneServer.headers)
    body = json.loads(resp.body_string())
    logging.debug("%s\n  Headers: %s\n  Payload: %s", resp.status, dict(resp.headers), body)
    return body

def to_field_identifier(s):
  fieldtype,sep,fieldname = s.partition(':')
  if sep == '' and fieldname == '':
    raise argparse.ArgumentTypeError("failed to parse field " + s)
  return (fieldtype,fieldname)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='perform a query')
  parser.add_argument('-H','--host',
    dest='host',
    metavar='HOST', 
    default='localhost',
    help='terane-indexer host')
  parser.add_argument('-P','--port',
    dest='port',
    metavar='PORT', 
    type=int,
    default=8080,
    help='terane-indexer port')
  parser.add_argument('--store',
    dest='store',
    metavar='STORE', 
    default='main',
    help='store to query')
  parser.add_argument('--fields',
    action='append',
    type=to_field_identifier,
    dest='fields',
    metavar='FIELDS', 
    help='event fields to return')
  parser.add_argument('--sort-by',
    action='append',
    type=to_field_identifier,
    dest='sortby',
    metavar='FIELDS', 
    help='sort events by the specified fields')
  parser.add_argument('--limit',
    dest='limit',
    metavar='LIMIT', 
    type=int,
    default=None,
    help='return no more than LIMIT events')
  parser.add_argument('--reverse',
    dest='reverse',
    action='store_true',
    help='reverse the sort order')
  parser.add_argument('-d','--debug',
    dest='debug',
    action='store_true',
    help='reverse the sort order')
  parser.add_argument('query',
    metavar='QUERY',
    nargs='+')

  args = parser.parse_args()

  loglevel = logging.DEBUG if args.debug else logging.INFO
  logging.basicConfig(level=loglevel, format='%(asctime)s %(levelname)s: %(message)s')

  endpoint = TeraneServer("http://" + args.host + ":" + str(args.port))

  result = endpoint.create_query(args)
  id = result['id']
  result = endpoint.retrieve_events(id)
  for id,event in result['events']:
    fields = {}
    for name,typevals in event.items():
      for t,v in typevals.items():
        fields["%s:%s" % (t,name)] = v
    print ' '.join(["%s=%s" % (k,v) for k,v in fields.items()])
