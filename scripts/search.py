#!/usr/bin/env python

import argparse, os, sys, time, json, pprint, restkit.resource

class TeraneServer(restkit.resource.Resource):

  headers = { 'Content-Type': 'application/json' }

  def __init__(self, url, **kwargs):
    super(TeraneServer, self).__init__(url, **kwargs)

  def create_query(self, args):
    query = ' '.join(args.query)
    body = { 'query': query, 'store': args.store }
    if args.fields != list():
      body['fields'] = args.fields
    if args.sortby != list():
      body['sortBy'] = args.sortby
    if args.limit != None:
      body['limit'] = args.limit
    payload = json.dumps(body)
    print "sending: " + payload
    resp = self.post('/1/queries', headers=TeraneServer.headers, payload=payload)
    return json.loads(resp.body_string())

  def retrieve_events(self, id):
    body = {}
    payload = json.dumps(body)
    print "sending: " + payload
    resp = self.post('/1/queries/' + id + '/events', headers=TeraneServer.headers, payload=payload)
    return json.loads(resp.body_string())

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
  parser.add_argument('query',
    metavar='QUERY',
    nargs='+')

  args = parser.parse_args()

  endpoint = TeraneServer("http://" + args.host + ":" + str(args.port))

  result = endpoint.create_query(args)
  id = result['id']
  print "created:"
  pprint.pprint(result)
  print "retrieved:"

  finished = False
  while not finished:
    result = endpoint.retrieve_events(id)
    pprint.pprint(result)
    finished = result['finished']
    time.sleep(1)
