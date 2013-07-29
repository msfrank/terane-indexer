#!/usr/bin/env python

import os, sys, time, json, pprint, restkit.resource


class TeraneServer(restkit.resource.Resource):

    headers = { 'Content-Type': 'application/json' }

    def __init__(self, url, **kwargs):
        super(TeraneServer, self).__init__(url, **kwargs)

    def create_query(self, query, store, fields=None, limit=100, reverse=False):
        body = { 'query': str(query), 'store': store, 'limit': int(limit), 'reverse': bool(reverse) }
        if fields != None:
            body['fields'] = list(fields)
        resp = self.post('/1/queries', headers=TeraneServer.headers, payload=json.dumps(body))
        return json.loads(resp.body_string())

    def retrieve_events(self, id):
        body = { 'limit': 5 }
        resp = self.post('/1/queries/' + id, headers=TeraneServer.headers, payload=json.dumps(body))
        return json.loads(resp.body_string())


if __name__ == "__main__":
    s = TeraneServer("http://localhost:8080")
    result = s.create_query(' '.join(sys.argv[1:]), "main")
    id = result['id']
    print "created:"
    pprint.pprint(result)
    print "retrieved:"
    finished = False
    while not finished:
    	result = s.retrieve_events(id)
        #pprint.pprint(result['events'])
        pprint.pprint(result)
        finished = result['finished']
        time.sleep(1)
