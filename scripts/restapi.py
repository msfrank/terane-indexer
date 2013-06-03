#!/usr/bin/env python

import os, sys, json, pprint, restkit.resource


class TeraneServer(restkit.resource.Resource):

    headers = { 'Content-Type': 'application/json' }

    def __init__(self, url, **kwargs):
        super(TeraneServer, self).__init__(url, **kwargs)

    def create_query(self, query, stores=None, fields=None, limit=100, reverse=False):
        body = { 'query': str(query), 'limit': int(limit), 'reverse': bool(reverse) }
        if stores != None:
            body['stores'] = list(stores)
        if fields != None:
            body['fields'] = list(fields)
        resp = self.post('/1/queries', headers=TeraneServer.headers, payload=json.dumps(body))
        return json.loads(resp.body_string())

    def retrieve_events(self, id):
        resp = self.post('/1/queries/' + id, headers=TeraneServer.headers)
        return json.loads(resp.body_string())


if __name__ == "__main__":
    s = TeraneServer("http://localhost:8080")
    created = s.create_query(' '.join(sys.argv[1:]))
    print "created: " + pprint.pformat(created)
    events = s.retrieve_events(created['id'])
    print "retrieved: " + pprint.pformat(events)
