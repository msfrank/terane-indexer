.. http:put:: /(int:version)/queries

   Create a query context.

   **Example request**:

   .. sourcecode:: http

      PUT /1/queries
      Host: example.com
      Accept: application/json

      {
        "store": "main",
        "query": "foo AND bar",
        "fields": [["message","text"], "origin"],
        "limit": 100,
        "reverse": false
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Location: http://example.com/1/queries/DFAFC942-B821-4741-848F-2F9F57787A19
      Content-Type: application/json

      {
        "id": "DFAFC942-B821-4741-848F-2F9F57787A19"
      }

   :statuscode 200: no error

.. http:delete:: /(int:version)/queries

   Delete all query contexts.

   :statuscode 200: no error

.. http:get:: /(int:version)/queries/(uuid:UUID)

   Retrieve query statistics

   **Example response**:

   .. sourcecode:: http

      GET /1/queries/DFAFC942-B821-4741-848F-2F9F57787A19
      Host: example.com
      Accept: application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/json

      {
        "id": "DFAFC942-B821-4741-848F-2F9F57787A19",
        "timestamp": "2013-05-31 22:47:55.977661052+00:00",
        "state": "running",
        "events-returned": 10,
        "stores-touched": 1,
        "postings-touched": 20,
        "events-cached": 0
      }

   :statuscode 200: no error

.. http:post:: /(int:version)/queries/(uuid:UUID)

   Retrieve the next batch of events

   **Example response**:

   .. sourcecode:: http

      POST /1/queries/DFAFC942-B821-4741-848F-2F9F57787A19
      Host: example.com
      Accept: application/json

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/json

      {
        "sequence": 0,
        "events": [
          [
            UUID(event.id),
            {
              "message": { "text": "hello, world!" },
              "origin": { "hostname": "localhost", "address": "127.0.0.1" }
            }
          ],
        ],
        "finished": false
      }

   :statuscode 200: no error
