# HDN
---

HDN is a multi threaded Hash Delivery network server. It runs on a network socket(default "127.0.0.1:7878") and accepts connections over TCP. It uses JSON API and accepts two types of queries: "store" and "load". It uses a DB file *hashdb.dat* for persistence.

**default network socket: 127.0.0.1:7878**

**query examples:**

- store a key: echo '{ "request_type": "store", "key": "some_key6", "hash": "0b672dd94fd3da6a8d404b66ee3f0c80" }' | nc -v 127.0.0.1 7878
- load a key: echo '{ "request_type": "load", "key": "some_key" }' | nc -v 127.0.0.1 7878
