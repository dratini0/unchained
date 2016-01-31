#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

# from https://docs.python.org/2/library/simplexmlrpcserver.html

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import apsw
from collections import Set
import time
import scrypt
import os
import random
import threading

database = "unchained_sample.sqlite"
#instanceID = Set("UnChained0", "UnChained1", "UnChained2", "UnChained3", "UnChained4", "UnChained5", "UnChained6", "UnChained7", "UnChained8", "UnChained9")

validTokens = {}
accounts = {'cambridge': 'scrypt\x00\x10\x00\x00\x00\x08\x00\x00\x00\x01\xe6\x8c\xde\xce\xb04\x03\xf4\xc8|\xea\xa94n\xd4}q8"\xb2\xadc\xfb\x13\xe9\xc9\xa2\xd6\xd7$Z=4L=\x9e\x9et\xceF%;4\xc2L\xb8\xb6\x04\x0c\x8d\x90\xde\xb5\xee\x05\xab\xa0\xc6O\xa2\xd8YB^\xc0v\x972g\xc5\xa9\xae\xc9N\r\xc0\xd5R\x85\x023o\x16\'\xd3\x7f\xb8\xed\x0c!\x0bft\xac\x8b\x87\x8d\x95E\xcb \xb4\x7fR$W\xa8g\xfc\xb8\x1bq\xbd\xe2\x08\x17tr\x1e\xf4RA\xd9\x1a\xf1\xb6\x9ef\xc9\x07\xc6\xb6\x8f\x9a\xd7\xc2>N\xc1\xd6\x82\xbe \x8fA~%=\xf9\xf5\xa6{p\x0f\x85D&\xaa\xbe\xca\xf8\x9e\xf4\xa6p\xa7\'\xf8\xb9\x96\xb7v\xda\x95Gz'}

connection = apsw.Connection(database)
cursor = connection.cursor()

print "schema"
cursor.execute("CREATE TABLE IF NOT EXISTS blocks (blockNumber INT PRIMARY KEY, hash TEXT)")
cursor.execute("CREATE TABLE IF NOT EXISTS messages (timestamp INT, firstSourceAddress TEXT, txID TEXT, blockOfOrigin INT, messageText TEXT)") # TODO add multipart messages, FTS
cursor.execute("CREATE INDEX IF NOT EXISTS messageTimestamp ON messages (timestamp)")
cursor.execute("CREATE INDEX IF NOT EXISTS messageOrigin ON messages (blockOfOrigin)")

print "Starting server..."

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

# Create server
server = SimpleXMLRPCServer(("localhost", 8000),
                            requestHandler=RequestHandler)
server.register_introspection_functions()

def timedPost(message):
    global connection
    cursor2 = connection.cursor()
    time.sleep(random.randrange(30, 90))
    cursor2.execute("INSERT INTO messages VALUES (?, ?, ?, ?, ?)", (int(time.time()), u'', u'', 600000, message))

class ServerFunctions:
    def authenticate(self, username, password):
        try:
            scrypt.decrypt(accounts[username], password, 0.5)
            token = os.urandom(64).encode('base64')
            validTokens[token] = username
            return token
        except scrypt.error, IndexError:
            return False
    
    def getLastMessages(self, offset, pagesize):
        global cursor
        assert pagesize <= 100
        cursor.execute("SELECT * FROM messages ORDER BY timestamp DESC LIMIT ? OFFSET ?", (pagesize, offset))
        return cursor.fetchall()

    def searchMessages(self, term, offset, pagesize):
        global cursor
        assert pagesize <= 100
        cursor.execute("SELECT * FROM messages WHERE messageText LIKE ? ORDER BY timestamp DESC LIMIT ? OFFSET ?", ("%" + term + "%", pagesize, offset))
        return cursor.fetchall()
    
    def publish(self, token, fee, message):
        assert token in validTokens
        assert len(message) < 400
        threading.Thread(target=timedPost, args=(message,)).start() 
        return True
        
    def getBalance(self, token):
        assert token in validTokens
        return 0.0012
    
    def getAddress(self, token):
        assert token in validTokens
        return "mfccwptKN73gBWARuNJbGh65RKqsZ7ST85"


server.register_instance(ServerFunctions())

# Run the server's main loop
server.serve_forever()
