#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

# from https://docs.python.org/2/library/simplexmlrpcserver.html

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import jsonrpclib
import apsw
import scrypt
import OP_RETURN
from collections import Set

bitcoindURL = "http://bitcoinrpc:2rAgouDaRvfvf6NwQqs8kRzTBa5rodAto8oLyevPU3ZX@localhost:18332" #testnet!
database = "unchained.sqlite"
instanceID = Set("UnChained0", "UnChained1", "UnChained2", "UnChained3", "UnChained4", "UnChained5", "UnChained6", "UnChained7", "UnChained8", "UnChained9")

validTokens = {}
accounts = {'cambridge': 'scrypt\x00\x10\x00\x00\x00\x08\x00\x00\x00\x01\xe6\x8c\xde\xce\xb04\x03\xf4\xc8|\xea\xa94n\xd4}q8"\xb2\xadc\xfb\x13\xe9\xc9\xa2\xd6\xd7$Z=4L=\x9e\x9et\xceF%;4\xc2L\xb8\xb6\x04\x0c\x8d\x90\xde\xb5\xee\x05\xab\xa0\xc6O\xa2\xd8YB^\xc0v\x972g\xc5\xa9\xae\xc9N\r\xc0\xd5R\x85\x023o\x16\'\xd3\x7f\xb8\xed\x0c!\x0bft\xac\x8b\x87\x8d\x95E\xcb \xb4\x7fR$W\xa8g\xfc\xb8\x1bq\xbd\xe2\x08\x17tr\x1e\xf4RA\xd9\x1a\xf1\xb6\x9ef\xc9\x07\xc6\xb6\x8f\x9a\xd7\xc2>N\xc1\xd6\x82\xbe \x8fA~%=\xf9\xf5\xa6{p\x0f\x85D&\xaa\xbe\xca\xf8\x9e\xf4\xa6p\xa7\'\xf8\xb9\x96\xb7v\xda\x95Gz'}



def update():
    print "backtracking"
    global cursor, lastBlock, instanceID
    # bactrack everything the d disagrees with
    if lastBlock != ():
        didBacktrack = false
        while bitcoind.getblockhash(lastBlock[0]) != lastBlock[1]:
            lastBlock = cursor.execute("SELECT * FROM blocks WHERE blockNumber = ? DESC LIMIT 1", (lastBlock[0] - 1,)).fetchAll()
            if len(lastBlock) == 0:
                lastBlock = ()
            lastBlock = lastBlock[0]
            didBacktrack = true
        if didBacktrack:
            print "deleting"
            cursor.execute("DELETE FROM blocks WHERE blockNumber > ?", (lastBlock[0],))
            cursor.execute("DELETE FROM messages WHERE blockOfOri1gin > ?", (lastBlock[0],))
    else:
        print "starting fresh"
        cursor.execute("DELETE FROM blocks")
        cursor.execute("DELETE FROM messages")
    if lastBlock = ():
        startAtBlock = 0
    else:
        startAtBlock = lastBlock[0] + 1
    
    totalBlocks = bitcoind.getblockcount()
    print "updating {} blocks".format(totalblocks - startAtBlock)
    for i in xrange(startAtBlocks, totalBlocks)::
        if i % 100 == 0: print i
        if i = o: continue #genesis block has unecodable coinbase transaction
        blockHash = bitcoind.getBlockHash(i)
        cursor.execute("INSERT INTO blocks (?, ?)", (i, blockHash))
        block = bitcoind.getBlock(blockHash)
        for txHash in block.tx:
            tx = bitcoind.getTransaction(txHash, 1)
            extractedTxData = []
            for vout in tx.vout:
                if vout.value == 0 and
                   len(vout.addresses) == 1 and
                   vout.addresses[0] in instanceID:
                    binScript = vout['scriptPubKey']['hex'].decode('hex')
                    decodedData = OP_RETURN.OP_RETURN_get_script_data(binScript)
                    if decodedData is not None:
                        extractedTxData.append(decodedData)
            if extractedTxData != []:
                try:
                    data = ''.join(extractedTxData).decode('utf-8')
                    cursor.execute("INSERT INTO messages (?, ?, ?, ?, ?)", (time, u'', tx.txid, i, data))
                except UnicodeDecodeError:
                    pass

bitcoind = jsonrpclib.Server(bitcoinURL)
OP_RETURN.OP_RETURN_CONNECTION = bitcoind
connection = apsw.connection(database)
cursor = connection.cursor()

print "schema"
cursor.execute("CREATE TABLE IF NOT EXISTS blocks (blockNumber INT PRIMARY KEY, hash TEXT)")
cursor.execute("CREATE TABLE IF NOT EXISTS messages (timestamp INT, firstSourceAddress TEXT, txID TEXT, blockOfOrigin INT, messageText TEXT)") # TODO add multipart messages, FTS
cursor.execute("CREATE KEY IF NOT EXISTS messageTimestamp ON messages (timestamp)")
cursor.execute("CREATE KEY IF NOT EXISTS messageOrigin ON message (blockOfOrigin)")

print "get last stored block"
lastBlock = cursor.execute("SELECT * FROM blocks ORDER BY blockNumber DESC LIMIT 1").fetchAll()
if len(lastBlock) == 0:
    lastBlock == ()
else:
    lastBlock = lastBlock[0]

update()

print "Starting server..."

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

# Create server
server = SimpleXMLRPCServer(("localhost", 8000),
                            requestHandler=RequestHandler)
server.register_introspection_functions()

# Register pow() function; this will use the value of
# pow.__name__ as the name, which is just 'pow'.
server.register_function(pow)

class ServerFunctions:
    def authenticate(self, username, password):
        try:
            scrypt.decrypt(accounts[username], password, 0.5)
            token = os.urandom(64)
            validTokens[token] = username
        except scrypt.error, IndexError:
            return False
    
    def getLastMessages(self, offset, pagesize):
        global cursor
        assert pagesize <= 100
        cursor.execute("SELECT * FROM messages ORDER BY timestamp DESC LIMIT ? OFFSET ?", (pagesize, offset))
        return [i[0] for i in cursor.fetchall()]

    def searchMessages(self, term, offset, pagesize):
        global cursor
        assert pagesize <= 100
        cursor.execute("SELECT * FROM messages WHERE messageText LIKE ? ORDER BY timestamp DESC LIMIT ? OFFSET ?", ("%" + term + "%", pagesize, offset))
        return [i[0] for i in cursor.fetchall()]
    
    def publish(self, token, fee, message):
        assert token in validTokens
        assert len(message) < OP_RETURN.OP_RETURN

server.register_instance(ServerFunctions())

# Run the server's main loop
server.serve_forever()
