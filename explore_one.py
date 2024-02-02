
import sys
# insert at 1, 0 is the script path (or '' in REPL)
sys.path.insert(1, '../')
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
import psycopg2
from datetime import datetime
import time
import re

# rpc_user and rpc_password are set in the bitcoin.conf file
rpc_user = "username"
rpc_pass = "password"
rpc_host = "127.0.0.1" # if running locally then 127.0.0.1
rpc_connection = AuthServiceProxy(f"http://{rpc_user}:{rpc_pass}@{rpc_host}:8332", timeout=240)

end_block = rpc_connection.getblockcount()
start_block = end_block - 1

def load_block_headers (type = 'all'):
  end_block = rpc_connection.getblockcount()
  recent_h = 0

  chunk_size = 1000
  chunks = int((end_block - recent_h) / chunk_size)

  for c in range(0, 3):
    start = c * chunk_size + recent_h

    if (c + 1) * chunk_size + recent_h >= end_block:
      end = end_block
    else:
      end = (c + 1) * chunk_size + recent_h

    commands = [ [ "getblockhash", height] for height in range(start, end) ]
    hashes = rpc_connection.batch_(commands)

    print(f'Processing blocks between {start} and {end - 1}')
    blocks = rpc_connection.batch_([ [ "getblock", h, 1 ] for h in hashes])

    for block in blocks:
      if 'previousblockhash' in block.keys():
        previousblockhash = block['previousblockhash']
      else:
        previousblockhash = None
      parsed_block = {
        'hash': block['hash'],
        'height': block['height'],
        'version': block['version'],
        'prev_hash': previousblockhash,
        'merkleroot': block['merkleroot'],
        'time': block['time'],
        'timestamp': datetime.fromtimestamp(block['time']).strftime('%Y-%m-%d %H:%M:%S'),
        'bits': block['bits'],
        'nonce': block['nonce'],
        'size': block['size'],
        'weight': block['weight'],
        'num_tx': block['nTx'],
        'confirmations': block['confirmations']
      }
      print(parsed_block)

load_block_headers()
