
import sys
sys.path.insert(1, '../')
from config import settings
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from io import StringIO
import csv
from datetime import datetime
import pandas as pd
import json
from sqlalchemy import create_engine
import psycopg2
from psql_insert_method import psql_insert_copy

# rpc_user and rpc_password are set in the bitcoin.conf file
rpc_user = "username"
rpc_pass = "password"
rpc_host = "127.0.0.1" # if running locally then 127.0.0.1
rpc_connection = AuthServiceProxy(f"http://{rpc_user}:{rpc_pass}@{rpc_host}:8332", timeout=240)

def get_most_recent_coinbase_tx():
  """
  Finds the most recent coinbase transaction in connected database
  """
  try:
    # connect to the PostgreSQL server
    conn = psycopg2.connect(user = settings.username, password = settings.password , host= settings.host, port = settings.port, database = settings.database)
    cur = conn.cursor()
    cur.execute('SELECT MAX(b.height) AS max_height FROM bitcoin.coinbase_txs AS ct INNER JOIN bitcoin.block_headers AS b ON ct.block_hash = b.hash')
    h = cur.fetchone()

    if h[0]:
      return h[0]
    else:
      return -1
  except (Exception, psycopg2.DatabaseError) as error:
      print(error)
  finally:
      if conn is not None:
          conn.close()

def get_most_recent_block_header():
  """
  Finds the most recent blockheader in connected database
  """
  try:
    # connect to the PostgreSQL server
    conn = psycopg2.connect(user = settings.username, password = settings.password , host= settings.host, port = settings.port, database = settings.database)
    cur = conn.cursor()
    cur.execute('SELECT MAX(height) AS max_height FROM bitcoin.block_headers')
    h = cur.fetchone()

    if h[0]:
      return h[0]
    else:
      return -1
  except (Exception, psycopg2.DatabaseError) as error:
      print(error)
  finally:
      if conn is not None:
          conn.close()

def load_block_headers (type = 'all'):
  """
  Extracts block data in chunks & uploads it to the connected database
  """
  end_block = rpc_connection.getblockcount()

  if type == 'update':
    recent_h = get_most_recent_block_header()
  else:
    recent_h = 0

  chunk_size = 1000
  chunks = int((end_block - recent_h) / chunk_size)

  for c in range(0, chunks + 1):
    blocks_to_insert = []
    if type == 'update' and c == 0:
      start = c * chunk_size + 1 + recent_h
    else:
      start = c * chunk_size + recent_h

    if (c + 1) * chunk_size + recent_h >= end_block:
      end = end_block + 1
    else:
      end = (c + 1) * chunk_size + recent_h

    print(f'Processing blocks between {start} and {end - 1}')
    commands = [ [ "getblockhash", height] for height in range(start, end) ]
    hashes = rpc_connection.batch_(commands)
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
        'prev_block_hash': previousblockhash,
        'merkle_root': block['merkleroot'],
        'timestamp': datetime.fromtimestamp(block['time']).strftime('%Y-%m-%d %H:%M:%S'),
        'median_time': block['mediantime'],
        'bits': block['bits'],
        'nonce': block['nonce'],
        'size': block['size'],
        'weight': block['weight'],
        'num_tx': block['nTx'],
        'difficulty': block['difficulty'],
        'confirmations': block['confirmations']
      }
      blocks_to_insert.append(parsed_block)

    engine = create_engine(f'postgresql://{settings.username}:{settings.password}@{settings.host}:{settings.port}/{settings.database}')
    blocks = pd.DataFrame(blocks_to_insert)
    blocks.to_sql(name='block_headers', con = engine, schema='bitcoin', if_exists='append', index=False, method=psql_insert_copy)

def load_transaction_ins (startUpload, endUpload, conn):
  end_block = endUpload

  chunk_size = 25
  chunks = int((end_block - startUpload) / chunk_size)

  for c in range(0, chunks + 1):
  # for c in range(0, 1):
    data_to_insert = []
    if type == 'update' and c == 0:
      start = c * chunk_size + 1 + startUpload
    else:
      start = c * chunk_size + startUpload

    if (c + 1) * chunk_size + startUpload >= end_block:
      end = end_block + 1
    else:
      end = (c + 1) * chunk_size + startUpload

    print(f'Processing blocks between {start} and {end - 1}')
    commands = [ [ "getblockhash", height] for height in range(start, end) ]
    hashes = rpc_connection.batch_(commands)
    blocks = rpc_connection.batch_([ [ "getblock", h, 2 ] for h in hashes])

    for block in blocks:
      add_vins = []
      for tx in block['tx']:
        for vin in tx['vin']:
          if 'txid' in vin:
            add_vins.append({vin['txid']: tx['txid']})

      if len(add_vins) == 0:
        add_vins = None
      else:
        add_vins = json.dumps(add_vins)
      # connect to the PostgreSQL server
      cur = conn.cursor()
      # create table one by one
      cur.execute("insert into bitcoin.block_vins values (%s, %s);", (block['hash'] , add_vins))
      conn.commit()

def load_one_block (block_h):
  blocks_to_insert = []

  commands = [ [ "getblockhash", block_h] ]
  hashes = rpc_connection.batch_(commands)

  block = rpc_connection.batch_([ [ "getblock", h, 1 ] for h in hashes])
  block = block[0]

  if 'previousblockhash' in block.keys():
    previousblockhash = block['previousblockhash']
  else:
    previousblockhash = None

  parsed_block = {
    'hash': block['hash'],
    'height': block['height'],
    'version': block['version'],
    'prev_block_hash': previousblockhash,
    'merkle_root': block['merkleroot'],
    'timestamp': datetime.fromtimestamp(block['time']).strftime('%Y-%m-%d %H:%M:%S'),
    'bits': block['bits'],
    'nonce': block['nonce'],
    'size': block['size'],
    'weight': block['weight'],
    'num_tx': block['nTx'],
    'confirmations': block['confirmations']
  }
  blocks_to_insert.append(parsed_block)
  engine = create_engine(f'postgresql://{settings.username}:{settings.password}@{settings.host}:{settings.port}/{settings.database}')
  blocks = pd.DataFrame(blocks_to_insert)
  blocks.to_sql(name='blocks', con = engine, schema='bitcoin', if_exists='append', index=False, method=psql_insert_copy)

def load_coinbase_txs (type = 'all'):
  end_block = get_most_recent_block_header()

  if type == 'update':
    recent_h = get_most_recent_coinbase_tx()
    if recent_h == -1:
      recent_h = 0
  else:
    recent_h = 0

  chunk_size = 1000
  chunks = int((end_block - recent_h) / chunk_size)

  for c in range(0, chunks + 1):
    coinbase_txs_to_insert = []

    if type == 'update' and c == 0:
      start = c * chunk_size + 1 + recent_h
    else:
      start = c * chunk_size + recent_h

    if (c + 1) * chunk_size + recent_h >= end_block:
      end = end_block + 1
    else:
      end = (c + 1) * chunk_size + recent_h

    print(f'Processing blocks between {start} and {end - 1}')
    commands = [ [ "getblockhash", height] for height in range(start, end) ]
    hashes = rpc_connection.batch_(commands)
    blocks = rpc_connection.batch_([ [ "getblock", h, 1 ] for h in hashes])

    for block in blocks:
      first_tx = block['tx']
      parsed_coinbase_tx = {
        'tx_id': first_tx[0]
        , 'block_hash': block['hash']
      }
      coinbase_txs_to_insert.append(parsed_coinbase_tx)
    engine = create_engine(f'postgresql://{settings.username}:{settings.password}@{settings.host}:{settings.port}/{settings.database}')
    coinbase_txs = pd.DataFrame(coinbase_txs_to_insert)
    coinbase_txs.to_sql(name='coinbase_txs', con = engine, schema='bitcoin', if_exists='append', index=False, method=psql_insert_copy)

def load_bitcoin_blocks (blocks):
  """
  Load bitcoin block data
  """
  engine = create_engine(f'postgresql://{settings.username}:{settings.password}@{settings.host}:{settings.port}/{settings.database}')

  blocks_to_insert = []
  for block in blocks:
    parsed_block = {
      'hash': block['hash'],
      'height': block['height'],
      'version': block['version'],
      'prev_hash': block['previousblockhash'],
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

    blocks_to_insert.append(parsed_block)

  df = pd.DataFrame(blocks_to_insert)
  df.to_sql(name='blocks', con = engine, schema='bitcoin', if_exists='append', index=False, method=psql_insert_copy)
