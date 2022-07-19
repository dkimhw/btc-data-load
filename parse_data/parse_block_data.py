
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
import requests
import time

# rpc_user and rpc_password are set in the bitcoin.conf file
rpc_user = "username"
rpc_pass = "password"
rpc_host = "127.0.0.1" # if running locally then 127.0.0.1
rpc_connection = AuthServiceProxy(f"http://{rpc_user}:{rpc_pass}@{rpc_host}:8332", timeout=240)


def get_most_recent_coinbase_tx():
  try:
    # connect to the PostgreSQL server
    conn = psycopg2.connect(user = settings.username, password = settings.password , host= settings.host, port = settings.port, database = settings.database)
    cur = conn.cursor()
    cur.execute('SELECT MAX(b.height) AS max_height FROM bitcoin.coinbase_txs AS ct INNER JOIN bitcoin.block_headers AS b ON ct.block_hash = b.hash WHERE b.height <= 500000')
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


def parse_coinbase_txs_data_in_chunks_esplora2(start_block):
  end_block = get_most_recent_block_header()

  chunk_size = 50
  chunks = int((end_block - start_block) / chunk_size)
  print("Num of iterations: ", chunks)

  for c in range(0, chunks + 1):
    if c == 0:
      start = c * chunk_size + 1 + start_block
    else:
      start = c * chunk_size + start_block

    if (c + 1) * chunk_size + start_block >= end_block:
      end = end_block + 1
    else:
      end = (c + 1) * chunk_size + start_block

    print(f'Processing blocks between {start} and {end - 1}')
    load_coinbase_txs_data_with_esplora(start, end)

def parse_coinbase_txs_data_in_chunks_esplora(type = 'all'):
  end_block = 500000 # get_most_recent_block_header()

  if type == 'update':
    recent_h = get_most_recent_coinbase_tx()
  else:
    recent_h = 0

  chunk_size = 250
  chunks = int((end_block - recent_h) / chunk_size)
  print("Num of iterations: ", chunks)

  for c in range(0, chunks + 1):
    if type == 'update' and c == 0:
      start = c * chunk_size + 1 + recent_h
    else:
      start = c * chunk_size + recent_h

    if (c + 1) * chunk_size + recent_h >= end_block:
      end = end_block + 1
    else:
      end = (c + 1) * chunk_size + recent_h

    print(f'Processing blocks between {start} and {end - 1}')
    load_coinbase_txs_data_with_esplora(start, end)

def load_coinbase_txs_data_with_esplora (sheight, eheight):
  """
  Get block header data & coinbase txs and insert the data into postgres database
  """
  coinbase_txs_to_insert = []
  print(time.time())
  for height in range(sheight, eheight):
    # /block-height/:height
    bhash_response = requests.get(f'https://blockstream.info/api/block-height/{height}')
    bhash = str(bhash_response.text)
    # print('Bhash', bhash)

    tx_id_response = requests.get(f'https://blockstream.info/api/block/{bhash}/txid/0')
    tx_id = str(tx_id_response.text)
    # print(tx_id)

    # Get coinbase tx
    tx_response = requests.get(f'https://blockstream.info/api/tx/{tx_id}').text
    tx = json.loads(tx_response)
    vout = json.dumps(tx['vout'])

    parsed_tx = {
      'txid': tx['txid'],
      'block_hash': bhash,
      'version': tx['version'],
      'locktime': tx['locktime'],
      'size': tx['size'],
      'weight': tx['weight'],
      'fee': tx['fee'],
      'outputs': vout
    }
    coinbase_txs_to_insert.append(parsed_tx)

  print(time.time())
  engine = create_engine(f'postgresql://{settings.username}:{settings.password}@{settings.host}:{settings.port}/{settings.database}')
  coinbase_txs_to_insert = pd.DataFrame(coinbase_txs_to_insert)
  print(coinbase_txs_to_insert)
  coinbase_txs_to_insert.to_sql(name='coinbase_txs', con = engine, schema='bitcoin', if_exists='append', index=False, method=psql_insert_copy)

def load_bitcoin_data (num_blocks):
  """
  Load bitcoin data in chunks of five
  """
  hashes = get_hashes(num_blocks)
  chunk_size = 5

  idx = 0
  while idx < len(hashes):
    chunk_hashes = []

    # check if it's out of range
    if (chunk_size + idx >= len(hashes)):
      loop_range = len(hashes)
    else:
      loop_range = idx + chunk_size

    for hash in range(idx, loop_range):
      chunk_hashes.append(hashes[hash])

    print('Processing: ', chunk_hashes)
    blocks = get_blocks_by_hashes(chunk_hashes)
    load_bitcoin_blocks(blocks)
    load_bitcoin_txs(blocks)
    idx += chunk_size

  print('Finished loading bitcoin data')

def get_hashes (num_blocks):
  """
  Retrieve hashes by desired blocks
  """
  end_block = rpc_connection.getblockcount()
  start_block = end_block - num_blocks

  commands = [ [ "getblockhash", height] for height in range(start_block, end_block) ]
  block_hashes = rpc_connection.batch_(commands)
  return block_hashes

def get_blocks_by_hashes (hashes):
  """
  Retrieve blocks by given hashes
  """
  blocks = rpc_connection.batch_([ [ "getblock", h, 2 ] for h in hashes ])
  return blocks

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


def load_bitcoin_txs (blocks):
    """
    Bulk uploads bitcoin data
    """
    transactions_to_insert = []
    tx_ins_to_insert = []
    tx_outs_to_insert = []
    for block in blocks:
      # Process block metadata
      transactions = block['tx']
      for transaction in transactions:
        #Parse fees; no fee on coinbase transaction
        if 'fee' in transaction.keys():
          fee = str(transaction['fee'])
        else:
          fee = '0.00000000'

        block_hash = block['hash']

        parsed_transaction = {
          'tx_id': transaction['txid'],
          'version': transaction['version'],
          'size': transaction['size'],
          'vsize': transaction['vsize'],
          'weight': transaction['weight'],
          'locktime': transaction['locktime'],
          'fee': fee,
          'block_hash': block_hash
        }

        transactions_to_insert.append(parsed_transaction)

        # Transaction Details
        tx_ins_to_insert += parse_tx_ins(transaction['vin'], transaction['txid'])
        tx_outs_to_insert += parse_tx_outs(transaction['vout'], transaction['txid'])

    tx = pd.DataFrame(transactions_to_insert)
    engine = create_engine(f'postgresql://{settings.username}:{settings.password}@{settings.host}:{settings.port}/{settings.database}')

    # Bulk insert TX metadata
    tx.to_sql(name='txs', con = engine, schema='bitcoin', if_exists='append', index=False, method=psql_insert_copy)

    # Bulk insert TX IN
    tx_in = pd.DataFrame(tx_ins_to_insert)
    tx_in.to_sql(name='tx_ins', con = engine, schema='bitcoin', if_exists='append', index=False, method=psql_insert_copy)

    # Bulk insert TX OUT
    tx_out = pd.DataFrame(tx_outs_to_insert)
    tx_out.to_sql(name='tx_outs', con = engine, schema='bitcoin', if_exists='append', index=False, method=psql_insert_copy)


def parse_tx_ins (t_ins, tx_id):
  to_insert = []
  for t_in in t_ins:
    if 'coinbase' in t_in.keys():
      continue
    else :
      parsed_transaction = {
        'prev_tx_id': t_in['txid'],
        'prev_n': t_in['vout'],
        'scriptsig': json.dumps(t_in['scriptSig']),
        'sequence': t_in['sequence'],
        'curr_tx_id': tx_id
      }
      to_insert.append(parsed_transaction)

  return to_insert

def parse_tx_outs (t_outs, tx_id):
  to_insert = []
  for t_out in t_outs:
    if 'address' in t_out['scriptPubKey'].keys():
      address = t_out['scriptPubKey']['address']
    else:
      address = None

    parsed_transaction = {
      'tx_id': tx_id,
      'n': t_out['n'],
      'value': str(t_out['value']),
      'scriptpubkey': json.dumps(t_out['scriptPubKey']),
      'address': address
    }
    to_insert.append(parsed_transaction)
  return to_insert

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data
    """
    # Gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)
