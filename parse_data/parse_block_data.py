
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

# rpc_user and rpc_password are set in the bitcoin.conf file
rpc_user = "username"
rpc_pass = "password"
rpc_host = "127.0.0.1" # if running locally then 127.0.0.1
rpc_connection = AuthServiceProxy(f"http://{rpc_user}:{rpc_pass}@{rpc_host}:8332", timeout=240)

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


def get_blocks (num_blocks):
  """
  Retrieves a block by blockhash
  """
  end_block = rpc_connection.getblockcount()
  start_block = end_block - num_blocks

  commands = [ [ "getblockhash", height] for height in range(start_block, end_block) ]
  block_hashes = rpc_connection.batch_(commands)
  blocks = rpc_connection.batch_([ [ "getblock", h, 2 ] for h in block_hashes ])

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
