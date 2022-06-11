

import sys
sys.path.insert(1, '../')
from config import settings
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
import psycopg2
from datetime import datetime
import time
import json

# rpc_user and rpc_password are set in the bitcoin.conf file
rpc_user = "username"
rpc_pass = "password"
rpc_host = "127.0.0.1" # if running locally then 127.0.0.1
rpc_connection = AuthServiceProxy(f"http://{rpc_user}:{rpc_pass}@{rpc_host}:8332", timeout=240)

def load_bitcoin_data (num_blocks):
    end_block = rpc_connection.getblockcount()
    start_block = end_block - num_blocks

    commands = [ [ "getblockhash", height] for height in range(start_block, end_block) ]
    block_hashes = rpc_connection.batch_(commands)
    blocks = rpc_connection.batch_([ [ "getblock", h, 2 ] for h in block_hashes ])

    try:
      connection = psycopg2.connect(user = settings.username, password = settings.password , host= settings.host, port = settings.port, database = settings.database)
    except (Exception, psycopg2.Error) as error:
      print("Failed to insert record into blocks table", error)

    for block in blocks:
      # Process block metadata
      parse_bitcoin_block(block, connection)

      # Process transactions
      parse_bitcoin_txs(block, connection)

    if connection:
        connection.close()
        print("PostgreSQL connection is closed")


#############################
# Parse Block
#############################

def parse_bitcoin_block(block, conn):
  parsed_block_data = (block['hash'], block['height'], block['version'], block['previousblockhash'], block['merkleroot']
  , block['time'], datetime.fromtimestamp(block['time']).strftime('%Y-%m-%d %H:%M:%S'), block['bits'], block['nonce']
  ,  block['size'], block['weight'], block['nTx'], block['confirmations'])

  insert_block(parsed_block_data, conn)

def insert_block (block, conn):
  try:
    cursor = conn.cursor()

    postgres_insert_query = """
    INSERT INTO bitcoin.blocks (hash, height, version, prevhash, merkleroot, time, timestamp
    , bits, nonce, size, weight, num_tx, confirmations)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    record_to_insert = block
    cursor.execute(postgres_insert_query, record_to_insert)

    conn.commit()
    count = cursor.rowcount
    print(count, "Record inserted successfully into blocks table")

  except (Exception, psycopg2.Error) as error:
    print("Failed to insert record into blocks table", error)

  finally:
      if cursor:
          cursor.close()


#############################
# Parse Transactions Metadata
#############################

def parse_bitcoin_txs (block, conn):
  block_hash = block['hash']
  transactions = block['tx']

  for transaction in transactions:
    # Parse fees; no fee on coinbase transaction
    if 'fee' in transaction.keys():
      fee = str(transaction['fee'])
    else:
      fee = '0.00000000'

    # Parse transaction metadata
    parsed_transaction = (transaction['txid'], transaction['version'], transaction['size'], transaction['vsize']
    , transaction['weight'], transaction['locktime'], fee, block_hash)
    insert_tx(parsed_transaction, conn)

    tx_id = transaction['txid']

    # Parse vin
    vin = transaction['vin']
    parse_tx_ins(vin, tx_id, conn)

    # Parse vout
    vout = transaction['vout']
    parse_tx_outs(vout, tx_id, conn)

def insert_tx (tx, conn):
  try:
    cursor = conn.cursor()

    postgres_insert_query = """ INSERT INTO bitcoin.txs (tx_id, version, size, vsize, weight, locktime, fee, block_hash)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    cursor.execute(postgres_insert_query, tx)

    conn.commit()
    count = cursor.rowcount
    print(count, "Record inserted successfully into table")
  except (Exception, psycopg2.Error) as error:
    print("Failed to insert record into table", error)

  finally:
      if cursor:
          cursor.close()


#############################
# Parse Transaction Ins
#############################

def parse_tx_ins (t_ins, tx_id, conn):
  for t_in in t_ins:
    # {'coinbase': '045a0c011c026d17', 'sequence': 4294967295}
    # {'txid': 'd561a3594fcf97dd1a1abe7a1eda15c8e335aaaecf97f959de0595298d87c6d5', 'vout': 1, 'scriptSig': {'asm': '3045022100fc81c1d7a6abac1e35d555204eb81825f0b1fcab6038d5b52c15c1547c66cfd6022055746f7ecde0c22a919a0a57ca134d47c47e0a375d6f043e3ee97f6108861c1d[ALL] 045f0d6e6e49cc01c7a1535c2fe67e9ebc85849a9a45f013ed3cc7db7e8b54276a86da044b703173ea812e5297c39bc28a0393414fc15614f08b7622d35bda466c', 'hex': '483045022100fc81c1d7a6abac1e35d555204eb81825f0b1fcab6038d5b52c15c1547c66cfd6022055746f7ecde0c22a919a0a57ca134d47c47e0a375d6f043e3ee97f6108861c1d0141045f0d6e6e49cc01c7a1535c2fe67e9ebc85849a9a45f013ed3cc7db7e8b54276a86da044b703173ea812e5297c39bc28a0393414fc15614f08b7622d35bda466c'}, 'sequence': 4294967295}
    if 'coinbase' in t_in.keys():
      continue
    else :
      current_transaction_in = (t_in['txid'], t_in['vout'], json.dumps(t_in['scriptSig']), t_in['sequence'], tx_id)
    insert_tx_in(current_transaction_in, conn)

def insert_tx_in (t_in, conn):
  try:
    cursor = conn.cursor()

    postgres_insert_query = """ INSERT INTO bitcoin.tx_ins (prev_tx_id, prev_n, scriptsig, sequence, curr_tx_id)
    VALUES (%s, %s, %s, %s, %s)"""
    cursor.execute(postgres_insert_query, t_in)

    conn.commit()
    count = cursor.rowcount
    print(count, "Record inserted successfully into tx_ins")

  except (Exception, psycopg2.Error) as error:
    print("Failed to insert record into tx_ins", error)

  finally:
      # closing database connection.
      if cursor:
        cursor.close()


#############################
# Parse Transaction Outs
#############################

def parse_tx_outs (t_outs, tx_id, conn):
  for t_out in t_outs:
    if 'address' in t_out['scriptPubKey'].keys():
      address = t_out['scriptPubKey']['address']
    else:
      address = None

    transactions_out = (tx_id, t_out['n'], str(t_out['value']), json.dumps(t_out['scriptPubKey']), address)
    insert_tx_out(transactions_out, conn)

def insert_tx_out (t_out, conn):
  try:
    cursor = conn.cursor()

    postgres_insert_query = """ INSERT INTO bitcoin.tx_outs (tx_id, n, value, scriptpubkey, address)
    VALUES (%s, %s, %s, %s, %s)"""
    cursor.execute(postgres_insert_query, t_out)

    conn.commit()
    count = cursor.rowcount
    print(count, "Record inserted successfully into tx_outs")

  except (Exception, psycopg2.Error) as error:
    print("Failed to insert record into tx_outs", error)

  finally:
      if cursor:
          cursor.close()
