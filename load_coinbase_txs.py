import sys
sys.path.insert(1, './parse_data')

import create_database as cd
import parse_block_data as pbd

if __name__ == '__main__':
  # Load only create tables
  cd.create_tables()
  recent_height = pbd.get_most_recent_coinbase_tx()
  if recent_height != -1:
    pbd.parse_coinbase_txs_data_in_chunks_esplora('update')
  else:
    pbd.parse_coinbase_txs_data_in_chunks_esplora()
