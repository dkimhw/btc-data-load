import sys
sys.path.insert(1, './parse_data')

import create_database as cd
import parse_block_data as pbd

if __name__ == '__main__':
  # Load only create tables
  # 91795 and 91879
  pbd.parse_coinbase_txs_data_in_chunks_esplora2(700000)
