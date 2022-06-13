import sys

from parse_data.parse_block_data import load_bitcoin_blocks
sys.path.insert(1, './parse_data')

import create_database as cd
import parse_block_data as pbd

if __name__ == '__main__':
  # Load only create tables
  cd.create_tables()
  recent_height = pbd.get_most_recent_block_header()
  if recent_height != -1:
    pbd.load_block_headers('update')
  else:
    pbd.load_block_headers()
