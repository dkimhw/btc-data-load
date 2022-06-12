import sys

from parse_data.parse_block_data import load_bitcoin_blocks
# insert at 1, 0 is the script path (or '' in REPL)
sys.path.insert(1, './parse_data')

import create_database as cd
import parse_block_data as pbd

if __name__ == '__main__':
  if sys.argv[1]:
    num_blocks = int(sys.argv[1])
  else:
    num_blocks = 10

  cd.create_tables()
  pbd.load_bitcoin_data(num_blocks)
