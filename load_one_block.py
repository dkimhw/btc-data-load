

import sys

sys.path.insert(1, './parse_data')

import create_database as cd
import parse_block_data as pbd

if __name__ == '__main__':
  # Load only create tables
  pbd.load_one_block(0)
