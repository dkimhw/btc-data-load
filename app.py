import sys
# insert at 1, 0 is the script path (or '' in REPL)
sys.path.insert(1, './parse_data')

import create_database as cd
import parse_block_data as pbd


if __name__ == '__main__':
    cd.create_tables()
    pbd.load_bitcoin_data(1)