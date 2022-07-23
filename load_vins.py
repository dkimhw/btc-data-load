import sys
from config import settings
from parse_data.parse_block_data import load_bitcoin_blocks
sys.path.insert(1, './parse_data')
import psycopg2
import parse_block_data as pbd

if __name__ == '__main__':
  # Load only create tables
  conn = psycopg2.connect(user = settings.username, password = settings.password , host= settings.host, port = settings.port, database = settings.database)
  pbd.load_transaction_ins(700000, 745525, conn)
