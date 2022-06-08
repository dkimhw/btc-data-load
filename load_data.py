from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
import sqlite3
import pandas as pd

# rpc_user and rpc_password are set in the bitcoin.conf file
rpc_user = "username"
rpc_pass = "password"
rpc_host = "127.0.0.1" # if running locally then 127.0.0.1
rpc_connection = AuthServiceProxy(f"http://{rpc_user}:{rpc_pass}@{rpc_host}:8332", timeout=240)


num_blocks = rpc_connection.getblockcount()
chunk_size = 25000
# chunks = int(num_blocks / chunk_size)
chunks = 3

# batch support : print timestamps of blocks 0 to 99 in 2 RPC round-trips:
commands = [ [ "getblockhash", height] for height in range(100) ]
block_hashes = rpc_connection.batch_(commands)
blocks = rpc_connection.batch_([ [ "getblock", h, 2 ] for h in block_hashes ])
print(blocks[0])

# def initial_load():
#     with sqlite3.connect('bitcoin_blockchain.db') as conn:
#         for c in range(0, chunks+1):
#             block_stats = [rpc_connection.getblockstats(i) for i in range(c*chunk_size+1, (c+1)*chunk_size)]

#             df = pd.DataFrame(block_stats)
#             df['feerate_percentiles'] = df['feerate_percentiles'].astype(str)
#             df.to_sql('blockchain', conn, if_exists='append')
#         print(f'finished {(c+1)*chunk_size} record')

# initial_load()
