
# rpc_connection = AuthServiceProxy("http://%s:%s@127.0.0.1:8332"%("username", "password"))
# num_blocks = rpc_connection.getblockcount()

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

# rpc_user and rpc_password are set in the bitcoin.conf file
rpc_user = "username"
rpc_pass = "password"
rpc_host = "127.0.0.1" # if running locally then 127.0.0.1
rpc_connection = AuthServiceProxy(f"http://{rpc_user}:{rpc_pass}@{rpc_host}:8332", timeout=240)


num_blocks = rpc_connection.getblockcount()
print(num_blocks)
