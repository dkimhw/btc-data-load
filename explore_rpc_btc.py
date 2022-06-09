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
# commands = [ [ "getblockhash", height] for height in range(100) ]
# block_hashes = rpc_connection.batch_(commands)
# print(block_hashes)

# print("\n")

# blocks = rpc_connection.batch_([ [ "getblock", h, 2 ] for h in block_hashes ])
# print(blocks[0])


#000000000000000000049ea0bd8e5752e963ea36696365a5562349c043ad98bf

# 720000
blocks = rpc_connection.batch_([ [ "getblock", '0000000000eb357d4c6fef6ad9a6fade126985ad36042a99cf215a4454545977', 2 ] ])
print(blocks)
# def initial_load():
#     with sqlite3.connect('bitcoin_blockchain.db') as conn:
#         for c in range(0, chunks+1):
#             block_stats = [rpc_connection.getblockstats(i) for i in range(c*chunk_size+1, (c+1)*chunk_size)]

#             df = pd.DataFrame(block_stats)
#             df['feerate_percentiles'] = df['feerate_percentiles'].astype(str)
#             df.to_sql('blockchain', conn, if_exists='append')
#         print(f'finished {(c+1)*chunk_size} record')

# initial_load()


block72000 = [{'hash': '0000000000eb357d4c6fef6ad9a6fade126985ad36042a99cf215a4454545977', 'confirmations': 668079, 'height': 72000, 'version': 1, 'versionHex': '00000001', 'merkleroot': 'f411d31339dfb78fb190e48c07aa8a66122f7814fbc02a0cb40118eb9c9d6bad', 'time': 1280824783, 'mediantime': 1280823515, 'nonce': 132516746, 'bits': '1c010c5a', 'difficulty': Decimal('244.2132230923753'), 'chainwork': '000000000000000000000000000000000000000000000000001165816b6b20ea', 'nTx': 3, 'previousblockhash': '000000000071932f27aec686d9210248065211cc2f421306f4149d67501a0551', 'nextblockhash': '00000000002ec11f3242aeba3465cc0a350722d355d42a6415222c82cb336bfd', 'strippedsize': 731, 'size': 731, 'weight': 2924, 'tx': [{'txid': '71dfefee83ea09b32ca8db10011fcb15c737ea42828435f23275b513ea9d2217', 'hash': '71dfefee83ea09b32ca8db10011fcb15c737ea42828435f23275b513ea9d2217', 'version': 1, 'size': 135, 'vsize': 135, 'weight': 540, 'locktime': 0, 'vin': [{'coinbase': '045a0c011c026d17', 'sequence': 4294967295}], 'vout': [{'value': Decimal('50.00000000'), 'n': 0, 'scriptPubKey': {'asm': '04e6565c3c615b6f5d1033c6d32c3ad0307794a0cc7e551c792bec6f9bfe481777e47ac745136798663b8ddb57a76744106293453b89dad1e411485281d28b7a08 OP_CHECKSIG', 'hex': '4104e6565c3c615b6f5d1033c6d32c3ad0307794a0cc7e551c792bec6f9bfe481777e47ac745136798663b8ddb57a76744106293453b89dad1e411485281d28b7a08ac', 'type': 'pubkey'}}], 'hex': '01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff08045a0c011c026d17ffffffff0100f2052a01000000434104e6565c3c615b6f5d1033c6d32c3ad0307794a0cc7e551c792bec6f9bfe481777e47ac745136798663b8ddb57a76744106293453b89dad1e411485281d28b7a08ac00000000'}, {'txid': 'ce644c1e8bcc39a1d42daa52bb156d4d8e9bfceec7289ed7eef9d3567c569abd', 'hash': 'ce644c1e8bcc39a1d42daa52bb156d4d8e9bfceec7289ed7eef9d3567c569abd', 'version': 1, 'size': 258, 'vsize': 258, 'weight': 1032, 'locktime': 0, 'vin': [{'txid': 'd561a3594fcf97dd1a1abe7a1eda15c8e335aaaecf97f959de0595298d87c6d5', 'vout': 1, 'scriptSig': {'asm': '3045022100fc81c1d7a6abac1e35d555204eb81825f0b1fcab6038d5b52c15c1547c66cfd6022055746f7ecde0c22a919a0a57ca134d47c47e0a375d6f043e3ee97f6108861c1d[ALL] 045f0d6e6e49cc01c7a1535c2fe67e9ebc85849a9a45f013ed3cc7db7e8b54276a86da044b703173ea812e5297c39bc28a0393414fc15614f08b7622d35bda466c', 'hex': '483045022100fc81c1d7a6abac1e35d555204eb81825f0b1fcab6038d5b52c15c1547c66cfd6022055746f7ecde0c22a919a0a57ca134d47c47e0a375d6f043e3ee97f6108861c1d0141045f0d6e6e49cc01c7a1535c2fe67e9ebc85849a9a45f013ed3cc7db7e8b54276a86da044b703173ea812e5297c39bc28a0393414fc15614f08b7622d35bda466c'}, 'sequence': 4294967295}], 'vout': [{'value': Decimal('0.33000000'), 'n': 0, 'scriptPubKey': {'asm': 'OP_DUP OP_HASH160 496446211f5f7a3325f5aeabc64b65ccc44cf360 OP_EQUALVERIFY OP_CHECKSIG', 'hex': '76a914496446211f5f7a3325f5aeabc64b65ccc44cf36088ac', 'address': '17h4TAEwZ9DZ1NpSfksrer84hr92TB5bwx', 'type': 'pubkeyhash'}}, {'value': Decimal('0.05000000'), 'n': 1, 'scriptPubKey': {'asm': 'OP_DUP OP_HASH160 16e0f4b29b715e4021b183f8ff2fe68af6c998f0 OP_EQUALVERIFY OP_CHECKSIG', 'hex': '76a91416e0f4b29b715e4021b183f8ff2fe68af6c998f088ac', 'address': '135yMRkeBNsUBHg7Tojbp7DBMKAN6NZpq7', 'type': 'pubkeyhash'}}], 'fee': Decimal('0E-8'), 'hex': '0100000001d5c6878d299505de59f997cfaeaa35e3c815da1e7abe1a1add97cf4f59a361d5010000008b483045022100fc81c1d7a6abac1e35d555204eb81825f0b1fcab6038d5b52c15c1547c66cfd6022055746f7ecde0c22a919a0a57ca134d47c47e0a375d6f043e3ee97f6108861c1d0141045f0d6e6e49cc01c7a1535c2fe67e9ebc85849a9a45f013ed3cc7db7e8b54276a86da044b703173ea812e5297c39bc28a0393414fc15614f08b7622d35bda466cffffffff02408af701000000001976a914496446211f5f7a3325f5aeabc64b65ccc44cf36088ac404b4c00000000001976a91416e0f4b29b715e4021b183f8ff2fe68af6c998f088ac00000000'}, {'txid': '1c5c96c54720e5c4341391dddb03ddcfac51b5caaf38bc942071dc7d083fa466', 'hash': '1c5c96c54720e5c4341391dddb03ddcfac51b5caaf38bc942071dc7d083fa466', 'version': 1, 'size': 257, 'vsize': 257, 'weight': 1028, 'locktime': 0, 'vin': [{'txid': 'ce644c1e8bcc39a1d42daa52bb156d4d8e9bfceec7289ed7eef9d3567c569abd', 'vout': 0, 'scriptSig': {'asm': '3044022003330a12485ba8d8cc911b655921b804cfb36ac5811e8d98100d4452f1e42e9d022061557aa3d65365eafffee12fe5eabee9733c2ea85bc171af6ba85c43c370c75f[ALL] 046ff8cad4c42ff026c391299b9581ab5b848bded37f28a06fd1ae641996eb97f41a12f7085f84334119f660346d6a26eeeb06cc24c225a592f2fd54b035b52565', 'hex': '473044022003330a12485ba8d8cc911b655921b804cfb36ac5811e8d98100d4452f1e42e9d022061557aa3d65365eafffee12fe5eabee9733c2ea85bc171af6ba85c43c370c75f0141046ff8cad4c42ff026c391299b9581ab5b848bded37f28a06fd1ae641996eb97f41a12f7085f84334119f660346d6a26eeeb06cc24c225a592f2fd54b035b52565'}, 'sequence': 4294967295}], 'vout': [{'value': Decimal('0.28000000'), 'n': 0, 'scriptPubKey': {'asm': 'OP_DUP OP_HASH160 0b91f67a67a1be13399efeee4387e56ab64a8db1 OP_EQUALVERIFY OP_CHECKSIG', 'hex': '76a9140b91f67a67a1be13399efeee4387e56ab64a8db188ac', 'address': '124BHnTALTptgwa6BC6K33BLWjuGaRpLng', 'type': 'pubkeyhash'}}, {'value': Decimal('0.05000000'), 'n': 1, 'scriptPubKey': {'asm': 'OP_DUP OP_HASH160 b462d3ab6a5f3f0bedfaf9173934d4ba36e5e458 OP_EQUALVERIFY OP_CHECKSIG', 'hex': '76a914b462d3ab6a5f3f0bedfaf9173934d4ba36e5e45888ac', 'address': '1HSo292G3wwdq1qZWEWohJJhiw7BRo1GYp', 'type': 'pubkeyhash'}}], 'fee': Decimal('0E-8'), 'hex': '0100000001bd9a567c56d3f9eed79e28c7eefc9b8e4d6d15bb52aa2dd4a139cc8b1e4c64ce000000008a473044022003330a12485ba8d8cc911b655921b804cfb36ac5811e8d98100d4452f1e42e9d022061557aa3d65365eafffee12fe5eabee9733c2ea85bc171af6ba85c43c370c75f0141046ff8cad4c42ff026c391299b9581ab5b848bded37f28a06fd1ae641996eb97f41a12f7085f84334119f660346d6a26eeeb06cc24c225a592f2fd54b035b52565ffffffff02003fab01000000001976a9140b91f67a67a1be13399efeee4387e56ab64a8db188ac404b4c00000000001976a914b462d3ab6a5f3f0bedfaf9173934d4ba36e5e45888ac00000000'}]}]
