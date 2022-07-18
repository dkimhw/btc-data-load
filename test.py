
import json


x = json.dumps('[{"scriptpubkey":"41044e548abe660b672f494f32dd58c84be584d6d64a7886af924bb32ffee2c156c00c605a2c5cc5be5c7d59bd96ed8220d82673700df0f2d03a434fb318f68ddc1fac","scriptpubkey_asm":"OP_PUSHBYTES_65 044e548abe660b672f494f32dd58c84be584d6d64a7886af924bb32ffee2c156c00c605a2c5cc5be5c7d59bd96ed8220d82673700df0f2d03a434fb318f68ddc1f OP_CHECKSIG","scriptpubkey_type":"p2pk","value":5000000000}],"size":135,"weight":540,"fee":0,"status":{"confirmed":true,"block_height":351,"block_hash":"00000000db629e4d40902dc756ee9e3ebb87a76d2750e0846849f3d5e93bb4d6","block_time":1231866358}}')

print(x)
