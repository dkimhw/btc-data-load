
## Instructions

1. Run a full Bitcoin node
2. Open the bitcoin.config file & paste the following:
  ```
  server = 1
  rpcbind = 127.0.0.1:8332
  rpcuser = username
  rpcpassword = password
  ```
4. Run `pip install -r requirements.txt` in the working directory of btc-data-load
3. Run script `python app.py`
