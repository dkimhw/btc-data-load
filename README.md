# BTC Data Load

Code for parsing Bitcoin blocks & coinbase txs data from a locally running Bitcoin node and uploading it into a postgres database.

## Overview

Bitcoin block & transaction data is stored on a blockchain - a distributed ledger shared among a computer network's nodes. Anyone running a node has access to this shared dataset but it's not easily accesible. The main goal of this repository is to extract data from a node running locally & transfer that data into postgres so that the data is more accessible for web applications and for analytics & reporting purposes.

Given the massive size of dataset, the data model done here focuses mostly on extracting the following data:

- Blocks
- Coinbase Txs

The extracted data allows for easy access to questions like: has transactions on the Bitcoin network been increasing over time?

```sql
  select
    date_trunc('month', bh.timestamp::date)::date as "date"
    , sum(bh.num_tx) as "value"
  from
    bitcoin.block_headers AS bh
  WHERE
    bh.timestamp BETWEEN '${startDate}'  AND '${endDate}'
  group by 1 order by 1 asc;
```

The individual transactions (`vins` & `vouts`) on the Bitcoin network were not included given that there are hundreds of millions of rows of transactions and many of the high level questions regarding the Bitcoin network can be answered via blocks & coinbase transactions.

## Installation

1. Download Bitcoin Core & wait for initial synchronization (must have enough space on local drive - ~500GB+)
2. Open the bitcoin.config file & paste the following:

```
server = 1
rpcbind = 127.0.0.1:8332
rpcuser = username
rpcpassword = password
```

3. Configure `settings.toml` file - add in the username, host, port & database for a postgres database

```
username =
host =
port =
database =
```

4. Run `pip install -r requirements.txt` in the working directory of btc-data-load

## Instructions

- To load blocks

```
python load_blocks.py
```

- To load coinbase txs

```
python load_coinbase_txs.py
```

## Remaining Tasks

[] Retry upload
[] Function definitions
[] Add definitions
[] Vins
