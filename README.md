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

## Data Definitions

### Block Data

| Header            | Definition                                                                                                                                                                             |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `hash`            | A unique identifier that is given to each block on the blockchain                                                                                                                      |
| `height`          | The number of blocks mined (or validated) since the creation of Bitcoin network i.e. a block with height 6 means that it's the sixth block that was created since the start of Bitcoin |
| `version`         | The block version number indicates which set of block validation rules to follow. See the list of block versions below.                                                                |
| `prev_block_hash` | A reference to the hash of the previous (parent) block in the chain.                                                                                                                   |
| `merkle_root`     | The merkle root is derived from the hashes of all transactions included in this block, ensuring that none of those transactions can be modified without modifying the header.          |
| `timestamp`       | The approximate creation time of this block (seconds from Unix Epoch).                                                                                                                 |
| `bits`            | This 4-byte field contains the target difficulty of the current Bitcoin block which determines how difficult the target hash will be to find.                                          |
| `nonce`           | An arbitrary number miners change to modify the header hash in order to produce a hash less than or equal to the target threshold                                                      |
| `size`            | The amount of data a given block has.                                                                                                                                                  |
| `weight`          | Block weight is a measure of the size of a block, measured in weight units.                                                                                                            |
| `num_tx`          | The number of transactions in a block.                                                                                                                                                 |
| `confirmations`   | The number of blocks that have been added to the Bitcoin blockchain since the block was added to the blockchain.                                                                       |

### Coinbase Transactions Data

| Header            | Definition                                                                                                                                                                  |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `txid`            | A unique identifier that is given to each trasnaction on the blockchain.                                                                                                    |
| `block_hash`      | A unique block identifier that shows what block the coinbase transaction was part of.                                                                                       |
| `version`         | The transaction version number denotees what consensus feature is supported.                                                                                                |
| `prev_block_hash` | A reference to the hash of the previous (parent) block in the chain.                                                                                                        |
| `locktime`        | Earliest time a transaction can be mined into a block.                                                                                                                      |
| `size`            | Size of the transaction in bytes                                                                                                                                            |
| `weight`          | A metric for measuring the "size" of a transaction. With the introduction of BIP 141 (Segregated Witness), transactions were given a new unit of measurement called weight. |
| `fee`             | A small amount of bitcoin paid to incentivize miners to include the transaction in the next block of the blockchain.                                                        |
| `outputs`         | Additional transaction data not parsed into cols.                                                                                                                           |

## Remaining Tasks

[x] Function definitions
[] Add definitions
[] Vins - is this necessary?
