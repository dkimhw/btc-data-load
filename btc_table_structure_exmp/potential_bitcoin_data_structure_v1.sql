CREATE TABLE IF NOT EXISTS bitcoin.blocks (
  hash          BYTEA PRIMARY KEY
  ,height       INT NOT NULL
  ,version      INT NOT NULL
  ,prev_hash    BYTEA
  ,merkleroot   BYTEA NOT NULL
  ,time         BIGINT NOT NULL
  ,timestamp    TIMESTAMP NOT NULL
  ,bits         TEXT NOT NULL
  ,nonce        BIGINT NOT NULL
  ,size         INT NOT NULL
  ,weight       INT NOT NULL
  ,num_tx       INT NOT NULL
  ,confirmations BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS bitcoin.coinbase_txs (
  tx_id         BYTEA NOT NULL
  ,block_hash   BYTEA NOT NULL
  ,PRIMARY KEY (tx_id, block_hash)
  ,CONSTRAINT fk_block_hash
      FOREIGN KEY(block_hash)
      REFERENCES bitcoin.blocks(hash)
);

CREATE TABLE IF NOT EXISTS bitcoin.exchange_addresses (
  btc_address STRING NOT NULL
  , PRIMARY KEY (btc_address)
)
