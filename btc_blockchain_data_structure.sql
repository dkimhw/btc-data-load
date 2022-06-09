  CREATE TABLE blocks (
  hash          BYTEA NOT NULL -- primary key
  ,height       INT NOT NULL
  ,version      INT NOT NULL
  ,prevhash     BYTEA NOT NULL
  ,merkleroot   BYTEA NOT NULL
  ,time         INT NOT NULL
  ,bits         INT NOT NULL
  ,nonce        INT NOT NULL
  ,size         BIGINT NOT NULL
  ,weight       BIGINT NOT NULL
  ,num_tx       INT NOT NULL
  ,confirmations BIGINT NOT NULL
  -- Columns orphan, status, filen and filepos are from the CBlockIndex class which is serialized in LevelDb and not formally part of the blockchain.
  -- ,orphan       BOOLEAN NOT NULL DEFAULT false
  -- ,status       INT NOT NULL
  -- ,filen        INT NOT NULL
  -- ,filepos      INT NOT NULL
  );


  CREATE TABLE txs (
   txid         BYTEA NOT NULL -- Hash of the transaction; PRIMARY KEY
   ,version      INT NOT NULL
   ,locktime     INT NOT NULL
   ,size         INT NOT NULL
   ,weight       INT NOT NULL
   ,block_hash   BYTEA NOT NULL -- Foreign key to has in blocks table
  );


  CREATE TABLE txouts (
    txid        BIGINT NOT NULL -- hash of the tx that output belongs to
    ,n            INT NOT NULL -- n is the position within the output list.
    ,value        BIGINT NOT NULL -- bitcoin that wen out
    -- 'scriptPubKey': {'asm': 'OP_DUP OP_HASH160 496446211f5f7a3325f5aeabc64b65ccc44cf360 OP_EQUALVERIFY OP_CHECKSIG', 'hex': '76a914496446211f5f7a3325f5aeabc64b65ccc44cf36088ac', 'address': '17h4TAEwZ9DZ1NpSfksrer84hr92TB5bwx', 'type': 'pubkeyhash'}}
    ,scriptpubkey BYTEA NOT NULL -- Has address here that can be parsed out later
    ,address TEXT NOT NULL

    -- array addresses?

    -- Optimization
    -- The spent column is an optimization, it is not part of the blockchain. An output is spent if later in the blockchain there exists an input referencing it.
    -- Example of Spent: https://blockstream.info/tx/0cd43a7fd3c47ebe09b9c8001eec7a7af1effbfa87b5f74279d15c66b6c66ea7?expand
    -- ,spent        BOOL NOT NULL
  );


  -- Questions: How to tease out addresses for txins?
  CREATE TABLE txins (
    txid          BIGINT NOT NULL -- hash of the tx that input belongs to
    ,n             INT NOT NULL
    -- This is the hard part I think creating maybe an intermediary table that does this
    -- ,prevout_hash  BYTEA NOT NULL
    -- ,prevout_n     INT NOT NULL
    ,scriptsig     BYTEA NOT NULL
    ,sequence      INT NOT NULL -- value in 'vin'
    ,witness       BYTEA -- where is this?
    ,prevout_tx_id BIGINT
  );
