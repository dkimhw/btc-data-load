

import psycopg2
from config import settings

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
            CREATE SCHEMA IF NOT EXISTS bitcoin;
        """,
        """
            CREATE TABLE IF NOT EXISTS bitcoin.blocks (
                hash          BYTEA PRIMARY KEY
                ,height       INT NOT NULL
                ,version      INT NOT NULL
                ,prev_hash     BYTEA NOT NULL
                ,merkleroot   BYTEA NOT NULL
                ,time         INT NOT NULL
                ,timestamp    TIMESTAMP NOT NULL
                ,bits         TEXT NOT NULL
                ,nonce        BIGINT NOT NULL
                ,size         INT NOT NULL
                ,weight       INT NOT NULL
                ,num_tx       INT NOT NULL
                ,confirmations BIGINT NOT NULL
            );
        """,
        """
            CREATE TABLE IF NOT EXISTS bitcoin.txs (
                tx_id          BYTEA PRIMARY KEY
                ,version      INT NOT NULL
                ,size         INT NOT NULL
                ,vsize        INT NOT NULL
                ,weight       INT NOT NULL
                ,locktime     INT NOT NULL
                ,fee          DECIMAL(16, 8) NOT NULL
                ,block_hash   BYTEA NOT NULL
                ,CONSTRAINT fk_block_hash
                    FOREIGN KEY(block_hash)
                    REFERENCES bitcoin.blocks(hash)
            );
        """,
        """
            CREATE TABLE IF NOT EXISTS bitcoin.tx_outs (
                tx_id            BYTEA NOT NULL
                ,n              INT NOT NULL
                ,value          DECIMAL(16, 8) NOT NULL
                ,scriptpubkey   JSON NOT NULL
                ,address        TEXT
                ,PRIMARY KEY (tx_id, n)
                ,CONSTRAINT fk_tx
                    FOREIGN KEY(tx_id)
                    REFERENCES bitcoin.txs(tx_id)
            );
        """,
        """
            CREATE TABLE IF NOT EXISTS bitcoin.tx_ins (
                prev_tx_id      BYTEA NOT NULL -- previous hash tx_id
                ,prev_n         INT NOT NULL -- previous hash n
                ,scriptsig      JSON NOT NULL
                ,sequence       BIGINT NOT NULL
                ,curr_tx_id     BYTEA NOT NULL
                ,PRIMARY KEY (prev_tx_id, prev_n)
                ,CONSTRAINT fk_tx
                    FOREIGN KEY(curr_tx_id)
                    REFERENCES bitcoin.txs(tx_id)
                -- Due to the nature of this table - we cannot force foreign key constraint since we are limiting the size of the table
            );
        """)
    conn = None
    try:

        # connect to the PostgreSQL server
        conn = psycopg2.connect(user = settings.username, password = settings.password , host= settings.host, port = settings.port, database = settings.database)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    create_tables()
