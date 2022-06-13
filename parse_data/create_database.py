

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
        """,
        """
            CREATE TABLE IF NOT EXISTS bitcoin.coinbase_txs (
                tx_id         BYTEA NOT NULL
                ,block_hash   BYTEA NOT NULL
                ,PRIMARY KEY (tx_id, block_hash)
                ,CONSTRAINT fk_block_hash
                    FOREIGN KEY(block_hash)
                    REFERENCES bitcoin.blocks(hash)
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
