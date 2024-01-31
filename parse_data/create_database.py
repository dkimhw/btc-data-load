

import psycopg2
from config import settings

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
            CREATE SCHEMA IF NOT EXISTS bitcoin;
        """,
        """
            CREATE TABLE IF NOT EXISTS bitcoin.block_headers (
                hash varchar NOT NULL,
                height integer NOT NULL,
                "version" integer NOT NULL,
                prev_block_hash varchar,
                merkle_root varchar NOT NULL,
                "timestamp" timestamp NOT NULL,
                bits varchar NOT NULL,
                nonce bigint NOT NULL,
                size bigint NOT NULL,
                weight bigint NOT NULL,
                num_tx integer NOT NULL,
                confirmations integer NOT NULL,
                CONSTRAINT block_headers_pkey PRIMARY KEY(hash)
            );
        """,
        """
            CREATE TABLE bitcoin.block_vins (
                block_hash varchar primary key,
                vins varchar[],
                CONSTRAINT fk_block_hash
                    FOREIGN KEY(block_hash)
                    REFERENCES bitcoin.block_headers(hash)
            );
        """,
        """
            CREATE TABLE IF NOT EXISTS bitcoin.coinbase_txs (
                txid varchar NOT NULL,
                block_hash varchar NOT NULL,
                "version" integer NOT NULL,
                locktime integer NOT NULL,
                size bigint NOT NULL,
                weight bigint NOT NULL,
                fee int,
                outputs jsonb NOT NULL,
                CONSTRAINT coinbase_txs_pkey PRIMARY KEY(txid, block_hash),
                CONSTRAINT fk_block_hash
                    FOREIGN KEY(block_hash)
                    REFERENCES bitcoin.block_headers(hash)
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
