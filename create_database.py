

import psycopg2
from config import settings

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
            CREATE SCHEMA IF NOT EXISTS bitcoin;
        """,
        """
            CREATE TABLE IF NOT EXISTS bitcoin.blocks_test (
                hash          TEXT PRIMARY KEY
                ,height       INT NOT NULL
            );
        """,
        """
            CREATE TABLE IF NOT EXISTS bitcoin.blocks (
                hash          BYTEA PRIMARY KEY
                ,height       INT NOT NULL
                ,version      INT NOT NULL
                ,prevhash     BYTEA NOT NULL
                ,merkleroot   BYTEA NOT NULL
                ,time         INT NOT NULL
                ,timeestamp   TIMESTAMP NOT NULL
                ,bits         INT NOT NULL
                ,nonce        INT NOT NULL
                ,size         BIGINT NOT NULL
                ,weight       BIGINT NOT NULL
                ,num_tx       INT NOT NULL
                ,confirmations BIGINT NOT NULL
            );
        """,
        """
            CREATE TABLE IF NOT EXISTS bitcoin.txs (
                txid         BYTEA PRIMARY KEY
                ,version      INT NOT NULL
                ,locktime     INT NOT NULL
                ,size         INT NOT NULL
                ,weight       INT NOT NULL
                ,block_hash   BYTEA NOT NULL
            );
        """,
        """
            CREATE TABLE IF NOT EXISTS bitcoin.txouts (
                txid        BIGINT NOT NULL
                ,n            INT NOT NULL
                ,value        BIGINT NOT NULL
                ,scriptpubkey BYTEA NOT NULL
                ,address TEXT NOT NULL
                ,PRIMARY KEY (txid, n)
            );
        """,
        """
            CREATE TABLE IF NOT EXISTS bitcoin.txins (
                txid          BIGINT NOT NULL
                ,n             INT NOT NULL
                ,scriptsig     BYTEA NOT NULL
                ,sequence      INT NOT NULL
                ,PRIMARY KEY (txid, n)
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
