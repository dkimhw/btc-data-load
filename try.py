from config import settings
import psycopg2


conn = psycopg2.connect(user = settings.username, password = settings.password , host= settings.host, port = settings.port, database = settings.database)
cur = conn.cursor()
cur.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'bitcoin'""")
for table in cur.fetchall():
    print(table)

# cur.execute("""SELECT * FROM blocks""")
# for i in cur.fetchall():
#   print(i)
