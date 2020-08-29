from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

def make_database():
    """
    Make the Postgres database and creates the table
    """

    pwd = os.getenv('pwd')
    dbname    = 'weatherdb'
    tablename = 'dallas_weather'

    engine = create_engine('postgresql+psycopg2://postgres:%s@localhost:5432/%s'%(pwd, dbname))

    if not database_exists(engine.url):
        create_database(engine.url)

    conn = psycopg2.connect(database = dbname)

    cur = conn.cursor()

    create_table = """
    CREATE TABLE IF NOT EXISTS %s (
        id          SERIAL,
        city        TEXT,
        country     TEXT,
        latitude    REAL,
        longitude   REAL,
        sunrise     BIGINT,
        sunset      BIGINT,
        wind_speed  REAL,
        humidity    REAL,
        pressure    REAL,
        min_temp    REAL,
        max_temp    REAL,
        temp        REAL,
        weather     TEXT,
        created     TIMESTAMP WITHOUT TIME ZONE)
    """ % tablename

    cur.execute(create_table)
    conn.commit()
    conn.close()
    
if __name__ == "__main__":
    make_database()
