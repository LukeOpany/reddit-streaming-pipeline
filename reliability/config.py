import os
from psycopg.conninfo import make_conninfo


def database_dsn():
    return os.getenv('DATABASE_URL') or make_conninfo(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5433'),
        dbname=os.environ['POSTGRES_DB'], user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'])
