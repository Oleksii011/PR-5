import psycopg2
from flask_jwt_extended import JWTManager

jwt = JWTManager()

DB_CONFIG = {
    'host': 'junction.proxy.rlwy.net',
    'port': '19910',
    'database': 'RailwayDB',
    'user': 'postgres',
    'password': 'ybFsTxMPmeikkNgpBcAGDwjbLfMGJbDG'
}

def get_db_connection():
    connection = psycopg2.connect(
        dbname=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port']
    )
    return connection
