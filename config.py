import os

# Database configuration parameters
DB_CONFIG = {
    'dbname': 'my_database',  # Replace with your database name
    'user': 'postgres',         # Replace with your username
    'password': 'mypassword',     # Replace with your password
    'host': '127.0.0.1',
    'port': '5433'
}

# Path configuration
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
