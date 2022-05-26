import os

# Настройки Redis
REDIS_DSL = {
    'host': os.environ.get('REDIS_HOST', '127.0.0.1'),
    'port': os.environ.get('REDIS_PORT', '6379')
}

# Настройки postgres
PG_DSL = {
    'dbname': os.environ.get('DB_NAME', 'movies_database'),
    'user': os.environ.get('DB_USER', 'app'),
    'password': os.environ.get('DB_PASSWORD', '123qwe'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('DB_PORT', '5432'),
}
# Настройки Elasticsearch


ELASTIC_DSL = {
    'host': os.getenv('ES_HOST', '127.0.0.1'),
    'port': os.getenv('ES_PORT', '9200')
}

# размер пачки файлов
PAGE_SIZE = 50

# частота запуска ETL
UPDATE_FREQUENCY = 5

# Корень проекта
# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
