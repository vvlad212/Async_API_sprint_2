import os

# Настройки Redis (test)
REDIS_HOST = os.getenv('REDIS_HOST_TEST', '127.0.0.1')
REDIS_PORT = int(os.getenv('REDIS_PORT_TEST', 6379))

# Настройки Elasticsearch (test)
ELASTIC_HOST = os.getenv('ES_HOST_TEST', '127.0.0.1')
ELASTIC_PORT = int(os.getenv('ES_PORT_TEST', 9201))

# Настройки Elasticsearch (prod)
ELASTIC_HOST_SOURCE = os.getenv('ES_HOST', '127.0.0.1')
ELASTIC_PORT_SOURCE = int(os.getenv('ES_PORT', 9200))

# URL приложения
SERVICE_URL = os.getenv('SERVICE_URL', 'http://127.0.0.1:8000')

# Версия API
API = os.getenv('API', '/api/v1')
