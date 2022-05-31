import os

# Настройки Redis
REDIS_HOST = os.getenv('REDIS_HOST_TEST', 'redis-cache_test')
REDIS_PORT = int(os.getenv('REDIS_PORT_TEST', 6379))

# Настройки Elasticsearch
ELASTIC_HOST = os.getenv('ES_HOST_TEST', 'elasticsearch_test')
ELASTIC_PORT = int(os.getenv('ES_PORT_TEST', 9200))

# URL приложения
SERVICE_URL = os.getenv('SERVICE_URL', 'http://web_test:8000')

# Версия API
API = os.getenv('API', '/api/v1')
