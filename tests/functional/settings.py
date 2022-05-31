import os

# Настройки Redis
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cache_test')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# Настройки Elasticsearch
ELASTIC_HOST = os.getenv('ES_HOST', 'elasticsearch_test')
ELASTIC_PORT = int(os.getenv('ES_PORT', 9200))

# URL приложения
SERVICE_URL = os.getenv('SERVICE_URL', 'http://web_test:8000')

# Версия API
API = os.getenv('API', '/api/v1')
