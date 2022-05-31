import os

# Настройки Redis
REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# Настройки Elasticsearch
ELASTIC_HOST = os.getenv('ES_HOST', '127.0.0.1')
ELASTIC_PORT = int(os.getenv('ES_PORT', 9200))

# URL приложения
SERVICE_URL = 'http://127.0.0.1:8000'
# Версия API
API = '/api/v1'
