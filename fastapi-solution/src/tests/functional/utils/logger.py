log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

log_conf = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'console': {
            'format': log_format,
        },
    },
    'handlers': {
        'console': {
            'formatter': 'console',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
        },
    },
    'root': {
        'level': 'DEBUG',
        'handlers': [
            'console',
        ],
    }
}
