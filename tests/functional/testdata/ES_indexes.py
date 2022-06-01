settings = {
    "refresh_interval": "1s",
    "analysis": {
        "filter": {
            "english_stop": {
                "type": "stop",
                "stopwords": "_english_"
            },
            "english_stemmer": {
                "type": "stemmer",
                "language": "english"
            },
            "english_possessive_stemmer": {
                "type": "stemmer",
                "language": "possessive_english"
            },
            "russian_stop": {
                "type": "stop",
                "stopwords": "_russian_"
            },
            "russian_stemmer": {
                "type": "stemmer",
                "language": "russian"
            }
        },
        "analyzer": {
            "ru_en": {
                "tokenizer": "standard",
                "filter": [
                    "lowercase",
                    "english_stop",
                    "english_stemmer",
                    "english_possessive_stemmer",
                    "russian_stop",
                    "russian_stemmer"
                ]
            }
        }
    }
}

a1 = {
    'refresh_interval': '1s',
    'analysis':
        {
            'filter': {
                'russian_stemmer': {'type': 'stemmer', 'language': 'russian'},
                'english_stemmer': {'type': 'stemmer', 'language': 'english'},
                'english_possessive_stemmer': {'type': 'stemmer', 'language': 'possessive_english'},
                'russian_stop': {'type': 'stop', 'stopwords': '_russian_'},
                'english_stop': {'type': 'stop', 'stopwords': '_english_'}},
            'analyzer': {
                'ru_en': {
                    'filter': ['lowercase',
                               'english_stop',
                               'english_stemmer',
                               'english_possessive_stemmer',
                               'russian_stop',
                               'russian_stemmer'
                               ],
                    'tokenizer': 'standard'}
            }
        },
}


mappings = {
    'person':
        {
"settings": settings,
"mappings": {
    "dynamic": "strict",
    "properties": {
        "id": {
            "type": "keyword"
        },
        "full_name": {
            "type": "text",
            "analyzer": "ru_en"
        }
    }
}
},
'genre':
{
"settings": settings,
"mappings": {
    "dynamic": "strict",
    "properties": {
        "id": {
            "type": "keyword"
        },
        "name": {
            "type": "text",
            "analyzer": "ru_en"
        }
    }
}
},
}

a = {'settings': {
'index': {
    'routing': {
        'allocation': {
            'include': {'_tier_preference': 'data_content'}}}, 'refresh_interval': '1s',
    'number_of_shards': '1', 'provided_name': 'genres', 'creation_date': '1654022881002', 'analysis': {
        'filter': {
            'russian_stemmer': {'type': 'stemmer', 'language': 'russian'},
            'english_stemmer': {'type': 'stemmer', 'language': 'english'},
            'english_possessive_stemmer': {'type': 'stemmer', 'language': 'possessive_english'},
            'russian_stop': {'type': 'stop', 'stopwords': '_russian_'},
            'english_stop': {'type': 'stop', 'stopwords': '_english_'}},
        'analyzer': {
            'ru_en': {
                'filter': ['lowercase',
                           'english_stop',
                           'english_stemmer',
                           'english_possessive_stemmer',
                           'russian_stop',
                           'russian_stemmer'
                           ],
                'tokenizer': 'standard'}
        }
    },
    'number_of_replicas': '1',
    'uuid': 'dyCi5aWaSrWYHYpVC6xpBg', 'version': {'created': '7160399'}}}}
