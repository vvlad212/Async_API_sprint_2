from typing import List


def create_match_query(match_query: dict,
                       page: int,
                       size: int,
                       sort: dict = {}):
    """формирование запроса для elastic.

    Args:
        match_query:
        page:
        size:
        sort:

    Returns:

    """

    body = {
        "bool": {
            "must": [],
        }
    }
    body['bool']['must'].append(match_query)
    query = {'query': body,
             "from": page,
             "size": size,
             "sort": [sort],
             "aggs": {}
             }
    return query


def create_nested_query(cond: str,
                        nested_filter: List[dict],
                        page: int,
                        size: int,
                        sort: dict = {}):
    def create_nested(filter_values: dict):
        return {
            "nested": {
                "path": filter_values['path'],
                "query": {
                    "bool": {
                        "must": [
                            {"match": filter_values['value']}
                        ]
                    }
                }
            }
        }

    query = {
        "query": {
            "bool": {
                cond: [create_nested(element) for element in nested_filter]
            }
        },
        "from": page,
        "size": size,
        "sort": [sort],
        "aggs": {}
    }
    return query
