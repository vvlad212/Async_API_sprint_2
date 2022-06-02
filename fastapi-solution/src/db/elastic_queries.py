from typing import List


def match_query(
        match_value: dict,
        offset_from: int,
        page_size: int,
        sort: dict = {}):
    """формирование запроса для elastic.

    Args:
        match_value:
        offset_from:
        page_size:
        sort:

    Returns:

    """

    body = {
        "bool": {
            "must": [],
            "should": []
        }
    }
    body['bool']['must'].append(match_value)
    query = {'query': body,
             "from": offset_from,
             "size": page_size,
             "sort": [sort],
             "aggs": {}
             }
    return query


def nested_query(
        condition: str,
        nested_filter: List[dict],
        offset_from: int,
        page_size: int,
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
                condition: [create_nested(element) for element in nested_filter]
            }
        },
        "from": offset_from,
        "size": page_size,
        "sort": [sort],
    }
    return query
