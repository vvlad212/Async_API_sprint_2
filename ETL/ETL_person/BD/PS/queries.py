def create_query(time_select, query_name: str):
    queries = {'person_query': f"""
                        SELECT 
                            full_name,
                            id, 
                            modified
                        FROM content.person
                        where modified > '{time_select}'
                        order by modified;                        
                        """,
               'genre_query': f"""
                        SELECT 
                            name,
                            id, 
                            modified
                        FROM content.genre
                        where modified > '{time_select}'
                        order by modified;                        
                        """,
               }
    return queries[query_name]
