from models import Person


class PersonEtl:
    """Класс описывающий элемент ETL."""

    def __init__(self):
        self.model = Person
        self.index_name = 'person'
        self.query = 'person_query'
