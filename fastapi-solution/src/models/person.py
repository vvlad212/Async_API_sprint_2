from models.config import Base
from pydantic.class_validators import Optional


class Person(Base):
    full_name: Optional[str] = None
    id: str
