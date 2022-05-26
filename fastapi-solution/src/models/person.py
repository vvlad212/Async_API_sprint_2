from pydantic.class_validators import Optional
from models.config import Base


class Person(Base):
    full_name: Optional[str] = None
    id: str
