from models.config import Base
from pydantic.class_validators import Optional


class Genres(Base):
    name: Optional[str] = None
    id: str
