from pydantic.class_validators import Optional
from models.config import Base


class Genres(Base):
    name: Optional[str] = None
    id: str
