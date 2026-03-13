"""Доменная сущность пользователя."""

import uuid
import re
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional

from .exceptions import InvalidEmailError


# TODO: Реализовать класс User
# - Использовать @dataclass
# - Поля: email, name, id, created_at
# - Реализовать валидацию email в __post_init__
# - Regex: r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"

@dataclass
class User:
    email: str
    name: Optional[str] = None
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    created_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        if not re.match(pattern, self.email):
            raise InvalidEmailError(f"Некорректный email: {self.email}")
