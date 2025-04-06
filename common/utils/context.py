from dataclasses import dataclass

from users.user import User


@dataclass
class Context:
    user: User
