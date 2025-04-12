from flask import g
from common.utils.context import Context
import users.users_dao as users_dao


def get_user_context() -> Context:
    access_token = g.get("access_token")
    if not access_token:
        return None

    auth_id = access_token.get("sub")
    user = users_dao.getUserFromAccessToken(auth_id)

    if user is None:
        return None

    return Context(user)
