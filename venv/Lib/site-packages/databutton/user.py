from pydantic import BaseModel


class User(BaseModel):
    name: str = ""
    email: str = ""


def get() -> User | None:
    """Get the current viewer of the app.

    This will return None if the user is
    not logged in and viewing a public app.
    """
    from databutton.internal.headers import _get_user_data

    user_data = _get_user_data()
    if user_data is None:
        return None
    return User(
        name=user_data.user_name or "",
        email=user_data.user_email or "",
    )
