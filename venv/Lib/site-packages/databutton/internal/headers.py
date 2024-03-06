import base64

from pydantic import BaseModel


def _get_streamlit_headers() -> dict[str, str]:
    """Return headers from current streamlit session.

    NB! The API guarantees from streamlit is rather unclear
    for this function, only some unofficial note that they'll
    provide a replacement if they remove it.
    """
    # Important to import streamlit conditionally in here as it takes a
    # while to import and we don't want that cost outside streamlit apps.
    from streamlit.web.server.websocket_headers import _get_websocket_headers

    return _get_websocket_headers() or {}


class DbtnAuthToken(BaseModel):
    user_email: str | None = None
    user_name: str | None = None


def _get_user_data() -> DbtnAuthToken | None:
    token = _get_streamlit_headers().get("X-Dbtn-Auth-Token")
    if not token:
        return None
    return DbtnAuthToken.parse_raw(base64.urlsafe_b64decode(token))
