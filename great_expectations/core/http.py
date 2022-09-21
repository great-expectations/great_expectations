import requests

from great_expectations import __version__


def session_factory(access_token: str) -> requests.Session:
    session = requests.Session()
    _update_headers(session, access_token)
    return session


def _update_headers(session: requests.Session, access_token: str) -> None:
    headers = {
        "Content-Type": "application/vnd.api+json",
        "Authorization": f"Bearer {access_token}",
        "Gx-Version": __version__,
    }
    session.headers.update(headers)
