import requests

from great_expectations import __version__


def session_factory(access_token: str) -> requests.Session:
    session = requests.Session()
    headers = _init_headers(access_token)
    session.headers.update(headers)
    return session


def _init_headers(access_token: str) -> dict:
    headers = {
        "Content-Type": "application/vnd.api+json",
        "Authorization": f"Bearer {access_token}",
        "Gx-Version": __version__,
    }
    return headers
