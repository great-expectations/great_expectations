import logging
from hashlib import md5

logger = logging.getLogger(__name__)


class Anonymizer(object):
    """Anonymize string names in an optionally-consistent way."""
    def __init__(self, salt=None):
        if salt is not None and not isinstance(salt, str):
            logger.error("invalid salt: must provide a string. Setting a random salt.")
            salt = None
        if salt is None:
            import secrets
            self._salt = secrets.token_hex(8)
        else:
            self._salt = salt

    @property
    def salt(self):
        return self._salt

    def anonymize(self, string_):
        salted = self._salt + string_
        return md5(salted.encode('utf-8')).hexdigest()
