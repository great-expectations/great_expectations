class Anonymizer(object):
    """Provide a private _token_hex property that is generated per instance."""
    def __init__(self):
        import secrets
        self._token_hex = secrets.token_hex(8)
