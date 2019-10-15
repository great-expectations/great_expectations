class InMemoryEvaluationParameterStore(object):
    """You want to be a dict. You get to be a dict. But we call you a Store."""
    
    def __init__(self, root_directory=None):
        self.store = {}

    def get(self, key):
        return self.store[key]

    def set(self, key, value):
        self.store[key] = value

    def has_key(self, key):
        return key in self.store

    def list_keys(self):
        return list(self.store.keys())
