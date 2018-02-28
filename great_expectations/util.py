import hashlib

class DotDict(dict):
    """dot.notation access to dictionary attributes"""
    def __getattr__(self, attr):
        return self.get(attr)
    __setattr__= dict.__setitem__
    __delattr__= dict.__delitem__
    def __dir__(self):
        return self.keys()

def expect_file_hash_to_equal(filename, value, hash_alg='md5'):
    """
    Return True or False indicating whether the hash matches the specified value for the default (md5) or user-specified hash algorithm

    Parameters
    ----------
    filename : string
        file on which the hash is computed
    value : string
        value to compare to computed hash
    hash_alg : string, default='md5'
        hash alogorithm to use. See hashlib.algorithms_available for supported algorithms.

    Returns
    -------
    True if the computed hash matches the specified value; False otherwise

    Raises
    ------
    IOError
        if there is a problem reading the specified file
    ValueError
        if the specified hash algorithm is not defined by hashlib

    """
    success = False
    try:
        hash = hashlib.new(hash_alg)
        # Limit file reads to 64 KB chunks at a time
        BLOCKSIZE = 65536
        try:
            with open(filename, 'rb') as file:
                file_buffer = file.read(BLOCKSIZE)
                while len(file_buffer) > 0:
                    hash.update(file_buffer)
                    file_buffer = file.read(BLOCKSIZE)
                success = hash.hexdigest() == value
        except IOError:
            raise
    except ValueError:
        raise
    return success
