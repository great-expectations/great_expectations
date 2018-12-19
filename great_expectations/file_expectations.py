# This code is early and experimental. eventually it will live in a RawFileDataset class, per issue 321. In the
# meantime, please use, enjoy, and share feedback.

import csv
import hashlib
import json
import jsonschema
import os.path


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


def expect_file_size_to_be_between(filename, minsize, maxsize):
    """
    Return True or False indicating whether the file size (in bytes) is (inclusively) between two values.

    Parameters
    ----------
    filename : string
        file to check file size
    minsize : integer
        minimum file size
    maxsize : integer
        maximum file size

    Returns
    -------
    True if the computed hash matches the specified value; False otherwise

    Raises
    ------
    OSError
        if there is a problem reading the specified file
    TypeError
        if minsize or maxsize are not integers
    ValueError
        if there is a problem with the integer value of minsize or maxsize

    """
    try:
        size = os.path.getsize(filename)
    except OSError:
        raise
    if type(minsize) != int:
        raise TypeError('minsize must be an integer')
    if type(maxsize) != int:
        raise TypeError('maxsize must be an integer')
    if minsize < 0:
        raise ValueError('minsize must be greater than of equal to 0')
    if maxsize < 0:
        raise ValueError('maxsize must be greater than of equal to 0')
    if minsize > maxsize:
        raise ValueError('maxsize must be greater than of equal to minsize')
    if (size >= minsize) and (size <= maxsize):
        return True
    else:
        return False


def expect_file_to_exist(filename):
    """
    Return True or False indicating whether the specified file exists

    Parameters
    ----------
    filename : string
        file to check for existence

    Returns
    -------
    True if the specified file exists; False otherwise

    """
    if os.path.isfile(filename):
        return True
    else:
        return False


def expect_file_unique_column_names_csv(filename,
                                        skipLines=0,
                                        sep=',',
                                        quoteChar='"',
                                        quot=csv.QUOTE_MINIMAL,
                                        doubleQuote=True,
                                        skipInitialSpace=False,
                                        escapeChar=None):
    """
    Return True or False indicating whether the specified CSV file has unique column names.

    Parameters
    ----------
    filename : string
        file on which the hash is computed
    skipLines : integer
        number of rows or lines to skip before reading the header line
    sep : string
        delimiter used for parsing CSV lines
    quoteChar : string
        one-character string used to quote fields containing special characters
    quot :
        controls when quotes should be recognised by the reader
    doubleQuote : boolean
        controls how instances of quotechar appearing inside a field should themselves be quoted
    skipInitialSpace : boolean
        when True, whitespace immediately following the delimiter is ignored
    escapeChar : string
        one-char string which removes any special meaning from the following character


    Returns
    -------
    True if the column names are unique; False otherwise

    Raises
    ------
    IOError
        if there is a problem reading the specified file
    csv.Error
        if there is an error in the csv methods

    """
    success = False
    try:
        with open(filename, 'r') as f:
            reader = csv.reader(f, delimiter=sep, quotechar=quoteChar, quoting=quot, doublequote=doubleQuote,
                                skipinitialspace=skipInitialSpace, escapechar=escapeChar, strict=True)
            if skipLines > 0:
                for i in range(0, skipLines):
                    next(reader)
            colnames = next(reader)
            if len(set(colnames)) == len(colnames):
                success = True
    except IOError:
        raise
    except csv.Error:
        raise
    return success


def expect_file_valid_json(filename, schema=None):
    """
    Return True or False indicating whether the specified file is valid JSON.

    Parameters
    ----------
    filename : string
        file to test as valid JSON
    schema : string
        optional JSON schema file on which JSON data file is validated against


    Returns
    -------
    True if the file is valid JSON; False otherwise

    Raises
    ------
    Error
        if there is a problem reading a specified file or parsing as JSON
    jsonschema.SchemaError
        if there is an error in the provided schema

    """
    success = False
    if schema is None:
        try:
            with open(filename, 'r') as f:
                json.load(f)
            success = True
        except ValueError:
            success = False
    else:
        try:
            with open(schema, 'r') as s:
                schema_data = s.read()
            sdata = json.loads(schema_data)
            with open(filename, 'r') as f:
                json_data = f.read()
            jdata = json.loads(json_data)
            jsonschema.validate(jdata, sdata)
            success = True
        except jsonschema.ValidationError:
            success = False
        except jsonschema.SchemaError:
            raise
        except:
            raise
    return success
