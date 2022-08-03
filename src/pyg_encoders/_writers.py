from pyg_encoders._encoders import csv_write, parquet_write, npy_write, pickle_write, _csv, _npy, _npa, _parquet, _pickle, root_path
from pyg_encoders._encode import encode, decode 
from pyg_base import passthru, is_str, as_list
from functools import partial

WRITERS = {_csv: csv_write , 
           _npy: partial(npy_write, append = False), 
           _npa: partial(npy_write, append = True), 
           _parquet: parquet_write, 
           _pickle : pickle_write}

def as_reader(reader = None):
    """
        returns a list of functions that are applied to an object to turn it into a valid document
    """
    if isinstance(reader, list):
        return sum([as_reader(r) for r in reader], [])
    elif reader is None or reader is True or reader == ():
        return [decode]
    elif reader is False or reader == 0:
        return [passthru]
    else:
        return [reader]

def as_writer(writer = None, kwargs = None, unchanged = None, unchanged_keys = None):
    """
    returns a list of functions that convert a document into an object that can be pushed into the storage mechanism we want

    :Parameters:
    ------------
    writer : None, callable, bool, string
        A function that loads an object. 
        The default is None.
    kwargs : dict, optional
        Parameters that can be used to resolve part of the writer if a string. The default is None.
    unchanged : type/list of types, optional
        inputs into the 'encode' function, allowing us to not-encode some of the values in document based on their type
    unchanged_keys : str/list of str, optional
        inputs into the 'encode' function, allowing us to not-encode some of the keys in document 

    Raises
    ------
    ValueError
        Unable to convert writer into a valid writer.

    Returns
    -------
    list
        list of functions.

    """
    if isinstance(writer, list):
        return sum([as_writer(w) for w in writer], [])
    e = encode if unchanged is None and unchanged_keys is None else partial(encode, unchanged = unchanged, unchanged_keys = unchanged_keys)
    if writer is None or writer is True or writer == ():
        return [e]
    elif writer is False or writer == 0:
        return [passthru]
    elif is_str(writer):
        for ext, w in WRITERS.items():
            if writer.endswith(ext):
                root = writer[:-len(ext)]                    
                if root:
                    if kwargs:
                        root = root_path(kwargs, root)
                    return [partial(w, root = root), e]
                else:
                    return [w, e]
        raise ValueError('We support only csv/npy/parquet/parquet writers and writer should look like: c:/somewhere/%name/%surname.csv or d:/archive/%country/%city/results.parquet or with .npy or .pickle')
    else:
        return as_list(writer)

