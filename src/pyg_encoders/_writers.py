from pyg_encoders._encoders import csv_write, parquet_write, npy_write, pickle_write, _csv, _npy, _npa, _parquet, _pickle, _dictable, root_path
from pyg_encoders._encoders import pickle_load, pd_read_csv, pd_read_parquet
from pyg_encoders._locks import _locked_pd_read_npy
from pyg_encoders._encode import encode, decode 
from pyg_base import passthru, is_str, as_list, get_cache, dt, dictattr, getargspec, partialize, dictdir
import os

_WRITERS = 'WRITERS'
_READERS = 'READERS'
if _WRITERS not in get_cache():
    get_cache()[_WRITERS] = {}
if _READERS not in get_cache():
    get_cache()[_READERS] = {}
    
WRITERS = get_cache()[_WRITERS]
READERS = get_cache()[_READERS]

WRITERS.update({_csv: csv_write , 
               _npy: partialize(npy_write, append = False), 
               '.np0': partialize(npy_write, append = False, max_workers = 0), 
               _npa: partialize(npy_write, append = True), 
               _parquet: parquet_write, 
               '.parque0' : partialize(parquet_write, max_workers = 0),
               '.pickl0' : partialize(pickle_write, max_workers = 0),
               _pickle : pickle_write})

READERS.update({_csv: pd_read_csv,
                _pickle: pickle_load, 
                _npy : _locked_pd_read_npy, 
                _npa: _locked_pd_read_npy,
                _parquet: pd_read_parquet
                })

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

def as_writer(writer = None, kwargs = None, unchanged = None, unchanged_keys = None, asof = None, **writer_kwargs):
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
    writer_kwargs:
        parameters specific to the writer we load, which we don't know in advance

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
        return sum([as_writer(w, kwargs = kwargs, unchanged = unchanged, unchanged_keys=unchanged_keys, asof = asof) for w in writer], [])
    e = encode if unchanged is None and unchanged_keys is None else partialize(encode, unchanged = unchanged, unchanged_keys = unchanged_keys)
    if writer is None or writer is True or writer == ():
        return [e]
    elif writer is False or writer == 0:
        return [passthru]
    elif is_str(writer):
        if '@' in writer:
            writer, asof = writer.split('@')
            asof = dt(asof)
        for ext, w in WRITERS.items():
            if writer.lower().endswith(ext):
                root = writer[:-len(ext)]                    
                if len(root)>0:
                    if kwargs and isinstance(kwargs, dict):
                        root = root_path(kwargs, root)
                    return [partialize(w, root = root, asof = asof, **writer_kwargs), e]
                else:
                    return [partialize(w, asof = asof, **writer_kwargs), e]
        err = 'Could not convert "%s" into a valid writer.\nAt the moment we support these extenstions: \n%s'%(writer, '\n'.join('%s maps to %s'%(k,v) for k,v in WRITERS.items()))
        err += '\nWriter should look like "d:/archive/%country/%city/results.parquet"'
        raise ValueError(err)
    else:
        return as_list(writer)



def _np_read_path(pth, ext, level = 0):
    reader = READERS[ext]
    if os.path.exists(pth) and os.path.exists(os.path.join(pth, 'data.npy')) and os.path.exists(os.path.join(pth, 'index.npy')):
        return reader(pth + ext)
    else:
        return dictattr({k: _np_read_path(p, ext) for k,p in dictdir(pth, level = level).items()})/None

def _pd_read_path(pth, ext, level = 0):
    if 'np' in ext:
        return _np_read_path(pth, ext, level)
    reader = READERS[ext]
    if os.path.exists(pth + ext):
        return reader(pth + ext)
    elif os.path.exists(pth):
        return dictattr({k[:-len(ext)]: reader(v) for k, v in dictdir(pth, level = level).items() if k.endswith(ext)})/None
        
    

def pd_read_root(root, doc = None, output = None, level = 0):
    """
    
    Returns a list of dataframes 

    Example: simple load of parquet file
    --------
    >>> from pyg import * 
    >>> root = 'c:/temp/%x/%y.parquet'
    >>> a = pd.Series([1,2,3], drange(2)); b = pd.DataFrame(dict(a = [1,2,3], b = 4), drange(2))
    >>> doc = cell(add_, a = a, b = b, x = 'x', y = 'y', db = root).go()
    >>> pd_read_root(root, cell(add_, a = a, b = b, x = 'x', y = 'y', db = root))

    Example: simple load of npy file
    --------
    >>> from pyg import * 
    >>> root = 'c:/temp/%x/%y.npy'
    >>> a = pd.Series([1,2,3], drange(2)); b = pd.DataFrame(dict(a = [1,2,3], b = 4), drange(2))
    >>> doc = cell(add_, a = a, b = b, x = 'x', y = 'npy', db = root).go()
    >>> pd_read_root(root, cell(add_, a = a, b = b, x = 'x', y = 'npy', db = root))

    Example: simple load of dict of files
    --------
    >>> from pyg import * 
    >>> root = 'c:/temp/%x/%y.parquet'
    >>> a = pd.Series([1,2,3], drange(2)); b = pd.DataFrame(dict(a = [1,2,3], b = 4), drange(2))
    >>> f = lambda a,b: dict(add = add_(a,b), mul = mul_(a,b)); f.output = ['add', 'mul']
    >>> doc = cell(f, a = a, b = b, x = 'x', y = 'parquet_dict', db = root).go()
    >>> pd_read_root(root, cell(f, a = a, b = b, x = 'x', y = 'parquet_dict', db = root))

    >>> f = lambda a,b: dict(add = add_(a,b), mul = mul_(a,b))
    >>> doc = cell(f, a = a, b = b, x = 'x', y = 'parquet_dict_in_data', db = root).go()
    >>> pd_read_root(root, cell(f, a = a, b = b, x = 'x', y = 'parquet_dict_in_data', db = root))

    Example: simple load of dict of npy files
    ---------
    >>> from pyg import * 
    >>> root = 'c:/temp/%x/%y.npy'
    >>> a = pd.Series([1,2,3], drange(2)); b = pd.DataFrame(dict(a = [1,2,3], b = 4), drange(2))
    >>> f = lambda a,b: dict(add = add_(a,b), mul = mul_(a,b))
    >>> doc = cell(f, a = a, b = b, x = 'x', y = 'npy_dict_in_data', db = root).go()
    >>> pd_read_root(root, cell(f, a = a, b = b, x = 'x', y = 'npy_dict_in_data', db = root))

    
    Parameters
    ----------
    root : str
        Such as 'c:/temp/%x/%y.parquet'
    doc : dict, optional
        A document to populate the root keys from.
    output : str/list, optional
        list of keys we are interested to load from file

    Returns
    -------
    dict of values
        
    """
    doc = doc or {}
    res = dictattr()
    path = root_path(doc = doc, root = root)
    if output is None:
        output = as_list(getattr(doc, '_output', 'data'))

    ext = '.' + root.split('.')[-1]
    path = path[:-len(ext)]
    for out in output:
        if doc.get(out) is None:
            pth = os.path.join(path, out)
            res[out] = _pd_read_path(pth, ext, level)
    return res / None
