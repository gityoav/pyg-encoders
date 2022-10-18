import pandas as pd
import numpy as np
from pyg_encoders._parquet import pd_to_parquet, pd_read_parquet
from pyg_encoders._encode import encode, decode
from pyg_base import is_pd, is_dict, is_series, is_arr, is_date, dt2str, tree_items, dictable, try_value, dt
from pyg_npy import pd_to_npy, np_save, pd_read_npy, mkdir
from pyg_base import Bi, bi_merge, is_bi, bi_read, try_none
from pyg_base._bitemporal import _asof
from functools import partial
import pickle

_pickle = '.pickle'
_parquet = '.parquet'
_dictable = '.dictable'
_npy = '.npy'; _npa = '.npa'
_csv = '.csv'
_series = '_is_series'
_root = 'root'
_db = 'db'
_obj = '_obj'
_writer = 'writer'

__all__ = ['root_path', 'pd_to_csv', 'pd_read_csv', 'parquet_encode', 'parquet_write', 'csv_encode', 'csv_write', 'pickle_dump', 'pickle_load', 'dictable_decode']


# def encode(value):
#     """
#     encoder is similar to pyg_base.encoder with the only exception being that bson.objectid.ObjectId used by mongodb to generate the document _id, are not encoded

#     Parameters
#     ----------
#     value : value/document to be encoded

#     Returns
#     -------
#     encoded value/document
#     """
#     return encode_(value, ObjectId)

def root_path(doc, root, fmt = None, **kwargs):
    """
    returns a location based on doc
    
    :Example:
    --------------
    >>> root = 'c:/%school/%pupil.name/%pupil.surname/'
    >>> doc = dict(school = 'kings', 
                   pupil = dict(name = 'yoav', surname = 'git'), 
                   grades = dict(maths = 100, physics = 20, chemistry = 80), 
                   report = dict(date = dt(2000,1,1), 
                                 teacher = dict(name = 'adam', surname = 'cohen')
                                 )
                   )
    
    >>> assert root_path(doc, root) == 'c:/kings/yoav/git/'

    The scheme is entirely up to the user and the user needs to ensure what defines the unique primary keys that prevent documents overstepping each other...
    >>> root = 'c:/%school/%pupil.name_%pupil.surname/'
    >>> assert root_path(doc, root) == 'c:/kings/yoav_git/'
    
    >>> root = 'c:/archive/%report.date/%pupil.name.%pupil.surname/'
    >>> assert root_path(doc, root, '%Y') == 'c:/archive/2000/yoav.git/'  # can choose to format dates by providing a fmt.
    """
    doc = dict(doc)
    doc.update(kwargs)
    items = sorted(tree_items(doc))[::-1]
    res = root
    for row in items:
        text = '%(' + '.'.join(row[:-1]) + ')'
        if text in root:
            value = dt2str(row[-1], fmt).replace(':','') if is_date(row[-1]) else str(row[-1]).replace(':', '')
            res = res.replace(text, '%s'% value)
        text = '%' + '.'.join(row[:-1])
        if text in root:
            value = dt2str(row[-1], fmt).replace(':','') if is_date(row[-1]) else str(row[-1]).replace(':', '')
            res = res.replace(text, '%s'% value)
    return res

def root_path_check(path):
    if '%' in path:
        raise ValueError('The document did not contain enough keys to determine the path %s'%path)
    return path

def pd_to_csv(value, path, asof = None):
    """
    A small utility to write both pd.Series and pd.DataFrame to csv files
    """    
    if '@' in path:
        path, asof = path.split('@')    
    if asof is not None:
        value = Bi(value, asof)
    if is_series(value):
        value.index.name = _series
    if value.index.name is None:
        value.index.name = 'index'
    if path[-4:].lower()!=_csv:
        path = path + _csv
    mkdir(path)
    if is_bi(value):
        old = try_none(pd_read_csv)(path)
        value = bi_merge(old, value)
    value.to_csv(path)
    return path


def _pickle_raw(path):
    try:
        with open(path) as f:
            df = pickle.load(f)
    except Exception: #pandas read_pickle sometimes work when pickle.load fails
        df = pd.read_pickle(path)
    return df


def pickle_dump(value, path, asof = None):
    if '@' in path:
        path, asof = path.split('@')
    mkdir(path)
    if asof is not None:
        value = Bi(value, asof)
    if is_bi(value):
        old  = try_none(_pickle_raw)(path)
        value = bi_merge(old, value)
    if hasattr(value, 'to_pickle'):
        value.to_pickle(path) # use object specific implementation if available
    else:
        with open(path, 'wb') as f:
            pickle.dump(value, f)
    return path


def pickle_load(path, asof = None, what = 'last'):
    df = _pickle_raw(path)
    if asof is not None:
        df = bi_read(df, asof, what)
    return df
            

def pd_read_csv(path, asof = None, what = 'last'):
    """
    A small utility to read both pd.Series and pd.DataFrame from csv files
    """
    res = pd.read_csv(path)
    if asof is not None:
        res = bi_read(res, asof, what)
    if res.columns[0] == _series and res.shape[1] == 2:
        res = pd.Series(res[res.columns[1]], res[_series].values)
        return res
    if res.columns[0] == 'index':
        res = res.set_index('index')
    return res

def dictable_decode(df, loader = None):
    """
    converts a dataframe with objects encoded into a dictable with decoded objects
    :Parameters:
    ------------
    df: str/dataframe/dictable
        items that can be converted into a dictable
    loader: df may need to be loaded if it is e.g. a path to a sql database
    
    """
    if loader is not None and not isinstance(df, dictable):
        df = loader(df)
    res = dictable(df)
    res = res.rename(lambda _col: _col[1:-1] if _col.startswith('"') else _col)
    res = res.do(decode)
    return res

def dictable_decoded(path):
    return dictable_decode(path)

_pd_read_csv = encode(pd_read_csv)
_pd_read_parquet = encode(pd_read_parquet)
_pd_read_npy = encode(pd_read_npy)
_pickle_load = encode(pickle_load)
_np_load = encode(np.load)
_dictable_decode = encode(dictable_decode)


def pickle_encode(value, path, asof = None):
    """
    encodes a single DataFrame or a document containing dataframes into a an abject of multiple pickled files that can be decoded
    """
    if '@' in path:
        path, asof = path.split('@')
    if path.endswith(_pickle):
        path = path[:-len(_pickle)]
    if path.endswith('/'):
        path = path[:-1]
    if is_pd(value):
        path = root_path_check(path)
        path = path if path.endswith(_pickle) else path + _pickle
        path = pickle_dump(value, path = path, asof = asof)
        if asof is None:
            return dict(_obj = _pickle_load, path = path)
        else:
            return dict(_obj = _pickle_load, path = path, asof = dt()) 
    elif is_arr(value):
        path = root_path_check(path)
        mkdir(path + _npy)
        np.save(path + _npy, value)
        return dict(_obj = _np_load, file = path + _npy)        
    elif is_dict(value):
        res = type(value)(**{k : pickle_encode(v, '%s/%s'%(path,k), asof) for k, v in value.items()})
        if isinstance(value, dictable):
            return dict(_obj = _dictable_decode,
                        df = dict(_obj = _pickle_load, 
                                  path = pickle_dump(res, path if path.endswith(_dictable) else path + _dictable)))
        return res
    elif isinstance(value, (list, tuple)):
        return type(value)([pickle_encode(v, '%s/%i'%(path,i), asof) for i, v in enumerate(value)])
    else:
        return value

pd_to_parquet_twice = try_value(pd_to_parquet, repeat = 2, sleep = 1, return_value = False)


def parquet_encode(value, path, compression = 'GZIP', asof = None):
    """
    encodes a single DataFrame or a document containing dataframes into a an abject that can be decoded

    >>> from pyg import *     
    >>> path = 'c:/temp'
    >>> value = dict(key = 'a', n = np.random.normal(0,1, 10), data = dictable(a = [pd.Series([1,2,3]), pd.Series([4,5,6])], b = [1,2]), other = dict(df = pd.DataFrame(dict(a=[1,2,3], b= [4,5,6]))))
    >>> encoded = parquet_encode(value, path)
    >>> assert encoded['n']['file'] == 'c:/temp/n.npy'
    >>> assert encoded['data'].a[0]['path'] == 'c:/temp/data/a/0.parquet'
    >>> assert encoded['other']['df']['path'] == 'c:/temp/other/df.parquet'

    >>> decoded = decode(encoded)
    >>> assert eq(decoded, value)

    """
    if '@' in path:
        path, asof = path.split('@')
    if path.endswith(_parquet):
        path = path[:-len(_parquet)]
    if path.endswith('/'):
        path = path[:-1]
    if is_pd(value):
        path = root_path_check(path)
        path = pd_to_parquet_twice(value, path + _parquet, asof = asof)
        if asof is None:
            return dict(_obj = _pd_read_parquet, path = path)
        else:
            return dict(_obj = _pd_read_parquet, path = path, asof = dt())
    elif is_arr(value):
        path = root_path_check(path)
        mkdir(path + _npy)
        np.save(path + _npy, value)
        return dict(_obj = _np_load, file = path + _npy)        
    elif is_dict(value):
        res = type(value)(**{k : parquet_encode(v, '%s/%s'%(path,k), compression, asof = asof) for k, v in value.items()})
        if isinstance(value, dictable):
            df = pd.DataFrame(res)
            return dict(_obj = _dictable_decode,
                        df = dict(_obj = _pd_read_parquet, 
                                  path = pd_to_parquet_twice(df, path + _dictable)))
        return res
    elif isinstance(value, (list, tuple)):
        return type(value)([parquet_encode(v, '%s/%i'%(path,i), compression, asof = asof) for i, v in enumerate(value)])
    else:
        return value
    
def npy_encode(value, path, append = False):
    """
    >>> from pyg_base import * 
    >>> value = pd.Series([1,2,3,4], drange(-3))

    """
    mode = 'a' if append else 'w'
    if path.endswith(_npy):
        path = path[:-len(_npy)]
    if path.endswith('/'):
        path = path[:-1]
    if is_pd(value):
        path = root_path_check(path)
        res = pd_to_npy(value, path, mode = mode)
        res[_obj] = _pd_read_npy
        return res
    elif is_arr(value):
        path = root_path_check(path)
        fname = path + _npy 
        np_save(fname, value, mode = mode)
        return dict(_obj = _np_load, file = fname)        
    elif is_dict(value):
        res = type(value)(**{k : npy_encode(v, '%s/%s'%(path,k), append = append) for k, v in value.items()})
        if isinstance(value, dictable):
            df = pd.DataFrame(res)
            return dict(_obj = _dictable_decode,
                        df = dict(_obj = _pd_read_parquet, path = pd_to_parquet_twice(df, path + _dictable)))
        return res
    elif isinstance(value, (list, tuple)):
        return type(value)([npy_encode(v, '%s/%i'%(path,i), append = append) for i, v in enumerate(value)])
    else:
        return value
    

def csv_encode(value, path, asof = None):
    """
    encodes a single DataFrame or a document containing dataframes into a an abject that can be decoded while saving dataframes into csv
    
    >>> path = 'c:/temp'
    >>> value = dict(key = 'a', data = dictable(a = [pd.Series([1,2,3]), pd.Series([4,5,6])], b = [1,2]), other = dict(df = pd.DataFrame(dict(a=[1,2,3], b= [4,5,6]))))
    >>> encoded = csv_encode(value, path)
    >>> assert encoded['data'].a[0]['path'] == 'c:/temp/data/a/0.csv'
    >>> assert encoded['other']['df']['path'] == 'c:/temp/other/df.csv'

    >>> decoded = decode(encoded)
    >>> assert eq(decoded, value)
    """
    if '@' in path:
        path, asof = path.split('@')
    if path.endswith(_csv):
        path = path[:-len(_csv)]
    if path.endswith('/'):
        path = path[:-1]
    if is_pd(value):
        path = root_path_check(path)
        path = pd_to_csv(value, path, asof = asof)
        if asof is None:
            return dict(_obj = _pd_read_csv, path = path)
        else:
            return dict(_obj = _pd_read_csv, path = path, asof = dt())
    elif is_dict(value):
        res = type(value)(**{k : csv_encode(v, '%s/%s'%(path,k)) for k, v in value.items()})
        if isinstance(value, dictable):
            df = pd.DataFrame(res)
            return dict(_obj = _dictable_decode, 
                        df = dict(_obj = _pd_read_csv, path = pd_to_csv(df, path)))
        return res
    elif isinstance(value, (list, tuple)):
        return type(value)([csv_encode(v, '%s/%i'%(path,i)) for i, v in enumerate(value)])
    else:
        return value

def cell_root(doc, root = None):
    if _root in doc:
        root  = doc[_root]
    if root is None and _db in doc and isinstance(doc[_db], partial):
        keywords = doc[_db].keywords
        if _root in keywords:
            root = keywords[_root]
        elif _writer in keywords:
            root = keywords[_writer]
    return root

def npy_write(doc, root = None, append = True, asof = None):
    """
    MongoDB is great for manipulating/searching dict keys/values. 
    However, the actual dataframes in each doc, we may want to save in a file system. 
    - The DataFrames are stored as bytes in MongoDB anyway, so they are not searchable
    - Storing in files allows other non-python/non-MongoDB users easier access, allowing data to be detached from app
    - MongoDB free version has limitations on size of document
    - file based system may be faster, especially if saved locally not over network
    - for data licensing issues, data must not sit on servers but stored on local computer

    Therefore, the doc encode will cycle through the elements in the doc. Each time it sees a pd.DataFrame/pd.Series, it will 
    - determine where to write it (with the help of the doc)
    - save it to a .parquet file

    >>> from pyg_base import *
    >>> from pyg_mongo import * 
    >>> db = mongo_table(db = 'temp', table = 'temp', pk = 'key', writer = 'c:/temp/%key.npy')         
    >>> a = pd.DataFrame(dict(a = [1,2,3], b= [4,5,6]), index = drange(2)); b = pd.DataFrame(np.random.normal(0,1,(3,2)), columns = ['a','b'], index = drange(2))
    >>> doc = dict(a = a, b = b, c = add_(a,b), key = 'b')
    >>> path ='c:/temp/%key'

    """
    root = cell_root(doc, root)
    if root is None:
        return doc
    path = root_path(doc, root)
    return npy_encode(doc, path, append = append)



def pickle_write(doc, root = None, asof = None):
    """
    MongoDB is great for manipulating/searching dict keys/values. 
    However, the actual dataframes in each doc, we may want to save in a file system. 
    - The DataFrames are stored as bytes in MongoDB anyway, so they are not searchable
    - Storing in files allows other non-python/non-MongoDB users easier access, allowing data to be detached from app
    - MongoDB free version has limitations on size of document
    - file based system may be faster, especially if saved locally not over network
    - for data licensing issues, data must not sit on servers but stored on local computer

    Therefore, the doc encode will cycle through the elements in the doc. Each time it sees a pd.DataFrame/pd.Series, it will 
    - determine where to write it (with the help of the doc)
    - save it to a .parquet file

    >>> from pyg_base import *
    >>> from pyg_mongo import * 
    >>> db = mongo_table(db = 'temp', table = 'temp', pk = 'key', writer = 'c:/temp/%key.pickle')         
    >>> a = pd.DataFrame(dict(a = [1,2,3], b= [4,5,6]), index = drange(2)); b = pd.DataFrame(np.random.normal(0,1,(3,2)), columns = ['a','b'], index = drange(2))
    >>> doc = dict(a = a, b = b, c = add_(a,b), key = 'b')
    >>> path ='c:/temp/%key'

    """
    root = cell_root(doc, root)
    if root is None:
        return doc
    path = root_path(doc, root)
    return pickle_encode(doc, path, asof = asof)


def parquet_write(doc, root = None, asof = None):
    """
    MongoDB is great for manipulating/searching dict keys/values. 
    However, the actual dataframes in each doc, we may want to save in a file system. 
    - The DataFrames are stored as bytes in MongoDB anyway, so they are not searchable
    - Storing in files allows other non-python/non-MongoDB users easier access, allowing data to be detached from app
    - MongoDB free version has limitations on size of document
    - file based system may be faster, especially if saved locally not over network
    - for data licensing issues, data must not sit on servers but stored on local computer

    Therefore, the doc encode will cycle through the elements in the doc. Each time it sees a pd.DataFrame/pd.Series, it will 
    - determine where to write it (with the help of the doc)
    - save it to a .parquet file

    >>> from pyg_base import *
    >>> from pyg_mongo import * 
    >>> db = mongo_table(db = 'temp', table = 'temp', pk = 'key', writer = 'c:/temp/%key.parquet')         
    >>> a = pd.DataFrame(dict(a = [1,2,3], b= [4,5,6]), index = drange(2)); b = pd.DataFrame(np.random.normal(0,1,(3,2)), columns = ['a','b'], index = drange(2))
    >>> doc = dict(a = a, b = b, c = add_(a,b), key = 'b')
    >>> path ='c:/temp/%key'

    """
    root = cell_root(doc, root)
    if root is None:
        return doc
    path = root_path(doc, root)
    return parquet_encode(doc, path, asof = asof)

def csv_write(doc, root = None, asof = None):
    """
    MongoDB is great for manipulating/searching dict keys/values. 
    However, the actual dataframes in each doc, we may want to save in a file system. 
    - The DataFrames are stored as bytes in MongoDB anyway, so they are not searchable
    - Storing in files allows other non-python/non-MongoDB users easier access, allowing data to be detached from orignal application
    - MongoDB free version has limitations on size of document
    - file based system may be faster, especially if saved locally not over network
    - for data licensing issues, data must not sit on servers but stored on local computer

    Therefore, the doc encode will cycle through the elements in the doc. Each time it sees a pd.DataFrame/pd.Series, it will 
    - determine where to write it (with the help of the doc)
    - save it to a .csv file

    """
    root = cell_root(doc, root)
    if root is None:
        return doc
    path = root_path(doc, root)
    return csv_encode(doc, path, asof = asof)


