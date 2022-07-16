import pandas as pd
import numpy as np
from pyg_encoders._parquet import pd_to_parquet, pd_read_parquet
from pyg_encoders._encode import encode
from pyg_base import is_pd, is_dict, is_series, is_arr, is_date, dt2str, tree_items
from pyg_npy import pd_to_npy, np_save, pd_read_npy, mkdir
from functools import partial
import pickle

_pickle = '.pickle'
_parquet = '.parquet'
_npy = '.npy'; _npa = '.npa'
_csv = '.csv'
_series = '_is_series'
_root = 'root'
_db = 'db'
_obj = '_obj'
_writer = 'writer'

__all__ = ['root_path', 'pd_to_csv', 'pd_read_csv', 'parquet_encode', 'parquet_write', 'csv_encode', 'csv_write', 'pickle_dump', 'pickle_load']


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

def _check_path(path):
    if '%' in path:
        raise ValueError('The document did not contain enough keys to determine the path %s'%path)
    return path

def pd_to_csv(value, path):
    """
    A small utility to write both pd.Series and pd.DataFrame to csv files
    """
    assert is_pd(value), 'cannot save non-pd'
    if is_series(value):
        value.index.name = _series
    if value.index.name is None:
        value.index.name = 'index'
    if path[-4:].lower()!=_csv:
        path = path + _csv
    mkdir(path)
    value.to_csv(path)
    return path


def pickle_dump(value, path):
    mkdir(path)
    with open(path, 'wb') as f:
        pickle.dump(value, f)
    return path

def pickle_load(path):
    with open(path) as f:
        df = pickle.load(f)
    return df

def pd_read_csv(path):
    """
    A small utility to read both pd.Series and pd.DataFrame from csv files
    """
    res = pd.read_csv(path)
    if res.columns[0] == _series and res.shape[1] == 2:
        res = pd.Series(res[res.columns[1]], res[_series].values)
        return res
    if res.columns[0] == 'index':
        res = res.set_index('index')
    return res

_pd_read_csv = encode(pd_read_csv)
_pd_read_parquet = encode(pd_read_parquet)
_pd_read_npy = encode(pd_read_npy)
_pickle_load = encode(pickle_load)
_np_load = encode(np.load)


def pickle_encode(value, path):
    """
    encodes a single DataFrame or a document containing dataframes into a an abject of multiple pickled files that can be decoded
    """
    if path.endswith(_pickle):
        path = path[:-len(_pickle)]
    if path.endswith('/'):
        path = path[:-1]
    if is_pd(value):
        path = _check_path(path)
        return dict(_obj = _pickle_load, path = pickle_dump(value, path + _pickle))
    elif is_arr(value):
        path = _check_path(path)
        mkdir(path + _npy)
        np.save(path + _npy, value)
        return dict(_obj = _np_load, file = path + _npy)        
    elif is_dict(value):
        return type(value)(**{k : pickle_encode(v, '%s/%s'%(path,k)) for k, v in value.items()})
    elif isinstance(value, (list, tuple)):
        return type(value)([pickle_encode(v, '%s/%i'%(path,i)) for i, v in enumerate(value)])
    else:
        return value

    
def parquet_encode(value, path, compression = 'GZIP'):
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
    if path.endswith(_parquet):
        path = path[:-len(_parquet)]
    if path.endswith('/'):
        path = path[:-1]
    if is_pd(value):
        path = _check_path(path)
        return dict(_obj = _pd_read_parquet, path = pd_to_parquet(value, path + _parquet))
    elif is_arr(value):
        path = _check_path(path)
        mkdir(path + _npy)
        np.save(path + _npy, value)
        return dict(_obj = _np_load, file = path + _npy)        
    elif is_dict(value):
        return type(value)(**{k : parquet_encode(v, '%s/%s'%(path,k), compression) for k, v in value.items()})
    elif isinstance(value, (list, tuple)):
        return type(value)([parquet_encode(v, '%s/%i'%(path,i), compression) for i, v in enumerate(value)])
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
        path = _check_path(path)
        res = pd_to_npy(value, path, mode = mode)
        res[_obj] = _pd_read_npy
        return res
    elif is_arr(value):
        path = _check_path(path)
        fname = path + _npy 
        np_save(fname, value, mode = mode)
        return dict(_obj = _np_load, file = fname)        
    elif is_dict(value):
        return type(value)(**{k : npy_encode(v, '%s/%s'%(path,k), append = append) for k, v in value.items()})
    elif isinstance(value, (list, tuple)):
        return type(value)([npy_encode(v, '%s/%i'%(path,i), append = append) for i, v in enumerate(value)])
    else:
        return value
    

def csv_encode(value, path):
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
    if path.endswith(_csv):
        path = path[:-len(_csv)]
    if path.endswith('/'):
        path = path[:-1]
    if is_pd(value):
        path = _check_path(path)
        return dict(_obj = _pd_read_csv, path = pd_to_csv(value, path))
    elif is_dict(value):
        return type(value)(**{k : csv_encode(v, '%s/%s'%(path,k)) for k, v in value.items()})
    elif isinstance(value, (list, tuple)):
        return type(value)([csv_encode(v, '%s/%i'%(path,i)) for i, v in enumerate(value)])
    else:
        return value

def _find_root(doc, root = None):
    if _root in doc:
        root  = doc[_root]
    if root is None and _db in doc and isinstance(doc[_db], partial):
        keywords = doc[_db].keywords
        if _root in keywords:
            root = keywords[_root]
        elif _writer in keywords:
            root = keywords[_writer]
    return root

def npy_write(doc, root = None, append = True):
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
    root = _find_root(doc, root)
    if root is None:
        return doc
    path = root_path(doc, root)
    return npy_encode(doc, path, append = append)



def pickle_write(doc, root = None):
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
    root = _find_root(doc, root)
    if root is None:
        return doc
    path = root_path(doc, root)
    return pickle_encode(doc, path)


def parquet_write(doc, root = None):
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
    root = _find_root(doc, root)
    if root is None:
        return doc
    path = root_path(doc, root)
    return parquet_encode(doc, path)

def csv_write(doc, root = None):
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
    root = _find_root(doc, root)
    if root is None:
        return doc
    path = root_path(doc, root)
    return csv_encode(doc, path)


