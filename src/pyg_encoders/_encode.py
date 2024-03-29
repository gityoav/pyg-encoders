import jsonpickle as jp
import re
from pyg_base import is_int, cache_func, cache, is_float, is_str, is_date, is_bool, is_pd, is_arr, as_list , dt, iso, uk2dt, dt2str, try_back, logger, loop, getargs, dictable, as_primitive

import pickle
import datetime
from functools import partial
from enum import Enum
import numpy as np
import json

_obj = '_obj'
_data = 'data'
iso_quote = re.compile('^"[0-9]{4}-[0-9]{2}-[0-9]{2}T')

__all__ = ['encode', 'decode', 'pd2bson', 'bson2pd', 'bson2np', 'dumps', 'loads']

@try_back
def decode_str(value):
    """
    A safer version of jp.decode

    :Parameters:
    ----------------
    value : str
        string to be decoded.

    :Returns:
    -------
    object
        value decoded or original value if failed.

    """
    res = jp.decode(value)
    if res is None and value!='null':
        logger.warning('could not decode value: %s'%value)
        return value
    else:
        return res

@loop(list, tuple)
def _decode(value, date = None):
    if is_str(value):
        if value.startswith('{'):
            value = decode_str(value)
            if not isinstance(value, str):
                value = _decode(value, date)
            return value
        elif value == 'null':
            return None
        if date in (None, False):
            return value
        elif date == 'iso' or date is True:
            if iso.search(value) is not None:
                return datetime.datetime.fromisoformat(value) 
            elif iso_quote.search(value) is not None:
                return datetime.datetime.fromisoformat(value.replace('"', ''))
            else:
                return value
        else:
            return value if date.search(value) is None else dt(value)
    elif isinstance(value, dict):
        res = type(value)(**{_decode(k, date) : _decode(v, date) for k, v in value.items()})
        if _obj in res.keys():
            obj = res.pop(_obj)
            if isinstance(obj, str): # we have been unable to convert to object
                obj = json.loads(obj)
            if isinstance(obj, dict) and not callable(obj): 
                v = list(obj.values())[0]
                try:
                    import pyg
                    obj = getattr(pyg, v.split('.')[-1])
                except:
                    raise ValueError('Unable to map "%s" into a valid object'%v)
            try:
                res = obj(**res)
            except TypeError: # function got an unexpected keys. This is because we do not delete old keys in documents
                args = getargs(obj)
                res = obj(**{k : v for k, v in res.items() if k in args})
        return res
    else:
        return value
    
def decode(value, date = None):
    """
    decodes a string or an object dict 

    :Parameters:
    -------------
    value : str or dict
        usually a json
    date : None, bool or a regex expression, optional
        date format to be decoded
        
    :Returns:
    -------
    obj
        the json decoded.
    
    :Examples:
    --------------
    >>> from pyg import *
    >>> class temp(dict):
    >>>    pass
    
    >>> orig = temp(a = 1, b = dt(0))
    >>> encoded = encode(orig)
    >>> assert eq(decode(encoded), orig) # type matching too...
    
    
    >>> from pyg import * 
    >>> f = cache(add_)
    >>> decode(encode(f)) == cache(add_)
    >>> f(1,2)
    >>> decode(encode(f)) == cache(add_)
    
    >>> g = partial(f, b = 2)
    >>> assert not eq(decode(encode(g)) , g) ## because g has a cache
    >>> assert eq(decode(encode(g)) , partial(cache(add_), b = 2))
    
    """
    return _decode(value, date = date)

loads = partial(decode, date = True)
def partial_(func, args, keywords):
    return partial(func, *args, **keywords)

@loop(list, tuple)
def _encode(value, unchanged = None, unchanged_keys = None):
    if hasattr(value, '_encode') and not isinstance(value, type):
        res = value._encode
        if not isinstance(res, str):
            res = res()
        return res
    if is_bool(value):
        return True if value else False
    elif is_int(value):
        return int(value)
    elif is_float(value):
        return float(value)
    elif is_date(value):
        return value if isinstance(value, datetime.datetime) else dt(value) 
    elif isinstance(value, Enum):
        return _encode(value.value)
    elif value is None or is_str(value):
        return value
    elif unchanged and isinstance(value, unchanged):
          return value
    elif isinstance(value, dictable):
        res = {k : v if unchanged_keys and k in unchanged_keys else _encode(v, unchanged, unchanged_keys) for k, v in value.items()}
        if _obj not in res:
            res[_obj] = _encode(type(value))
        res['columns'] = value.columns
        return res    
    elif isinstance(value, cache_func) and hasattr(value, 'cache') and len(value.cache):
        return _encode(cache(value.function), unchanged, unchanged_keys)
    elif isinstance(value, dict):
        unchanged_keys = as_list(unchanged_keys)
        res = {k : v if unchanged_keys and k in unchanged_keys else _encode(v, unchanged, unchanged_keys) for k, v in value.items()}
        if _obj not in res and type(value)!=dict:
            res[_obj] = _encode(type(value), unchanged, unchanged_keys)
        return res
    elif 'tensorflow.python.keras' in str(type(value)): ## A bit of a cheat not to have tensorflow explicit dependency
        res = _encode(model_to_config_and_weights(value), unchanged, unchanged_keys)
        res['_obj'] = _keras_from_config_and_weights
        return res        
    elif is_pd(value):
        return {_data : pd2bson(value), _obj : _bson2pd}
    elif is_arr(value):
        if value.dtype == np.dtype('O'):
            return {_data : pd2bson(value), _obj : _bson2pd}
        else:
            return {_data : value.tobytes(), 'shape' : value.shape, 'dtype' : encode(value.dtype), _obj : _bson2np}
    elif isinstance(value, partial):
        func = _encode(value.func, unchanged, unchanged_keys)
        args = _encode(value.args, unchanged, unchanged_keys)
        keywords = _encode(value.keywords, unchanged, unchanged_keys)
        res = dict(_obj = _partial, func = func, args = args, keywords = keywords)
        return res
    else:
        res = jp.encode(value)
        return res

_partial = _encode(partial_)


def encode(value, unchanged = None, unchanged_keys = None):
    """
    
    encode/decode are performed prior to sending to mongodb or after retrieval from db. 
    The idea is to make object embedding in Mongo transparent to the user.
    
    - We use jsonpickle package to embed general objects. These are encoded as strings and can be decoded as long as the original library exists when decoding.
    - pandas.DataFrame are encoded to bytes using pickle while numpy arrays are encoded using the faster array.tobytes() with arrays' shape & type exposed and searchable.
    
    :Example:
    ----------
    >>> from pyg import *; import numpy as np
    >>> value = Dict(a=1,b=2)
    >>> assert encode(value) == {'a': 1, 'b': 2, '_obj': '{"py/type": "pyg_base._dict.Dict"}'}
    >>> assert decode({'a': 1, 'b': 2, '_obj': '{"py/type": "pyg_base._dict.Dict"}'}) == Dict(a = 1, b=2)
    >>> value = dictable(a=[1,2,3], b = 4)
    >>> assert encode(value) == {'a': [1, 2, 3], 'b': [4, 4, 4], '_obj': '{"py/type": "pyg_base._dictable.dictable"}'}
    >>> assert decode(encode(value)) == value
    >>> assert encode(np.array([1,2])) ==  {'data': bytes,
    >>>                                     'shape': (2,),
    >>>                                     'dtype': '{"py/reduce": [{"py/type": "numpy.dtype"}, {"py/tuple": ["i4", false, true]}, {"py/tuple": [3, "<", null, null, null, -1, -1, 0]}]}',
    >>>                                     '_obj': '{"py/function": "pyg_base._encode.bson2np"}'}
    
    :Example: functions and objects
    -------------------------------
    >>> from pyg import *; import numpy as np
    >>> assert encode(ewma) == '{"py/function": "pyg.timeseries._ewm.ewma"}'
    >>> assert encode(Calendar) == '{"py/type": "pyg_base._drange.Calendar"}'
    
    :Parameters:
    ----------------
    value : obj
        An object to be encoded 
        
    :Returns:
    -------
    A pre-json object

    """
    return _encode(value, unchanged, unchanged_keys)

_uk2dt = encode(uk2dt)
_array = encode(np.array)

@loop(list, tuple, dict)
def _dumps(value):
    if is_date(value):
        return dict(_obj = _uk2dt, t = dt2str(value))
    elif isinstance(value, np.ndarray):
        if len(value.shape) == 0:
            return as_primitive(value)
        elif len(value.shape) == 1:
            return dict(_obj = _array, object = _dumps(list(value)))
        elif len(value.shape) == 2:
            return dict(_obj = _array, object = _dumps(list(map(list,value))))
        elif len(value.shape) == 3:
            return dict(_obj = _array, object = _dumps([list(map(list,v)) for v in value]))
        else:
            return as_primitive(value)
    else:
        return as_primitive(value)

def dumps(value):
    """
    an extended version of json.dumps, being able to handle dates and arrays
    """
    value = _dumps(value)
    return json.dumps(value)

@loop(list, tuple, dict)
def _loads(value):
    """
    an extended version of json.dumps, being able to handle dates and arrays
    """
    if isinstance(value, str):    
        value = json.loads(value)
        return decode(value)
    else:
        return value
    
def loads(value):
    return _loads(value)    


def model_to_config_and_weights(value):
    return dict(model = type(value), weights = value.get_weights(), config = value.get_config())

def pd2bson(value):
    """
    converts a value (usually a pandas.DataFrame/Series) to bytes using pickle
    """
    return pickle.dumps(value)


def pd2pa(value):
    """serialize using arrow. Slightly slower than pickle"""
    import pyarrow as pa
    buf = pa.serialize(value).to_buffer()
    res = buf.to_pybytes()
    return res

def pa2pd(value):
    """serialize using arrow. Slightly slower than pickle"""
    import pyarrow as pa
    buf = pa.py_buffer(memoryview(value))
    df = pa.deserialize(buf)
    return df


def np2bson(value):
    """
    converts a numpy array to bytes using value.tobytes(). This is much faster than pickle but does not save shape/type info which we save separately.
    """
    return value.tobytes()

def bson2np(data, dtype, shape):
    """
    converts a byte with dtype and shape information into a numpy array.
    
    """
    res = np.frombuffer(data, dtype = dtype)
    return np.reshape(res, shape) if len(shape)!=1 else res

def bson2pd(data):
    """
    converts a pickled object back to an object. We insist that new object has .shape to ensure we did not unpickle gibberish.
    """
    try:
        res = pickle.loads(data)
        res.shape
        return res
    except Exception:
        return None
    
def keras_from_config_and_weights(model, config, weights):
    """

    Parameters
    ----------
    model : keras model
        Keras model
    config : str
        json of the model.
    weights : list of numpy arrays 
        model weights

    Returns
    -------
    res : keras model

    """
    res = model.from_config(config)
    res.set_weights(weights)
    return res
    
_keras_from_config_and_weights = encode(keras_from_config_and_weights)
_bson2pd = encode(bson2pd)
_bson2np = encode(bson2np)
