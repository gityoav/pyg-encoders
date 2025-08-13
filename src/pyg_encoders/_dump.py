# -*- coding: utf-8 -*-
from numpy import nan, inf, isnan
import re
from pyg_base import is_str, is_int, is_date, dt, as_float, is_num, dt2str, is_bool
import json

CONSTS = dict(true = True, false = False, none = None, nan = nan, inf = inf)

_is_date1 = re.compile('^[1-2][0-9]{3}-[0-9]{1,2}-[0-9]{1,2}$')
_is_date2 = re.compile('^[1-2][0-9]{3}/[0-9]{1,2}/[0-9]{1,2}$')
_is_date3 = re.compile('^[1-2][0-9]{3}[0-1][0-9][0-3][0-9]$')
_is_date4 = re.compile('^[1-2][0-9]{3}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]{1}$')
_is_date5 = re.compile('^[1-2][0-9]{3}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]{1}.[0-9]{6}$')
_is_date4_ = re.compile('^[1-2][0-9]{3}-[0-1][0-9]-[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9]{1}$')
_is_date5_ = re.compile('^[1-2][0-9]{3}-[0-1][0-9]-[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9]{1}.[0-9]{6}$')


_dates = [_is_date1, _is_date2, _is_date3, _is_date4, _is_date5, _is_date4_, _is_date5_]
_is_e = re.compile('^[-/+]{0,1}[0-9]+[.]{0,1}[0-9]*e[-]{0,1}[0-9][1-9]{0,1}$')



def _json_loads(value):
    """
    >>> assert _json_loads("{'a':1}") == {'a':1}
    """
    try:
        return json.loads(value)
    except json.decoder.JSONDecodeError:
        if "'" in value:
            value = value.replace("'", '"')
        while ': ' in value:
            value = value.replace(': ', '')
        value = value.replace(':', ': ')
        return json.loads(value)

def _is_date(value):
    if is_str(value):
        for pattern in _dates:
            if pattern.search(value) is not None:
                return True
    elif is_int(value) and value > 15000100 and value<25001231:
        return True
    elif is_date(value):
        return True
    return False        
        

_bools = dict(n = False, no = False, false = False, f = False, t = True, true = True, y = True, yes = True)

def as_bool(value):
    if value is None:
        return None
    if is_str(value):
        return _bools[value.strip().lower()]
    else:
        return bool(value)

_casts = dict(float = as_float, 
              int = lambda value: int(value),
              dt = dt, 
              date = lambda value: dt(value).date(),
              bool = as_bool, 
              str = str)


def load(value, tokens = None):
    """
    This function handles loading of data which can be written without quotes and is very brief. It is paired with dump which creates brief strings
    
    >>> assert load('{a:1, b:true, c:hello, d:1985-12-31}') == {'a': 1, 'b': True, 'c': 'hello', 'd': dt(1985,12,31)}
    >>> assert load('{a:1, b:[true,true,false], c:hello, d:1985-12-31}') == {'a': 1, 'b': [True,True,False], 'c': 'hello', 'd': dt(1985,12,31)}
    >>> assert load('{a:1, b:[bool(t),bool(no)], c:hello, d:1985-12-31}') == {'a': 1, 'b': [True,False], 'c': 'hello', 'd': dt(1985,12,31)}
    >>> assert load('{a:[1,hi,nan], b:[true,true,false], c:hello, d:date(19851231)}') == {'a': [1,'hi',np.nan],'b': [True,True,False], 'c': 'hello', 'd': dt(1985,12,31).date()}
    """
    tokens = tokens or {}
    value = value.strip()
    while value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    while value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    if value.lower() in CONSTS:
        return CONSTS[value.lower()]
    if value.startswith('-') or value.startswith('+'):
        if value.replace(' ', '')[1:].isdigit():
            return int(value.replace(' ', ''))
    if _is_date(value):
        return dt(value)
    if value.isdigit():
        return int(value)
    if value.replace('.', '').replace('-','').isdigit() or _is_e.search(value) is not None:
        return float(value)
    try:
        return _json_loads(value)
    except json.decoder.JSONDecodeError:
        pass
    if value.startswith('@') and value[1:].split(' ')[0].isdigit() and int(value[1:].split(' ')[0]) in tokens:
        return tokens[int(value[1:].split(' ')[0])]
    if value.endswith(')'):
        for name, func in _casts.items():
            if value.startswith(name+ '('):
                return func(load(value[len(name)+1:-1], tokens))
    for pair in ['{}', '[]']:
        lhs, rhs = pair
        while lhs in value[1:] and rhs in value[:-1]:
            ub = value.index(rhs)+1
            sub = value[:ub]
            lb = len(sub) - sub[::-1].index(lhs)-1
            sub = sub[lb:]
            token = load(sub, tokens)
            key = max(tokens, default = 0) + 1
            tokens[key] = token
            value = value[:lb] + f'@{key}' + value[ub:]
    if value.startswith('{') and value.endswith('}'):
        pairs = [pair.split(':') for pair in value[1:-1].split(',')]
        return {load(pair[0], tokens) : load(':'.join(pair[1:]),tokens) for pair in pairs}
    if value.startswith('[') and value.endswith(']'):
        return [load(v, tokens) for v in value[1:-1].split(',')]
    return value

    
def dump(value):
    """
    value = [2, 3, dict(a = 1, b = dt(0), c = True, d = np.nan, t = 'text', n = None, i = np.inf)]
    assert dump(value) == '[2, 3, {a:1, b:20250811, c:true, d:nan, t:text, n:none, i:inf}]'
    assert load(dump(value)) == value
    """
    if value is None:
        return 'none'
    elif is_num(value) and isnan(value):
        return 'nan'
    elif is_date(value):
        return dt2str(value)
    elif is_bool(value):
        return str(value).lower()
    elif isinstance(value, (list,tuple)):
        res = ', '.join([dump(v) for v in value])
        return f'[{res}]'
    elif isinstance(value, dict):
        res = ', '.join([f'{dump(k)}:{dump(v)}' for k,v in sorted(value.items())])
        return '{' + res + '}'
    else:
        return str(value)          
    