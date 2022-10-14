### we create an interface for read/write of pandas which is bitemporal
# the data

from pyg_base import is_ts, is_pd,dt,is_date
import pandas as pd

_asof = '_asof'

def is_bi(df):
    return is_pd(df) and _asof in df.columns

def bi_read(df, asof = None, what = 'last'):
    """
    Parameters
    ----------
    df : a timeseries
        an item we can read bitemporally    
    
    Example:
        df = pd.DataFrame(dict(a = [1,2,3,4,5,6], 
            _asof = drange(-2) + drange(-1) + drange(0)), 
            index = [dt(-2)] * 3 + [dt(-1)] * 2 + [dt(0)])
    
    """
    if not is_bi(df):
        return df
    if is_date(asof):
        df = df[df[_asof]<=asof]
    if is_bi(asof):
        df = df[df[_asof] <= asof.reindex(df.index)[_asof]]
    index_name = df.index.name
    if index_name is None:
        df.index.name = 'index'
    gb = df.sort_values(_asof).groupby(df.index.name)
    res = gb.apply(what)
    res = res.drop(columns = _asof)
    res.index.name = index_name
    return res

def bi_updates(new, old, asof = None):
    """
    >>> old = pd.DataFrame(dict(a = [1,2,3,4,5,6], 
            _asof = drange(-2) + drange(-1) + drange(0)), 
            index = [dt(-2)] * 3 + [dt(-1)] * 2 + [dt(0)])

    >>> new = pd.DataFrame(dict(a = [7,8,3,10],
                                _asof = drange(-2,1)), 
                           index = drange(-4,-1))
    """
    if _asof not in new:
        raw = new
        new = new.copy()
        new[_asof] = dt(asof)
    else:
        raw = new.drop(columns = _asof)

    prev = bi_read(old, asof = new, what = 'last')
    update = new[~(prev.reindex(raw.index) == raw).min(axis=1)]
    return update


