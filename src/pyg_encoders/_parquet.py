from pyg_npy import mkdir, path_name
from pyg_base._types import is_series, is_df, is_pd, is_int, is_date, is_bool, is_str, is_float
from pyg_base._dates import dt2str, dt
from pyg_base._logger import logger
from pyg_base._as_list import as_list
from pyg_base import try_none, bi_read, is_bi, bi_merge, Bi
from pyg_encoders._threads import executor_pool
import pandas as pd
import numpy as np
import jsonpickle as jp
from pyg_base._bitemporal import _series
import os

__all__ = ['pd_to_parquet', 'pd_read_parquet']


def _pd_to_parquet(value, path, compression = 'GZIP', asof = None, existing_data = 'shift'):
    if is_series(value):
        mkdir(path)
        df = pd.DataFrame(value)
        df.columns = [_series]
        try:
            df.to_parquet(path, compression = compression)
        except ValueError:
            df = pd.DataFrame({jp.dumps(k) : [v] for k,v in dict(value).items()})
            df[_series] = True
            df.to_parquet(path, compression = compression)
        return path
    elif is_df(value):
        if is_bi(value):
            old = try_none(_read_parquet)(path)
            value = bi_merge(old_data = old, new_data = value, asof = asof, existing_data = existing_data)
        mkdir(path)
        df = value.copy()
        try:
            df.to_parquet(path, compression  = compression)
        except Exception:            
            df.columns = [jp.dumps(col) for col in df.columns]
            df.to_parquet(path, compression  = compression)
        return path



def pd_to_parquet(value, path, compression = 'GZIP', asof = None, existing_data = 'shift', max_workers = 4, pool_name = None):
    """
    a small utility to save df to parquet, extending both pd.Series and non-string columns    

    Parameters
    -----------
    value: dataframe/series
        value to be saved to file
    
    path: str
        file location
    
    compression: str
        compression type
    
    asof:
        if not none, will convert value into a bitemporal dataframe using asof
    existing_data:
        policy for handling existing data if value is bitemporal.
        'overwrite/ignore': overwrite existing data
        0/False: ignore if not bitemporal itself, otherwise bi_merge
        
    
    :Example:
    -------
    >>> from pyg_base import *
    >>> import pandas as pd
    >>> import pytest

    >>> df = pd.DataFrame([[1,2],[3,4]], drange(-1), columns = [0, dt(0)])
    >>> s = pd.Series([1,2,3], drange(-2))

    >>> with pytest.raises(ValueError): ## must have string column names
            df.to_parquet('c:/temp/test.parquet')

    >>> with pytest.raises(AttributeError): ## pd.Series has no to_parquet
            s.to_parquet('c:/temp/test.parquet')
    
    >>> df_path = pd_to_parquet(df, 'c:/temp/df.parquet')
    >>> series_path = pd_to_parquet(s, 'c:/temp/series.parquet')

    >>> df2 = pd_read_parquet(df_path)
    >>> s2 = pd_read_parquet(series_path)

    >>> assert eq(df, df2)
    >>> assert eq(s, s2)
    
    :Example: using threading to save the data
    ---------
    >>> from pyg import *  
    >>> value = pd.DataFrame(np.random.normal(0,1,(10000,26)), columns = list(ALPHABET), index = drange(-9999))
    >>> path = 'c:/temp/test_thread.parquet'
    >>> blocking_time = timer(pd_to_parquet, n = 100, time = True)(value, path, max_workers = 0)
    >>> threading_time = timer(pd_to_parquet, n = 100, time = True)(value, path, max_workers = 4)
    >>> timer(lambda value, path: value.to_parquet(path), n = 10)(value, path)
    >>> assert blocking_time/threading_time > 10

    """
    if '@' in path:
        path, asof = path.split('@')
    if asof is not None:
        value = Bi(value, asof)
    if not is_pd(value):
        return value
    if max_workers == 0:
        _pd_to_parquet(value, path, compression = compression, asof = asof, existing_data = existing_data)
    else:
        executor_pool(max_workers, pool_name).submit(_pd_to_parquet, value, path, compression, asof, existing_data)
    return path



def _read_parquet(path):
    if not os.path.exists(path):
        return
    try:
        df = pd.read_parquet(path)
    except Exception:
        logger.warning('WARN: unable to read pd.read_parquet("%s")'%path)
        return None
    try:    
        df.columns = [jp.loads(col) for col in df.columns]
    except Exception:
        pass
    return df


def pd_read_parquet(path, asof = None, what = 'last', **kwargs):
    """
    a small utility to read df/series from parquet, extending both pd.Series and non-string columns 

    :Example:
    -------
    >>> from pyg import *
    >>> import pandas as pd
    >>> import pytest

    >>> df = pd.DataFrame([[1,2],[3,4]], drange(-1), columns = [0, dt(0)])
    >>> s = pd.Series([1,2,3], drange(-2))

    >>> with pytest.raises(ValueError): ## must have string column names
            df.to_parquet('c:/temp/test.parquet')

    >>> with pytest.raises(AttributeError): ## pd.Series has no to_parquet
            s.to_parquet('c:/temp/test.parquet')
    
    >>> df_path = pd_to_parquet(df, 'c:/temp/df.parquet')
    >>> series_path = pd_to_parquet(s, 'c:/temp/series.parquet')

    >>> df2 = pd_read_parquet(df_path)
    >>> s2 = pd_read_parquet(series_path)

    >>> assert eq(df, df2)
    >>> assert eq(s, s2)

    """
    path = path_name(path)
    df = _read_parquet(path)
    if asof is not None:
        df = bi_read(df, asof, what = what)
    if is_df(df):
        if df.columns[-1] == _series:
            if len(df.columns) == 1:
                res = df[_series]
                res.name = None
                return res
            else:
                return pd.Series({jp.loads(k) : df[k].values[0] for k in df.columns[:-1]})
    return df

