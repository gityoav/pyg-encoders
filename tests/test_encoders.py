from pyg_base import eq, decode, dt, drange, encode, passthru
from pyg_encoders import parquet_write, csv_write, npy_write, root_path, parquet_encode, csv_encode, as_writer, as_reader

# import Dict, pd_read_parquet, parquet_write, mongo_table, dictable, eq, passthru, cell, drange, root_path, dt, parquet_encode, csv_encode
import pandas as pd
import numpy as np
import pytest
from functools import partial

df = pd.DataFrame(dict(a = [1.,np.nan,3], b = ['a', 'b', 'c'], c = drange(2)), index = drange(2))
s = pd.Series([1.,np.nan,3], drange(2))
doc = dict(df = df, s = s, key1 = 'a', key2 = 'b', x = dict(y = s * 2))

def test_root_path():
    root = 'c:/%school/%pupil.name/%pupil.surname/' 
    doc = dict(school = 'kings',  
               pupil = dict(name = 'yoav', surname = 'git'),  
               grades = dict(maths = 100, physics = 20, chemistry = 80),  
               report = dict(date = dt(2000,1,1),  
                             teacher = dict(name = 'adam', surname = 'cohen') 
                             ) 

                ) 

    assert root_path(doc, root) == 'c:/kings/yoav/git/' 
    root = 'c:/%school/%pupil.name_%pupil.surname/' 
    assert root_path(doc, root) == 'c:/kings/yoav_git/' 
    root = 'c:/archive/%report.date/%pupil.name.%pupil.surname/' 
    assert root_path(doc, root, '%Y') == 'c:/archive/2000/yoav.git/'  # can choose to format dates by providing a fmt. 

    root = 'c:/%name/%surname/%age/'
    doc = dict(name = 'yoav', surname = 'git')
    with pytest.raises(ValueError):
        root_path(doc, root)


def test_parquet_write():
    root = 'c:/test/%key1/%key2.parquet'
    res = parquet_write(doc, root)
    assert res['df']['path'] == 'c:/test/a/b/df.parquet'
    assert eq(df, decode(res['df']))

def test_csv_write():
    root = 'c:/test/%key1/%key2.csv'
    res = csv_write(doc, root)
    assert res['df']['path'] == 'c:/test/a/b/df.csv'
    assert isinstance(decode(res['df']).c.values[0], str) ## date columns are back as string from csv
    assert eq(df.a, decode(res['df']).a)
    

def test_parquet_encode():
    value = dict(a = 1)
    path = 'c:/.parquet'
    assert parquet_encode(value, path) == value
    assert csv_encode(value, path) == value

def test_as_writer():
    assert as_writer(None) == [encode]
    assert as_writer(False) == [passthru]
    assert eq(as_writer('c:/temp.npy') , [partial(npy_write, append = False, root = 'c:/temp'), encode])
    assert eq(as_writer('c:/temp.npa') , [partial(npy_write, append = True, root = 'c:/temp'), encode])
    assert eq(as_writer('c:/temp.csv') , [partial(csv_write, root = 'c:/temp'), encode])
    assert eq(as_writer('c:/temp.parquet') , [partial(parquet_write, root = 'c:/temp'), encode])
    assert eq(as_writer('c:/temp/%key1/%key2.npy', kwargs = dict(key1 = 'a')) , [partial(npy_write, append = False, root = 'c:/temp/a/%key2'), encode]) ## partial complettion of root

def test_as_reader():
    assert as_reader(None) == [decode]
    assert as_reader(False) == [passthru]
