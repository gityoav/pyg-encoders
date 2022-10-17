from pyg import * 

def f():
    t = dt()
    return pd.Series([9]*10, drange(-9))


def test_bitemp():
    db = partial(mongo_table, db = 'test', table = 'test', pk = 'key',
                  writer = 'c:/test/%key.parquet@now')
    db().drop()
    c = db_cell(f, db = db, key = 'bi').go()
    db()[0]
    db().deleted.inc(key = 'bi')[-4]

    pd.read_parquet('c:/test/bi/data.parquet')

def test_bitemp_pickle():
    db = partial(mongo_table, db = 'test', table = 'test', pk = 'key',
                  writer = 'c:/test/%key.pickle@now')

    c = db_cell(f, db = db, key = 'bi').go()
    db().deleted[-2]


