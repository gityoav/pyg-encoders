from pyg import * 

def f():
    t = dt()
    return pd.Series([dt().second]*10, drange(-9))


def test_bitemp_parquet():
    db = partial(mongo_table, db = 'test', table = 'test', pk = 'key',
                  writer = 'c:/test/%key.parquet@now')
    db().drop()
    db().deleted.drop()
    c = db_cell(f, db = db, key = 'bi').go()
    db()[0]
    assert eq(pd_read_parquet('c:/test/bi/data.parquet', dt()), c.data)


def test_bitemp_pickle():
    db = partial(mongo_table, db = 'test', table = 'test', pk = 'key',
                  writer = 'c:/test/%key.pickle@now')
    db().deleted.drop()
    db().drop()
    c = db_cell(f, db = db, key = 'bi').go()
    assert '_asof' in pickle_load('c:/test/bi/data.pickle').columns
    assert '_asof' not in pickle_load('c:/test/bi/data.pickle', asof = dt())
    c = db_cell(f, db = db, key = 'bi').go()
    db().deleted.inc(key = 'bi').read(0, passthru) 
    pickle_load('c:/test/bi/data.pickle')

