import threading
from collections import defaultdict
import pickle
import numpy as np
import pandas as pd
import jsonpickle as jp
from pyg_npy import np_save, pd_read_npy, pd_to_npy
import json


_LOCKS = defaultdict(threading.Lock)
# -*- coding: utf-8 -*-

### writers

def _locked_to_csv(value, path, **params):
    with _LOCKS[path]:
        value.to_csv(path, **params)
    return path


def _locked_np_save(value, path, mode = 'w'):
    with _LOCKS[path]:
        np_save(path = path, value = value, mode = mode)
    return path


def _locked_to_parquet(value, path, compression = 'GZIP'):
    with _LOCKS[path]:
        try:
            value.to_parquet(path, compression  = compression)
        except Exception:            
            df = value.copy()
            df.columns = [jp.dumps(col) for col in df.columns]
            df.to_parquet(path, compression  = compression)
    return path

    
def _locked_to_pickle(value, path):
    with _LOCKS[path]:
        if hasattr(value, 'to_pickle'):
            value.to_pickle(path) # use object specific implementation if available
        else:
            with open(path, 'wb') as f:
                pickle.dump(value, f)
    return path

def _locked_json_dumps(value, path):
    with _LOCKS[path]:
        json.dump(value, path)
    return path


def _locked_pd_to_npy(value, path, mode='w', check=True):
    with _LOCKS[path]:
        pd_to_npy(value, path, mode=mode, check=check)
    return path
                
### readers
                
def _locked_pd_read_npy(path, columns = None, index=None, latest=None, allow_pickle=False, allow_async=False, **kwargs):
    with _LOCKS[path]:
        df = pd_read_npy(path, columns = columns, index=index, latest=latest, allow_pickle=allow_pickle, allow_async=allow_async, **kwargs)
    return df


def _locked_read_pickle(path):
    with _LOCKS[path]:
        try:
            with open(path) as f:
                df = pickle.load(f)
        except Exception: #pandas read_pickle sometimes work when pickle.load fails
            df = pd.read_pickle(path)
    return df


def _locked_read_csv(path):
    with _LOCKS[path]:
        df = pd.read_csv(path)
    return df


def _locked_read_parquet(path):
    with _LOCKS[path]:
        df = pd.read_parquet(path)
    return df
    

def _locked_json_load(path):
    with _LOCKS[path]:
        with open(path, 'r') as fp:
            j = json.load(fp)
    return j