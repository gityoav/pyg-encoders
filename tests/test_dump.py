from pyg_encoders import load, dump
from pyg_base import dt
import numpy as np

def test_load_dump():    
    value = [2, 3, dict(a = 1, b = dt(2025,8,1), c = True, d = np.nan, t = 'text', n = None, i = np.inf)]
    assert dump(value) == '[2, 3, {a:1, b:20250801, c:true, d:nan, t:text, n:none, i:inf}]'
    assert load(dump(value)) == value
    assert load('[2,+3,{a:1,b:2025-8-1,c:bool(t), d:nan, t:text, n:none, i:inf}]')
    assert load('[2,3e0,{a:1,b:2025-8-1,c:bool(t), d:nan, t:text, n:none, i:inf}]')
    assert load('[2,3e0,{a:1,b:2025-8-1,c:bool(t), d:nan, t:text, n:none, i:inf}]')
    assert load('[2,3e0,{a:1,b:2025/08/01,c:bool(t), d:nan, t:text, n:none, i:inf}]')
    assert load('[2,3e0,{a:1,b:2025-08-01 00:00:00,c:bool(t), d:nan, t:text, n:none, i:inf}]')
