#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  quantpy
@file:    test.py
@time:    2018/4/11 17:42
"""


import numpy as np
import pandas as pd

import pd_parallel
from utils import sort


def main():
    n = 1000000
    df = pd.DataFrame(np.random.randn(n, 3), columns=list('ABC'), index=pd.date_range('20000101', periods=n, freq='1h'))
    ret = df.apply_parallel(sort, axis=1, progress=True)
    print('finished!', ret.head())


if __name__ == '__main__':
    main()