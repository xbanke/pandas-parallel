#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  quantpy
@file:    utils.py
@time:    2018/4/11 17:56
"""


import pandas as pd
import numpy as np


def sort(s):
    return pd.Series(np.sort(s.values), index=s.index)