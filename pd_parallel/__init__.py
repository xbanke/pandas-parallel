#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  quantpy
@file:    __init__.py
@time:    2018/4/11 12:08
"""


from .apply_parallel import df_group_apply_parallel, df_apply_parallel
from .tools import get_grouper, double_groupby_apply_parallel


__version__ = '0.1.3'