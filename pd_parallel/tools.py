#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  quantpy
@file:    tools.py
@time:    2018/4/11 16:25
"""

import os
import pandas as pd
from .apply_parallel import df_group_apply_parallel


def get_grouper(df: pd.DataFrame, by=None, axis=0, level=None,
                section_size=None, section_count=os.cpu_count(), **kwargs):
    """regroup by df.groupby"""
    df_group = df.groupby(by=by, axis=axis, level=level, **kwargs)
    # n_groups = df_group.ngroups
    n_groups = df_group.count().shape[0]

    if section_size is None:
        section_size = n_groups // section_count

    grouper = [pd.Series(i // section_size, index=d.index) for i, (k, d) in enumerate(df_group)]
    grouper = pd.concat(grouper)
    return grouper


def double_groupby_apply_parallel(df: pd.DataFrame, func, *args, grouper_kws: dict, **kwargs):
    """"""
    grouper = get_grouper(df, **grouper_kws)
    df_group = df.groupby(grouper)
    ret = df_group_apply_parallel(df_group, func, *args, **kwargs)
    return ret

