#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  quantpy
@file:    apply_parallel.py
@time:    2018/4/11 12:41
"""
import os
import warnings
import traceback

from functools import partial
import multiprocessing

import numpy as np
import pandas as pd
from pandas.core.groupby import GroupBy

# import dill
from tqdm import tqdm

from .timeout import TimeoutJob


def get_worker_count(n_workers=None):
    if n_workers is None:
        cpu_count = os.cpu_count() or 1
        n_workers = cpu_count - 1 or 1
    return max(min(n_workers, os.cpu_count()), 1)


def items_apply_parallel(iteritems, func, *args, n_workers, progress=False, **kwargs):
    n_workers = get_worker_count(n_workers)
    if n_workers < 2:
        apply = TimeoutJob
    else:
        pool = multiprocessing.Pool(n_workers)
        apply = pool.apply_async
    to_do = {key: apply(func, (df, ) + args, kwargs) for key, df in iteritems}
    if n_workers >= 2:
        pool.close()

    res_dict, err_dict = {}, {}
    with tqdm(to_do.items(), ascii=True, disable=not progress) as iterable:
        for key, res in iterable:
            iterable.set_description(str(key)[:10])
            # res_dict[key] = res.get(0xffff)
            try:
                res_dict[key] = res.get(0xffff)
            except KeyboardInterrupt:
                print('Job canceled, only part of the result will returned!')
                break
            except Exception as e:
                err_dict[key] = traceback.format_exc()
                warnings.warn(repr(e))

    return res_dict, err_dict


def df_group_apply_parallel(df_group: GroupBy, func, *args, n_workers=None, progress=False, **kwargs):
    if get_worker_count(n_workers) < 2:
        return df_group.apply(func, *args, **kwargs)
    names = df_group.grouper.names
    res_dict, err_dict = items_apply_parallel(df_group, func, *args, n_workers=n_workers, progress=progress, **kwargs)

    if not res_dict:
        return
    k, v = res_dict.popitem()
    res_dict[k] = v
    if isinstance(v, (pd.DataFrame, pd.Series)):
        res = pd.concat(res_dict)
    else:
        res = pd.Series(res_dict)
    if res.index.nlevels > 1:
        res.index.set_names(names, level=range(len(names)), inplace=True)
    else:
        res.index.names = names

    return res


def func_group(df, func, args, **kwargs):
    return df.apply(func, axis=1, args=args, **kwargs)


def df_apply_parallel(df: pd.DataFrame, func, *args, axis=0,
                      n_workers=None, group_size=None, group_count=None,
                      progress=False, **kwargs):

    # if axis in (1, 'columns'):
    #     df = df.T
    #
    # iteritems = df.iteritems()
    # n_items = df.shape[1]
    # names = list(df.columns.names)
    #
    # return _apply_parallel(iteritems, names, n_items, func, *args, n_workers=n_workers, progress=progress, **kwargs)

    n_workers = get_worker_count(n_workers)

    # if n_workers == 1:
    #     return df.apply(func, axis=axis, args=args, **kwargs)

    # transform = False
    if axis in (0, 'index'):
        df = df.T
        # transform = True

    job_count = df.shape[0]

    if group_size is None:
        if group_count is None:
            group_count = n_workers
        group_size = int(np.ceil(job_count / group_count))

    grouper = pd.Series(np.arange(job_count) // group_size, index=df.index)
    df_group = df.groupby(grouper)
    # f = lambda d: d.apply(func, axis=1, args=args, **kwargs)
    f = partial(func_group, func=func, args=args, **kwargs)

    ret = df_group.apply_parallel(f, n_workers=n_workers, progress=progress)
    ret.index = ret.index.droplevel(-1)
    # if transform:
    #     ret = ret.T

    return ret


pd.core.groupby.DataFrameGroupBy.apply_parallel = df_group_apply_parallel
pd.core.groupby.SeriesGroupBy.apply_parallel = df_group_apply_parallel
pd.DataFrame.apply_parallel = df_apply_parallel
pd.Series.apply_parallel = df_apply_parallel

