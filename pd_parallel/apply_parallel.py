#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  quantpy
@file:    apply_parallel.py
@time:    2018/4/11 12:41
"""

import multiprocessing
import numpy as np
import pandas as pd
from pandas.core.groupby import GroupBy

# import dill
from tqdm import tqdm

from .utils import get_worker_count


# def job_worker(f_pickled, df: pd.DataFrame, *args, **kwargs):
#     func = dill.loads(f_pickled)
#     ret = func(df, *args, **kwargs)
#     return ret


def concat(res_dict: dict, with_keys=False):
    if not res_dict:
        return
    k, v = res_dict.popitem()
    res_dict[k] = v
    if isinstance(v, (pd.DataFrame, pd.Series)):
        if not with_keys:
            res_dict = list(res_dict.values())
        res = pd.concat(res_dict)
    else:
        res = pd.Series(res_dict)
    return res


def df_group_apply_parallel(df_group: GroupBy, func, *args, n_workers=None, progress=False, concat_keys=True, **kwargs):
    n_workers = get_worker_count(n_workers)
    if n_workers == 1:
        return df_group.apply(func, *args, **kwargs)
    # f_pickled = dill.dumps(func)
    pool = multiprocessing.Pool(n_workers)
    to_do = {key: pool.apply_async(func, (df, ) + args, kwargs) for key, df in df_group}
    pool.close()
    if progress:
        to_do = tqdm(to_do.items(), total=len(to_do), ascii=True)
    else:
        to_do = to_do.items()

    res_dict, err_dict = {}, {}
    for key, res in to_do:
        try:
            res_dict[key] = res.get(0xffff)
        except KeyboardInterrupt:
            print('Job canceled, only part of the result will returned!')
            break
        except Exception as e:
            print(repr(e))
            err_dict[key] = e
    return concat(res_dict, with_keys=concat_keys)


pd.core.groupby.DataFrameGroupBy.apply_parallel = df_group_apply_parallel
pd.core.groupby.SeriesGroupBy.apply_parallel = df_group_apply_parallel


def df_apply_parallel(df: pd.DataFrame, func, *args, axis=0,
                      n_workers=None, group_size=None, group_count=None,
                      progress=False, **kwargs):
    n_workers = get_worker_count(n_workers)

    if n_workers == 1:
        return df.apply(func, axis=axis, args=args, **kwargs)

    transform = False
    if axis in (0, 'index'):
        df = df.T
        transform = True

    job_count = df.shape[0]

    if group_size is None:
        if group_count is None:
            group_count = n_workers
        group_size = int(np.ceil(job_count / group_count))

    grouper = pd.Series(np.arange(job_count) // group_size, index=df.index)
    df_group = df.groupby(grouper)
    f = lambda d: d.apply(func, axis=1, args=args, **kwargs)

    ret = df_group.apply_parallel(f, n_workers=n_workers, progress=progress, concat_keys=True)

    if transform:
        ret = ret.T

    return ret


pd.DataFrame.apply_parallel = df_apply_parallel
pd.Series.apply_parallel = df_apply_parallel

