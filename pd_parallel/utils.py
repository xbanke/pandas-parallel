#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  quantpy
@file:    utils.py
@time:    2018/4/11 12:08
"""


import os


def get_worker_count(n_workers=None):
    if n_workers is None:
        n_workers = os.cpu_count() or 1
    return max(min(n_workers, os.cpu_count()), 1)
