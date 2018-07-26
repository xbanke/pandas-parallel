#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  quantpy
@file:    timeout.py
@time:    2018-07-03 14:26
"""
from functools import wraps

from contextlib import contextmanager
import threading
import _thread


@contextmanager
def timeout_context(seconds, msg=''):
    """超时环境管理器"""
    timer = threading.Timer(seconds, lambda: _thread.interrupt_main())
    timer.start()

    try:
        yield
    except KeyboardInterrupt:
        raise TimeoutError(f'timeout for operation {msg}')
    finally:
        timer.cancel()


def timeout_decorator(seconds, msg=''):
    """超时装饰器"""
    def decorate(func):
        @wraps(func)
        def decorated_func(*args, **kwargs):
            try:
                if seconds > 0:
                    with timeout_context(seconds, msg):
                        ret = func(*args, **kwargs)
                else:
                    ret = func(*args, **kwargs)
            except TimeoutError as e:
                ret = e
            return ret
        return decorated_func
    return decorate


class TimeoutJob(object):
    def __init__(self, func, args: tuple, kwargs: dict):
        """"""
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def get(self, timeout=0xffff):
        ret = timeout_decorator(timeout)(self.func)(*self.args, **self.kwargs)
        return ret

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)
