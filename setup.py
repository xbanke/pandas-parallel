#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  quantpy
@file:    setup.py
@time:    2018/4/11 11:32
"""

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name='pandas-parallel',
    version='0.1',
    description='pandas apply parallel',
    url='https://github.com/xbanke/pandas-parallel',
    author='quantpy',
    author_email='quantpy@qq.com',
    license='MIT',
    packages=['pd_parallel'],
    keywords=['pandas', 'parallel'],
    install_requires=['pandas', 'dill'],
    zip_safe=False,
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries'
    ]
)
