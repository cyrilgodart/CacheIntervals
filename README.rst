****************
CacheIntervals
****************

.. image:: http://www.repostatus.org/badges/latest/active.svg
   :target: http://www.repostatus.org/#active
.. image:: https://travis-ci.org/cyril.godart@gmail.com/CacheIntervals.svg?branch=master
   :target: https://travis-ci.org/cyril.godart@gmail.com/CacheIntervals/
.. image:: https://codecov.io/gh/cyril.godart@gmail.com/CacheIntervals/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/cyril.godart@gmail.com/CacheIntervals
.. image:: https://readthedocs.org/projects/CacheIntervals/badge/?version=latest
   :target: http://CacheIntervals.readthedocs.io/en/latest/?badge=latest


Memoization with interval parameters

Introduction
============

CacheIntervals allows lazy evaluation of functions with interval parameters. Several strategies and options are available.

Usage
============

Similarly to many caching library, CacheIntervals provides memoization through a decorator mechanism.
The constructor of the memoization must specify:
    - the positon arguments that are intervals to be lazy evaluated.
    - the key word arguments that are intervales to be lazy evaluated.

Author
======

- Cyril Godart <cyril.godart@gmail.com>


