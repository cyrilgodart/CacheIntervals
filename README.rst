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


The ``MemoizationWithIntervals`` constructor
*******************************************
interval parameters
----------------------

To properly handle a generic function, the interval parameters that are candidate for memoization
need to be specified by the user.

So the first two parameters of a ``MemoizationWithIntervals`` constructor are the indices of positional
parameters and names for the key-word arguments that are intervals.

Below is an example with one positional interval parameter and one key-word interval parameter:
::
    @MemoizationWithIntervals(
      [0],
      ['interval1'],
      aggregation=list,
      memoization=klepto.lru_cache(
          maxsize=200,
          cache=klepto.archives.hdf_archive(
              f'{pdl.today().to_date_string()}_memoisation.hdf5'),
          keymap=klepto.keymaps.stringmap(typed=False, flat=False)),
          classrecorder=RecordIntervalsPandas
    )
    def function_with_interval_params(interval0,
                                    interval1=pd.Interval(tstwodaysago, tstomorrow)):
      time.sleep(1)
      print('***')
      print(f'interval0: {interval0}')
      print(f'interval1: {interval1}')
      return (interval0, interval1)

The other arguments in the constructor will be detailed in the following sub-sections.

The return type issue: specifying an aggregation method
-------------------------------------------------------
It needs not to be so though. To properly account to the general case, the user needs to have the flexibility
to specify an aggregation operation. This aggregation specification will be a parameter of the memoization
class constructor. The aggregation function will take the list of results from the different call and
return a result whose type is compatible with the initial function.

So this aggregation function could be as simple as ``aggregation=list`` or
equivalently, for Pandas data frames, ``aggregation=pandas.concatenate``. In the
case where the user wishes to aggregate the values by summation, something along
the line of ``aggregation=lambda listr: reduce(lambda x,y: x+y, listr)`` would be
apply.

The memoization algorithm
----------------------

The sole purpose of the package described in this article is to apply a preprocessing to a function taking interval
parameters so that the lazy evaluation can be delegated to an existing implementation of the user's choice. The constructor
of the =MemoizationWithIntervals= object thus takes a fully constructed memoization object, that will perform
the lazy evaluation.

As can be seen, we chose the =klepto= package as default implementation. We found in that package unique features that
were compelling to us and kept it that way. It is not the best documented package but there are a lot of examples provided
from which the usage and options can be inferred.

So typically to use the ``functools cache`` algorithm:
::
  from functools import cache
  @MemoizationWithIntervals(
      [0],
      ['interval1'],
      aggregation=list,
      memoization=cache
  )
  def function_with_interval_params(interval0,
                                    interval1=pd.Interval(tstwodaysago, tstomorrow)):
      time.sleep(1)
      print('**********************************')
      print(f'interval0: {interval0}')
      print(f'interval1: {interval1}')
      return (interval0, interval1)

Handling other interval types
----------------------

Alexandre Decan's *Portion* package is a great package for interval arithmetic.
For the interval object itself, though, it is probably not the most common
implementation. Arguably, Pandas' Interval can claim that title. But one may have
one's own implementation. Using ``CacheIntervals`` with a particular interval type
requires creating an ad-hoc type of interval recorder and a bit of wrapping to allow
a two way translation between the *Portion*'s native interval type and the user's interval type.

The package ``CacheIntervals`` provides an example of such a wrapping for the
Pandas Interval. The purpose for implementing that specific interval was two
fold. On the one hand, it is a template for user who want to implement that
override. And on the other hand, the Pandas' ``Interval`` type, along with Alexandre
Decan's native type should cover most of the needs. By default, the type of
interval recorder is the one that accommodates Pandas' Intervals. To change
it, specify the new interval type as argument of the constructor: e.g:
::
    @MemoizationWithIntervals(
            [0],
            ['interval1'],
            aggregation=list,
            classrecorder=RecordIntervals
    )
    def function_with_interval_params(interval0,
                                      interval1=portion.closed(tstwodaysago, tstomorrow)):
            time.sleep(1)
            print('**********************************')
            print(f'interval0: {interval0}')
            print(f'interval1: {interval1}')
            return (interval0, interval1)

All other ``kwargs`` passed to the constructor  will be stored and used as arguments for the
``RecordIntervals`` constructor. Here are the ones used by the library. Other can be defined
by the user.

Tolerance
----------

In order to prevent unnecessary transactions following rapid succession of requests, one may decide
that below a tolerance threshold no new call is issued. This approach is common in caching algorithms
and is often known as rounding.

In our case, all it requires is a small modification of the =RecordIntervals= class. The constructor
now accepts a rounding argument and the =disjunct= member function will test if the boundary of the
newly requested interval is below the threshold, the new interval is not added.
::
    import pendulum as pdl
    timenow = pdl.now()
    timenowplus5s = timenow.add(seconds=5)
    fiveseconds = timenowplus5s - timenow

    @MemoizationWithIntervals(
        [],
        ['period'],
        aggregation=list,
        rounding=fiveseconds#extra kwargs directly passed to RecordIntervals constructor
      )
    def function_with_interval_params(array=['USD/JPY'],
                                        period=pd.Interval(tsyesterday, pd.Timestamp.now(tz="UTC"))):
          time.sleep(1)
          print('************* function called *********************')
          print(f'interval0: {period}')
          return (period)

    print('==== First pass ===')
    print(f'Final result: {function_with_interval_params(array=["USD/JPY"], period=pd.Interval(tstoday, pd.Timestamp.now(tz="UTC")))}')
    print('==== Second pass ===')
    # This call happens below tolerance threshold and should not generate a real call
    time.sleep(1)
    print(f'Final result: {function_with_interval_params(["USD/JPY"], period=pd.Interval(tstoday, pd.Timestamp.now(tz="UTC")))}')
    # This call happens behond the tolerance threshold and will generate a real call
    time.sleep(6)
    print('==== 3rd pass ===')
    print(f'Final result: {function_with_interval_params(["USD/JPY"], period=pd.Interval(tstoday, pd.Timestamp.now(tz="UTC")))}')

Changing interval strategy for proper aggregation
---------------------------------------------------

The default interval strategy returns a superset of the requested interval if such is already stored.
This is incompatible with an aggregation strategy that takes the cumulative sum or the average of the data
returned over the interval.

Access to cached function
--------------------------

Passing the key-word argument =get_function_cachedQ=True= will result in all other arguments
being ignored and the cached function being returned. Depending on the underlying memoization implementation,
some introspection might be available.
::
        @MemoizationWithIntervals(
            [0], ['interval1'],
            aggregation=list,
            debug=True,
            memoization=klepto.lru_cache(
                maxsize=200,
                cache=klepto.archives.dict_archive(),
                keymap=klepto.keymaps.stringmap(typed=False, flat=False)))
        def function_with_interval_params(interval0,
                                          interval1=pd.Interval(
                                              tstwodaysago,
                                              tstomorrow)):
            time.sleep(1)
            print('**********************************')
            print(f'interval0: {interval0}')
            print(f'interval1: {interval1}')
            return (interval0, interval1)

        print('==== First pass ===')
        # function_with_interval_params(pd.Interval(pdl.yesterday(), pdl.today(),closed='left'),
        #                               interval1 = pd.Interval(pdl.yesterday().add(days=0),
        #                                                           pdl.today(), closed='both')
        #                               )
        f_mzed = function_with_interval_params(get_function_cachedQ=True)
        print(
            f'Final result:\n{function_with_interval_params(pd.Interval(tsyesterday, tstoday))}'
        )
        print(f'==============\nf_memoized live cache: {f_mzed.__cache__()}')
        print(f'f_memoized live cache type: {type(f_mzed.__cache__())}')
        print(f'f_memoized file cache: {f_mzed.__cache__().archive}')
        print(f'f_memoized live cache: {f_mzed.info()}')
        f_mzed.__cache__().dump()


Testing
=======

In order to run the tests, you need to first generate a SQL Lite database. To do so, run the ``GeneratorTests.py``
script from the ``Ancillaries`` directory.


Author
======

- Cyril Godart <cyril.godart@gmail.com>


