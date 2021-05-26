from tempfile import mkdtemp
import pendulum as pdl
import sys

sys.path.append(".")
# the memoization-related library
import loguru

import portion
import CacheIntervals as ci
from CacheIntervals.Intervals import pd2po, po2pd
from CacheIntervals.SetsAndIterators import flatten
from CacheIntervals.Dates import pdl2pd, pd2pdl
from CacheIntervals.Timer import Timer

import itertools
import klepto
import klepto.safe
import klepto.archives
import klepto.keymaps

class QueryRecorder:
    pass

class RecordIntervals:
    '''
   The memoisation of time series involve:
    1. a persistence of all intervals calculated
    2. a calculation of the overlap of any new interval passed as parameter with the
       previously calculated intervals.
    In the below, =i= is the interval passed as parameter, while =s= are the stored intervals.
    i is always atomic
    =s= is actually a IntervalDict: interval -> time of call
    ** i disjunct from  any of the atomic interval in s
        - store i with value now
        - call the function with parameter i (no memoisation)
    ** i overlaps an atomic interval in s
        - store the call time with the key which is  the difference of i with that interval
        - the intersection is treated with [[id:5e181d0d-65c1-42fd-9122-e5dbd19275bb][first case]]
        - the difference with [[id:34d9e82e-64d1-4e1c-9fde-5d8afbce5360][the second case]]
    '''
    def __init__(self,
                 rounding=None,
                 subintervals_requiredQ=False,
                 subinterval_minQ=False):
        '''
        :param time_between_calls allows not updating the
            calls unless a minimum time has passed
        :param subintervals_requiredQ: if an existing interval overlaps
        returns:
           - the whole interval (subinterval_requiredQ=False)
           - replace the existing interval by the intersection and complement
        The storage of intervals is done through a portion.IntervalDict
        see https://github.com/AlexandreDecan/portion
        '''
        # IntervalDict prevent merge of adjacent or overalapping intervales
        # an undesirable feature give the fact that a interval correpsond to an actual call of the function.
        self.intervals = portion.IntervalDict()
        # calls is the latest
        self.calls = []
        self.tol = rounding
        self.subintervalsQ=subintervals_requiredQ
        self.subintervals_minQ=subinterval_minQ


    def disjunct(self, i):
        '''
        if i is disjunct from all previously
        stored intervals:
        - store this new interval
        - instruct to call the function
          with this interval as parameter
        :param i: an interval that
               has no overlap wih
               any previously stored interval
        '''
        if self.tol is not None:
            if i.upper - i.lower <= self.tol: return

        self.intervals[i] = pdl.now()
        self.calls.append(i)

    def contained(self, s):
        '''
        if i is contained in one of the
        stored intervals:
        - instruct to call the function
          with the stored containing interval as parameter
          Note: memoisation happens
        :param s: an interval that
                  contains the interval
                  passed originally as
                  argument to the function
        '''
        self.calls.append(list(s))
        pass

    def __call__(self, i):
        '''
        the main function
        :param i: the original interval passed
                as parameter to the function
        :return: the calls to be made
        '''
        # reinitialise calls.
        self.calls = []
        try:
            itvls_overlap = self.intervals[i]
        except Exception as e:
            logging.getLogger(__name__).error(f'{e}', exc_info=True)
        if len(itvls_overlap.keys()) == 0:
            self.disjunct(i)
        else:
            if not self.subintervalsQ:
                ''' 
                if the interval requested is contained in an existing
                intervale return the larger interval. 
                This makes often sense as the filtering of the extra-data
                can be faster than querying again
                '''
                for s in self.intervals.keys():
                    if not (i & s).empty: self.contained(s)
                intervals = portion.empty()
                for s in itvls_overlap.keys():
                    intervals = intervals | s  #itertools.accumulate(itvls_overlap, lambda i,o: i | o   )
                disjuncts = i - intervals
                for s in disjuncts:
                    if not s.empty: self.disjunct(s)
            else:
                '''
                if a subset of an existing interval is requested then break it down
                Several strategies are possible here
                
                '''
                if not self.subintervals_minQ:
                    '''
                    the new intervals called will not be split. 
                    the old one will
                    '''
                    intervals_contained = portion.empty()
                    for s in self.intervals:
                        if s in i:
                            intervals_contained |= s
                            self.contained(s)
                    disjuncts = i - intervals_contained
                    for s in disjuncts:
                        if not s.empty: self.disjunct(s)
                else:
                    '''
                    both overlapping intervals will be split
                    '''
                    #self.disjunct(i)

                    inter = portion.empty()
                    diffs = []
                    for s in self.intervals:
                        diffs.append(i - s)
                    diff = list(itertools.accumulate(diffs, lambda x,y: x&y))
                    if len(diff):
                        diff = diff[-1]
                        for s in diff:
                            self.disjunct(s)
                    inter = []
                    for s in self.intervals:
                        inter = s&i
                        for ii in inter:
                            if ii not in self.intervals:
                                self.disjunct(ii)
                            else:
                                if ii not in self.calls:
                                    self.contained(ii)

        calls = sorted(map(list, flatten(self.calls)))
        return calls

class RecordIntervalsPandas(RecordIntervals):
    '''
    Adapter allows to pass pandas interval to the
    RecordIntervals class.
    '''
    def __init__(self,
                 rounding=None,
                 subintervals_requiredQ=False,
                 subinterval_minQ=False):
        '''
        :param time_between_calls allows not updating the
            calls unless a minimum time has passed
        The storage of intervals is done through a portion.IntervalDict
        see https://github.com/AlexandreDecan/portion
        '''
        super().__init__(rounding, subintervals_requiredQ, subinterval_minQ)

    def __call__(self, i):
        calls = super().__call__(pd2po(i))
        calls = list(map(po2pd, flatten(calls)))
        return calls


class MemoizationWithIntervals(object):
    '''
    The purpose of this class is to optimise
    the number of call to a function retrieving
    possibly disjoint intervals:
    - do standard caching for a given function
    - additively call for a date posterior to one
      already cached is supposed to yield a pandas
      Frame which can be obtained by concatenating
      the cached result and a -- hopefully much --
      smaller query
    Maintains a list of intervals that have been
    called.
    With a new interval:
    -
    '''
    keymapper = klepto.keymaps.stringmap(typed=False, flat=False)

    def __init__(self,
                 pos_args=None,
                 names_kwarg=None,
                 classrecorder=RecordIntervalsPandas,
                 aggregation=lambda listdfs: pd.concat(listdfs, axis=0),
                 debug=False,
                 # memoization=klepto.lru_cache(
                 #     cache=klepto.archives.hdf_archive(
                 #         f'{pdl.today().to_date_string()}_memoization.hdf5'),
                 #     keymap=keymapper),
                 memoization=klepto.lru_cache(
                     cache=klepto.archives.dict_archive(),
                     keymap=keymapper),
                 **kwargs):
        '''

            :param pos_args: the indices of the positional
                   arguments that will be handled as intervals
            :param names_kwarg: the name of the named parameters
                     that will be handled as intervals
            :param classrecorder: the interval recorder type
                                we want to use
            :param memoization: a memoization algorithm
            '''
        # A dictionary of positional arguments indices
        # that are intervals

        self.argsi = {}
        self.kwargsi = {}
        # if pos_args is not None:
        #     for posarg in pos_args:
        #         self.argsi[posarg] = classrecorder(**kwargs)
        self.pos_args_itvl = pos_args if pos_args is not None else []
        #print(self.args)
        # if names_kwarg is not None:
        #     for namedarg in names_kwarg:
        #         self.kwargsi[namedarg] = classrecorder(**kwargs)
        self.names_kwargs_itvl = names_kwarg if names_kwarg is not None else {}
        #print(self.kwargs)
        self.memoization = memoization
        self.aggregation = aggregation
        self.debugQ = debug
        self.argsdflt = None
        self.kwargsdflt = None
        self.time_last_call = pdl.today()
        self.classrecorder = classrecorder
        self.kwargsrecorder = kwargs
        self.argssolver = None
        self.query_recorder = QueryRecorder()

    def __call__(self, f):
        '''
        The interval memoization leads to several calls to the
        standard memoised function and generates a list of return values.
        The aggregation is needed for the doubly lazy
        function to have the same signature as the

        To access, the underlying memoized function pass
        get_function_cachedQ=True to the kwargs of the
        overloaded call (not of this function
        :param f: the function to memoize
        :return: the wrapper to the memoized function
        '''
        if self.argssolver is None:
            self.argssolver = ci.Functions.ArgsSolver(f, split_args_kwargsQ=True)

        @self.memoization
        def f_cached(*args, **kwargs):
            '''
            The cached function is used for a double purpose:
            1. for standard calls, will act as the memoised function in a traditional way
            2. Additively when pass parameters of type QueryRecorder, it will create
               or retrieve the interval recorders associated with the values of
               non-interval parameters.
               In this context, we use the cached function as we would a dictionary.
            '''
            QueryRecorderQ = False
            args_new = []
            kwargs_new = {}
            '''
            check whether this is a standard call to the user function
            or a request for the interval recorders
            '''
            for i,arg in enumerate(args):
                if isinstance(arg, QueryRecorder):
                   args_new.append(self.classrecorder(**self.kwargsrecorder))
                   QueryRecorderQ = True
                else:
                    args_new.append(args[i])
            for name in kwargs:
                if isinstance(kwargs[name], QueryRecorder):
                    kwargs_new[name] = self.classrecorder(**self.kwargsrecorder)
                    QueryRecorderQ = True
                else:
                    kwargs_new[name] = kwargs[name]
            if QueryRecorderQ:
                return args_new, kwargs_new
            return f(*args, **kwargs)

        def wrapper(*args, **kwargs):
            if kwargs.get('get_function_cachedQ', False):
                return f_cached
            #loguru.logger.debug(f'function passed: {f_cached}')
            loguru.logger.debug(f'args passed: {args}')
            loguru.logger.debug(f'kwargs passed: {kwargs}')
            # First pass: resolve the recorders
            dargs_exp, kwargs_exp = self.argssolver(*args, **kwargs)
            # Intervals are identified by position and keyword name
            # 1. First get the interval recorders
            args_exp = list(dargs_exp.values())
            args_exp_copy = args_exp.copy()
            kwargs_exp_copy = kwargs_exp.copy()
            for i in self.pos_args_itvl:
                args_exp_copy[i] = self.query_recorder
            for name in self.names_kwargs_itvl:
                    kwargs_exp_copy[name] = self.query_recorder
            args_with_ri, kwargs_with_ri = f_cached(*args_exp_copy, **kwargs_exp_copy)
            # 2. Now get the the actual list of intervals
            for i in self.pos_args_itvl:
                # reuse args_exp_copy to store the list
                args_exp_copy[i] = args_with_ri[i](args_exp[i])
            for name in self.names_kwargs_itvl:
                    # reuse kwargs_exp_copy to store the list
                    kwargs_exp_copy[name] = kwargs_with_ri[name](kwargs_exp[name])
            '''3. Then generate all combination of parameters
            3.a - args'''
            ns_args = range(len(args_exp))
            lists_possible_args = [[args_exp[i]]  if i not in self.pos_args_itvl else args_exp_copy[i] for i in ns_args]
            # Take the cartesian product of these
            calls_args = list( map(list,itertools.product(*lists_possible_args)))
            '''3.b kwargs'''
            #kwargs_exp_vals = kwargs_exp_copy.values()
            names_kwargs = list(kwargs_exp_copy.keys())
            lists_possible_kwargs = [[kwargs_exp[name]]  if name not in self.names_kwargs_itvl
                                   else kwargs_exp_copy[name] for name in names_kwargs]
            calls_kwargs = list(map(lambda l: dict(zip(names_kwargs,l)), itertools.product(*lists_possible_kwargs)))
            calls = list(itertools.product(calls_args, calls_kwargs))
            if self.debugQ:
                results = []
                for call in calls:
                    with Timer() as timer:
                        results.append(f_cached(*call[0], **call[1]) )
                    print('Timer to demonstrate caching:')
                    timer.display(printQ=True)
            else:
                results = [f_cached(*call[0], **call[1]) for call in calls]
            result = self.aggregation(results)
            return result

        return wrapper


if __name__ == "__main__":
    import logging
    import daiquiri
    import pandas as pd
    import portion as po
    import datetime
    import time
    daiquiri.setup(logging.DEBUG)
    logging.getLogger('OneTick64').setLevel(logging.WARNING)
    logging.getLogger('databnpp.ODCB').setLevel(logging.WARNING)
    logging.getLogger('requests_kerberos').setLevel(logging.WARNING)
    pd.set_option('display.max_rows', 200)
    pd.set_option('display.width', 600)
    pd.set_option('display.max_columns', 200)


    tssixdaysago = pdl2pd(pdl.yesterday('UTC').add(days=-5))
    tsfivedaysago = pdl2pd(pdl.yesterday('UTC').add(days=-4))
    tsfourdaysago = pdl2pd(pdl.yesterday('UTC').add(days=-3))
    tsthreedaysago = pdl2pd(pdl.yesterday('UTC').add(days=-2))
    tstwodaysago = pdl2pd(pdl.yesterday('UTC').add(days=-1))
    tsyesterday = pdl2pd(pdl.yesterday('UTC'))
    tstoday = pdl2pd(pdl.today('UTC'))
    tstomorrow = pdl2pd(pdl.tomorrow('UTC'))
    tsintwodays = pdl2pd(pdl.tomorrow('UTC').add(days=1))
    tsinthreedays = pdl2pd(pdl.tomorrow('UTC').add(days=2))



    def print_calls(calls):
        print( list( map( lambda i: (i.left, i.right), calls)))
    def print_calls_dates(calls):
        print( list( map( lambda i:
                                       (pd2pdl(i.left).to_date_string(), pd2pdl(i.right).to_date_string()),
                                       calls)))
    def display_calls(calls):
        loguru.logger.info( list( map( lambda i:
                    (pd2pdl(i.left).to_date_string(), pd2pdl(i.right).to_date_string()),
                    calls)))

    #                               Testing record intervals -> ok
    if False:
        itvals = RecordIntervals()
        calls = itvals(portion.closed(pdl.yesterday(), pdl.today()))
        print(list(map( lambda i: (i.lower.to_date_string(), i.upper.to_date_string()), calls)))
        print(list(map(lambda i: type(i), calls)))
        calls = itvals( portion.closed(pdl.yesterday().add(days=-1), pdl.today().add(days=1)))
        #print(calls)
        print( list( map( lambda i: (i.lower.to_date_string(), i.upper.to_date_string()),
                    calls)))
    #                            Testing record intervals pandas -> ok
    if False:
        itvals = RecordIntervalsPandas()
        # yesterday -> today
        calls = itvals(pd.Interval(pdl2pd(pdl.yesterday()), pdl2pd(pdl.today()), closed='left'))
        print( list( map( lambda i: (pd2pdl(i.left).to_date_string(), pd2pdl(i.right).to_date_string()), calls)))
        # day before yesterday -> tomorrow: should yield 3 intervals
        calls = itvals(pd.Interval(pdl2pd(pdl.yesterday().add(days=-1)), pdl2pd(pdl.today().add(days=1))))
        print( list( map( lambda i: (pd2pdl(i.left).to_date_string(), pd2pdl(i.right).to_date_string()), calls)))
        # day before yesterday -> day after tomorrow: should yield 4 intervals
        calls = itvals(
            pd.Interval(pdl2pd(pdl.yesterday().add(days=-1)),
                        pdl2pd(pdl.tomorrow().add(days=1))))
        print(
            list(
                map(
                    lambda i:
                    (pd2pdl(i.left).to_date_string(), pd2pdl(i.right).to_date_string()),
                    calls)))
        # 2 days before yesterday -> 2day after tomorrow: should yield 6 intervals
        calls = itvals(
            pd.Interval(pdl2pd(pdl.yesterday().add(days=-2)),
                        pdl2pd(pdl.tomorrow().add(days=2))))
        print(list(map( lambda i:
                    (pd2pdl(i.left).to_date_string(), pd2pdl(i.right).to_date_string()),
                    calls)))
    # Further tests on record intervals pandas
    if False:
        itvals = RecordIntervalsPandas()
        calls = itvals(pd.Interval(tstwodaysago, tstomorrow, closed='left'))
        display_calls(calls)
        calls = itvals( pd.Interval(tstwodaysago, tsyesterday))
        display_calls(calls)
        calls = itvals(
            pd.Interval(tstwodaysago, tsintwodays))
        display_calls(calls)
        calls = itvals(
            pd.Interval(pdl2pd(pdl.yesterday().add(days=-2)),
                        pdl2pd(pdl.tomorrow().add(days=2))))
        display_calls(calls)
    #      proof-of_concept of decorator to modify function parameters
    if False:
        class dector_arg:
            # a toy model
            def __init__(self,
                         pos_arg=None,
                         f_arg=None,
                         name_kwarg=None,
                         f_kwarg=None):
                '''

                :param pos_arg:  the positional argument
                :param f_arg: the function to apply to the positional argument
                :param name_kwarg: the  keyword argument
                :param f_kwarg: the function to apply to the keyword argument
                '''
                self.args = {}
                self.kwargs = {}
                if pos_arg:
                    self.args[pos_arg] = f_arg
                print(self.args)
                if name_kwarg:
                    self.kwargs[name_kwarg] = f_kwarg
                print(self.kwargs)

            def __call__(self, f):
                '''
                the decorator action
                :param f: the function to decorate
                :return: a function whose arguments
                         have the function f_args and f_kwargs
                         pre-applied.
                '''
                self.f = f

                def inner_func(*args, **kwargs):
                    print(f'function passed: {self.f}')
                    print(f'args passed: {args}')
                    print(f'kwargs passed: {kwargs}')
                    largs = list(args)
                    for i, f in self.args.items():
                        print(i)
                        print(args[i])
                        largs[i] = f(args[i])
                    for name, f in self.kwargs.items():
                        kwargs[name] = f(kwargs[name])
                    return self.f(*largs, **kwargs)

                return inner_func

        dec = dector_arg(pos_arg=0,
                         f_arg=lambda x: x + 1,
                         name_kwarg='z',
                         f_kwarg=lambda x: x + 1)

        @dector_arg(1, lambda x: x + 1, 'z', lambda x: x + 1)
        def g(x, y, z=3):
            '''
            The decorated function should add one to the second
            positional argument and
            :param x:
            :param y:
            :param z:
            :return:
            '''
            print(f'x->{x}')
            print(f'y->{y}')
            print(f'z->{z}')

        g(1, 10, z=100)
    if False:
        memo = MemoizationWithIntervals()
    # testing MemoizationWithIntervals

    #          typical mechanism
    if False:
        @MemoizationWithIntervals(
            None, ['interval'],
            aggregation=list,
            debug=True,
            memoization=klepto.lru_cache(
                maxsize=200,
                cache=klepto.archives.hdf_archive(
                    f'{pdl.today().to_date_string()}_memoisation.hdf5'),
                keymap=klepto.keymaps.stringmap(typed=False, flat=False)))
        def function_with_interval_param(dummy1,dummy2, kdummy=1,
                                          interval=pd.Interval(tstwodaysago, tstomorrow)):
            time.sleep(1)
            print('****')
            print(f'dummy1: {dummy1}, dummy2: {dummy2}')
            print(f'kdummy: {kdummy}')
            print(f'interval: {interval}')
            return [dummy1, dummy2, kdummy, interval]
        print('=*=*=*=*   MECHANISM DEMONSTRATION =*=*=*=*')
        print('==== First pass ===')
        print("initialisation with an interval from yesterday to today")
        # function_with_interval_params(pd.Interval(pdl.yesterday(), pdl.today(),closed='left'),
        #                               interval1 = pd.Interval(pdl.yesterday().add(days=0),
        #                                                           pdl.today(), closed='both')
        #                               )
        print( f'Final result:\n{function_with_interval_param(0, 1, interval=pd.Interval(tsyesterday, tstoday))}')
        print('==== Second pass ===')
        print("request for data from the day before yesterday to today")
        print("expected split in two intervals with results from yesterday to today being cached")
        print(
            f'Final result: {function_with_interval_param(0,1, interval=pd.Interval(tstwodaysago, tstoday))}'
        )
        print('==== 3rd pass ===')
        print("request for data from three days to yesterday")
        print("expected split in two intervals")
        print(f'Final result:\n {function_with_interval_param(0,1,  interval=pd.Interval(tsthreedaysago, tsyesterday))}' )
        print('==== 4th pass ===')
        print("request for data from three days to tomorrow")
        print("expected split in three intervals")
        print(f'Final result:\n\
           {function_with_interval_param(0,1, interval1=  pd.Interval(tsthreedaysago, tstomorrow))}' )
        print('==== 5th pass ===')
        print("request for data from  two days ago to today with different first argument")
        print("No caching expected and one interval")
        print( f'Final result:\n{function_with_interval_param(1, 1, interval=pd.Interval(tstwodaysago, tstoday))}' )
        print('==== 6th pass ===')
        print("request for data from three days ago to today with different first argument")
        print("Two intervals expected")
        print( f'Final result: {function_with_interval_param(1, 1, interval=pd.Interval(tsthreedaysago, tstoday))}' )
    #           Testing with an interval as position argument and one interval as keyword argument
    if False:
        @MemoizationWithIntervals(
            [0], ['interval1'],
            aggregation=list,
            debug=True,
            memoization=klepto.lru_cache(
                maxsize=200,
                cache=klepto.archives.hdf_archive(
                    f'{pdl.today().to_date_string()}_memoisation.hdf5'),
                keymap=klepto.keymaps.stringmap(typed=False, flat=False)))
        def function_with_interval_params(interval0,
                                          interval1=pd.Interval(tstwodaysago, tstomorrow)):
            time.sleep(1)
            print('***')
            print(f'interval0: {interval0}')
            print(f'interval1: {interval1}')
            return (interval0, interval1)
        print('=*=*=*=*     DEMONSTRATION WITH TWO INTERVAL PARAMETERS      =*=*=*=*')
        print('==== First pass ===')
        print(f'Initialisation: first interval:\nyest to tday - second interval: two days ago to tomorrow')
        print(f'Final result:\n{function_with_interval_params(pd.Interval(tsyesterday, tstoday))}')
        print('==== Second pass ===')
        print(f'Call with first interval:\n3 days ago to tday - second interval: unchanged')
        print('Expected caching and split of first interval in two')
        print( f'Final result: {function_with_interval_params(pd.Interval(tsthreedaysago, tstoday))}' )
        print('==== 3rd pass ===')
        print(f'Call with first interval:\nunchanged - second interval: yest to today')
        print('Expected only cached results and previous  split of first interval')
        print(f'Final result:\n {function_with_interval_params(pd.Interval(tsthreedaysago, tstoday), interval1 = pd.Interval(tsyesterday, tstoday))}' )
        print('==== 4th pass ===')
        print(f'Call with first interval:\n3 days ago to today - second interval: yest to today')
        print('Expected only cached results and only split of first interval')
        print(f'Final result:\n {function_with_interval_params(pd.Interval(tsthreedaysago, tstoday), interval1 = pd.Interval(tsyesterday, tstoday))}' )
        print('==== 5th pass ===')
        print(f'Call with first interval:\n3 days ago to yesterday - second interval: 3 days ago to tomorrow')
        print('Expected no split of first interval and split of second interval in two. Only one none-cached call')
        print(f'Final result:\n\
         {function_with_interval_params(pd.Interval(tsthreedaysago, tsyesterday), interval1=  pd.Interval(tsthreedaysago, tstomorrow))}'
              )
        print('==== 6th pass ===')
        print(f'Call with first interval:\n3 days ago to today - second interval: 3 days ago to tomorrow')
        print('Expected split of first interval in two and split of second interval in two. One non-cached call: today - tomorrow x ')
        print(f'Final result:\n\
         {function_with_interval_params(pd.Interval(tsthreedaysago, tstoday), interval1=pd.Interval(tsthreedaysago, tstomorrow))}'
          )
    #                           Showing the issue with the current version
    if False:
        @MemoizationWithIntervals(None,
            ['interval'],
            aggregation=list,
            debug=True,
            memoization=klepto.lru_cache(
                maxsize=200,
                keymap=klepto.keymaps.stringmap(typed=False, flat=False)))
        def function_with_interval_param(valint,
                                          interval=pd.Interval(tstwodaysago, tstomorrow)):
            time.sleep(1)
            print('**********************************')
            print(f'valint: {valint}')
            print(f'interval: {interval}')
            return (valint, interval)
        print('==== First pass ===')
        print( f'Final result:\n{function_with_interval_param(2, interval=pd.Interval(tsyesterday, tstoday))}')
        print('==== Second pass ===')
        print(f'Final result: {function_with_interval_param(2, interval=pd.Interval(tsthreedaysago, tstoday))}')
        print('==== 3rd pass ===')
        print( f'Final result:\n {function_with_interval_param(3, interval=pd.Interval(tsthreedaysago, tstoday))}')
        print('==== 4th pass ===')
        print(f'Final result:\n\ {function_with_interval_param(3, interval=pd.Interval(tsthreedaysago, tstomorrow))}')
    #                 testing getting back the memoized function from MemoizationWithIntervals
    if False:
        @MemoizationWithIntervals(
            [0], ['interval1'],
            aggregation=list,
            debug=True,
            memoization=klepto.lru_cache(
                maxsize=200,
                cache=klepto.archives.file_archive(
                    f'{pdl.today().to_date_string()}_memoisation.pkl'),
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
        print(f'f_memoized file cache: {f_mzed.__cache__().archive}')
        # print('==== Second pass ===')
        # print(f'Final result: {function_with_interval_params(pd.Interval(pdl.yesterday().add(days=-2), pdl.today()))}')
        # print('==== 3rd pass ===')
        # print(f'Final result:\n\
        #     {function_with_interval_params(pd.Interval(pdl.yesterday().add(days=-2), pdl.yesterday()), interval1 = pd.Interval(pdl.yesterday().add(days=0), pdl.today()))}')
        # print('==== 4th pass ===')
        # print(f'Final result:\n\
        #  {function_with_interval_params(pd.Interval(pdl.yesterday().add(days=-2), pdl.yesterday()), interval1=  pd.Interval(pdl.yesterday().add(days=-2), pdl.tomorrow()))}')
    # testing serialization with HDF5 memoized function from MemoizationWithIntervals
    if False:
        @MemoizationWithIntervals(
            [0], ['interval1'],
            aggregation=list,
            debug=True,
            memoization=klepto.lru_cache(
                maxsize=200,
                cache=klepto.archives.hdf_archive(
                    f'{pdl.today().to_date_string()}_memoisation.hdf5',
                    serialized=True,
                    cached=False,
                    meta=False),
                keymap=klepto.keymaps.stringmap(typed=False, flat=False)))
        def function_with_interval_params(interval0,
                                          interval1=pd.Interval(
                                              tstwodaysago,
                                              tstomorrow)):
            time.sleep(1)
            print('*********** function called *******************')
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
        print(f'f_memoized file cache: {f_mzed.__cache__().archive}')
    if False:
        @MemoizationWithIntervals([0], aggregation=list, debug=False)
        def function_with_interval_params(interval0):
            time.sleep(1)
            print('**********************************')
            print(f'interval0: {interval0}')
            return (interval0)

        print('==== First pass ===')
        print(
            f'Final result: {function_with_interval_params(pd.Interval(tsyesterday, tstoday))}'
        )
        print('==== Second pass ===')
        print(
            f'Final result: {function_with_interval_params(pd.Interval(tsthreedaysago, tstoday) )}'
        )
        print('==== 3rd pass ===')
        print(
            f'Final result: {function_with_interval_params(pd.Interval(tsthreedaysago, tsyesterday))}'
        )
        print('==== 4th pass ===')
        print(
            f'Final result: {function_with_interval_params(pd.Interval(tsthreedaysago, tstomorrow))}'
        )
    #                  Testing kwargs only
    if False:

        @MemoizationWithIntervals([], ['period'],
                                  aggregation=list,
                                  debug=False)
        def function_with_interval_params(array=['USD/JPY'],
                                          period=pd.Interval( tsyesterday, pd.Timestamp.now('UTC'))):
            time.sleep(1)
            print('************* function called *********************')
            print(f'interval0: {period}')
            return (array, period)

        print('==== First pass ===')
        print(
            f'Final result: {function_with_interval_params(array=["USD/JPY"], period = pd.Interval(tsyesterday, pd.Timestamp.now(tz="UTC")))}'
        )
        print('==== Second pass ===')
        print(
            f'Final result: {function_with_interval_params(array=["USD/JPY"],period = pd.Interval(tsyesterday, pd.Timestamp.now(tz="UTC")) )}'
        )
        print('==== 3rd pass ===')
        print(
            f'Final result: {function_with_interval_params(array=["USD/JPY"],period = pd.Interval(tsyesterday, pd.Timestamp.now(tz="UTC")))}'
        )
    #                  Testing tolerance
    if False:
        timenow = pdl.now()
        timenowplus5s = timenow.add(seconds=5)
        fiveseconds = timenowplus5s - timenow

        @MemoizationWithIntervals([], ['period'],
                                  aggregation=list,
                                  debug=False,
                                  rounding=fiveseconds)
        def function_with_interval_params(array=['USD/JPY'],
                                          period=pd.Interval(tsyesterday, pd.Timestamp.now(tz="UTC"))
                                              ):
            time.sleep(1)
            print('************* function called *********************')
            print(f'interval0: {period}')
            return (period)

        print('==== First pass ===')
        print(
            f'Final result: {function_with_interval_params(array=["USD/JPY"], period=pd.Interval(tstoday, pd.Timestamp.now(tz="UTC")))}'
        )
        print('==== Second pass ===')
        time.sleep(1)
        print(
            f'Final result: {function_with_interval_params(["USD/JPY"], period=pd.Interval(tstoday, pd.Timestamp.now(tz="UTC")))}'
        )
        time.sleep(6)
        print('==== 3rd pass ===')
        print(
            f'Final result: {function_with_interval_params(["USD/JPY"], period=pd.Interval(tstoday, pd.Timestamp.now(tz="UTC")))}'
        )
        print('==== 4th pass ===')
        print(
            f'Final result: {function_with_interval_params(["USD/JPY"], period = pd.Interval(tstoday, pd.Timestamp.now(tz="UTC")))}'
        )
    if False:
        itvals = RecordIntervalsPandas()
        calls = itvals(pd.Interval(pdl.yesterday(), pdl.today()))
        print(
            list(
                map(
                    lambda i:
                    (i.left.to_date_string(), i.right.to_date_string()),
                    calls)))
        calls = itvals(pd.Interval(pdl.yesterday().add(days=-2), pdl.today()))
        print(
            list(
                map(
                    lambda i:
                    (i.left.to_date_string(), i.right.to_date_string()),
                    calls)))
        calls = itvals(
            pd.Interval(pdl.yesterday().add(days=-2), pdl.yesterday()))
        print(
            list(
                map(
                    lambda i:
                    (i.left.to_date_string(), i.right.to_date_string()),
                    calls)))
        calls = itvals(
            pd.Interval(pdl.yesterday().add(days=-4), pdl.tomorrow()))
        print(
            list(
                map(
                    lambda i:
                    (i.left.to_date_string(), i.right.to_date_string()),
                    calls)))
    if False:

        def solution(data, n):
            counts = {}
            counts = {x: counts.get(x, data.count(x)) for x in data}
            return [x for x in data if counts[x] <= n]

        print(solution([1, 2, 3], 0))
        print(solution([1, 2, 2, 3, 3, 4, 5, 5], 1))
    if False:
        cont = {0: [0, 1], 2: [3, 4]}
        argsorg = [5, 6, 7]
        calls_args = [[
            arg if i not in cont.keys() else cont[i][j]
            for i, arg in enumerate(argsorg)
        ] for p in cont.keys() for j in range(len(cont[p]))]

    if True:
        print("Testing subintervals and strategies")
        print("1. No sub")
        itvals_nosub = RecordIntervalsPandas(subintervals_requiredQ=False, subinterval_minQ=False)
        print("-6->-1")
        calls = itvals_nosub(pd.Interval(-6, -1))
        print("-3->0")
        calls = itvals_nosub(pd.Interval(-3, 0 ))
        print_calls(calls)
        print("2. No sub first strategy")
        itvals_sub = RecordIntervalsPandas(subintervals_requiredQ=True, subinterval_minQ=False)
        print("-6->-1")
        calls = itvals_sub(pd.Interval(-6,-1))
        print("-3->0")
        calls = itvals_sub(pd.Interval(-3,0))
        print_calls(calls)
        print("3. Sub second strategy")
        itvals_sub2 = RecordIntervalsPandas(subintervals_requiredQ=True, subinterval_minQ=True)
        print("-6->-1")
        calls = itvals_sub2(pd.Interval(-6,-1))
        print("-3->0")
        calls = itvals_sub2(pd.Interval(-3,0))
        print_calls(calls)
    # Test ok
    if False:
        print("Testing subintervals and strategies")
        print("1. No sub")
        itvals_nosub = RecordIntervalsPandas(subintervals_requiredQ=False, subinterval_minQ=False)
        print("-6->-1")
        calls = itvals_nosub(pd.Interval(tssixdaysago, tsyesterday))
        print("-3->0")
        calls = itvals_nosub(pd.Interval(tsthreedaysago, tstoday ))
        print_calls(calls)
        print("2. No sub first strategy")
        itvals_sub = RecordIntervalsPandas(subintervals_requiredQ=True, subinterval_minQ=False)
        print("-6->-1")
        calls = itvals_sub(pd.Interval(tssixdaysago, tsyesterday))
        print("-3->0")
        calls = itvals_sub(pd.Interval(tsthreedaysago, tstoday ))
        print_calls(calls)
        print("3. Sub second strategy")
        itvals_sub2 = RecordIntervalsPandas(subintervals_requiredQ=True, subinterval_minQ=True)
        print("-6->-1")
        calls = itvals_sub2(pd.Interval(tssixdaysago, tsyesterday))
        print("-3->0")
        calls = itvals_sub2(pd.Interval(tsthreedaysago, tstoday))
        print_calls(calls)
    if False:
        print("Testing subinterval and first strategy")
        itvals = RecordIntervalsPandas(subintervals_requiredQ=True, subinterval_minQ=False)
        calls = itvals(pd.Interval(tsfourdaysago, tsthreedaysago))
        print(list(map(lambda i: (i.left, i.right), calls)))
        calls = itvals(pd.Interval(tstwodaysago, tstoday))
        print(list(map(lambda i: (i.left, i.right), calls)))
        calls = itvals(pd.Interval(tssixdaysago, tsyesterday))
        print(list(map(lambda i: (i.left, i.right), calls)))
        calls = itvals(pd.Interval(tssixdaysago, tsyesterday))
        print("should be broken in 3 intervals: -5->-4 | -4->-3 | -3->-1")
        print(sorted(list(map(lambda i: (i.left, i.right), calls))))
    if False:
        print("Testing subinterval and second strategy")
        itvals = RecordIntervalsPandas(subintervals_requiredQ=True, subinterval_minQ=True)
        calls = itvals(pd.Interval(tsfourdaysago, tsthreedaysago))
        print(list(map(lambda i: (i.left, i.right), calls)))
        calls = itvals(pd.Interval(tstwodaysago, tstoday))
        print(list(map(lambda i: (i.left, i.right), calls)))
        calls = itvals(pd.Interval(tssixdaysago, tsyesterday))
        print(sorted(list(map(lambda i: (i.left, i.right), calls))))
        calls = itvals(pd.Interval(tssixdaysago, tsyesterday))
        print("should be broken in 3 intervals: -5->-4 | -4->-3 | -3->-1")
        print(sorted(list(map(lambda i: (i.left, i.right), calls))))

    if False:
        print("Testing subinterval and first strategy")
        itvals = RecordIntervalsPandas(subintervals_requiredQ=True, subinterval_minQ=False)
        calls = itvals(pd.Interval(-2, 0))
        print_calls(calls)
        calls = itvals(pd.Interval(-4, -3))
        print_calls(calls)
        calls = itvals(pd.Interval(-6, 1))
        print("should be broken in 3 intervals: -6->-4 | -4->-3 | -3->-2 | -2->0 | 0->1")
        print_calls(calls)
    if False:
        print("Testing subinterval and second strategy")
        itvals = RecordIntervalsPandas(subintervals_requiredQ=True, subinterval_minQ=True)
        calls = itvals(pd.Interval(-2, 0))
        print_calls(calls)
        calls = itvals(pd.Interval(-4, -3))
        print_calls(calls)
        calls = itvals(pd.Interval(-6, 1))
        print("should be broken in 3 intervals: -6->-4 | -4->-3 | -3->-2 | -2->0 | 0->1")
        print_calls(calls)
