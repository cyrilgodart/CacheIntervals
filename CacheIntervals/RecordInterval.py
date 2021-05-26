import itertools
import portion
import pendulum as pdl

from CacheIntervals.utils import flatten
from CacheIntervals.Intervals import po2pd, pd2po

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
        self.subintervalsQ = subintervals_requiredQ
        self.subintervals_minQ = subinterval_minQ

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
                    intervals = intervals | s  # itertools.accumulate(itvls_overlap, lambda i,o: i | o   )
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
                    # self.disjunct(i)

                    inter = portion.empty()
                    diffs = []
                    for s in self.intervals:
                        diffs.append(i - s)
                    diff = list(itertools.accumulate(diffs, lambda x, y: x & y))
                    if len(diff):
                        diff = diff[-1]
                        for s in diff:
                            self.disjunct(s)
                    inter = []
                    for s in self.intervals:
                        inter = s & i
                        for ii in inter:
                            if ii not in self.intervals:
                                self.disjunct(ii)
                            else:
                                if ii not in self.calls:
                                    self.contained(ii)

        calls = sorted(flatten(self.calls))
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

if __name__ == "__main__":
    import logging
    import daiquiri
    import pandas as pd
    import loguru
    from CacheIntervals.utils.Dates import pdl2pd, pd2pdl, all2pdl

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
        if isinstance(calls[0], pd.Interval):
            print(list(map(lambda i: (i.left, i.right), calls)))
        else:
            print(list(map(lambda i: (i.lower, i.upper), calls)))


    def print_calls_dates(calls,itv_pandasQ = True):
        if itv_pandasQ:
            print( list( map( lambda i:
                              (all2pdl(i.left).to_date_string(), all2pdl(i.right).to_date_string()),
                              calls)))
        else:
            print(list(map(lambda i:
                           (all2pdl(i.lower).to_date_string(), all2pdl(i.upper).to_date_string()),
                           calls)))


    def display_calls(calls, ):
        if isinstance(calls[0], pd.Interval):
            loguru.logger.info( list( map( lambda i:
                                           (pd2pdl(i.left).to_date_string(), pd2pdl(i.right).to_date_string()),
                                           calls)))
        else:
            loguru.logger.info( list( map( lambda i:
                                           (pd2pdl(i.lower).to_date_string(), pd2pdl(i.upper).to_date_string()),
                                           calls)))

    #                               Testing record intervals -> ok
    if True:
        itvals = RecordIntervals()
        calls = itvals(portion.closed(pdl.yesterday(), pdl.today()))
        print_calls_dates(calls, False)
        print(list(map(lambda i: type(i), calls)))
        calls = itvals( portion.closed(pdl.yesterday().add(days=-1), pdl.today().add(days=1)))
        print_calls_dates(calls, False)
    #                            Testing record intervals pandas -> ok
    if True:
        itvals = RecordIntervalsPandas()
        # yesterday -> today
        calls = itvals(pd.Interval(pdl2pd(pdl.yesterday()), pdl2pd(pdl.today()), closed='left'))
        print_calls_dates(calls)
        # day before yesterday -> tomorrow: should yield 3 intervals
        calls = itvals(pd.Interval(pdl2pd(pdl.yesterday().add(days=-1)), pdl2pd(pdl.today().add(days=1))))
        print_calls_dates(calls)
        # day before yesterday -> day after tomorrow: should yield 4 intervals
        calls = itvals(
            pd.Interval(pdl2pd(pdl.yesterday().add(days=-1)),
                        pdl2pd(pdl.tomorrow().add(days=1))))
        print_calls_dates(calls)
        # 2 days before yesterday -> 2day after tomorrow: should yield 6 intervals
        calls = itvals(
            pd.Interval(pdl2pd(pdl.yesterday().add(days=-2)),
                        pdl2pd(pdl.tomorrow().add(days=2))))
        print_calls_dates(calls)
    #                         Further tests on record intervals pandas
    if True:
        itvals = RecordIntervalsPandas()
        calls = itvals(pd.Interval(tstwodaysago, tstomorrow, closed='left'))
        display_calls(calls)
        calls = itvals( pd.Interval(tstwodaysago, tsyesterday))
        display_calls(calls)
        calls = itvals(
            pd.Interval(tstwodaysago, tsintwodays))
        display_calls(calls)
        calls = itvals( pd.Interval(tsthreedaysago, tsintwodays, closed='left'))
        display_calls(calls)
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
