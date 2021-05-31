import portion
import pendulum as pdl


import pandas as pd
from CacheIntervals.utils.Dates import pdl2pd, pd2pdl, all2pdl
from CacheIntervals.RecordInterval import RecordIntervals, RecordIntervalsPandas

def test_RI_0():
    # Simple case
    itvals = RecordIntervals()
    itvals(portion.closed(-2, 0))
    itvals(portion.closed(-1, 0))
    itvals(portion.closed(-3, -1))
    itvals(portion.closed(-5, -4))
    calls = itvals(portion.closed(-6, 0))
    expected = [portion.closedopen(-6, -5),
                portion.closed(-5, -4),
                portion.open(-4, -3),
                portion.closedopen(-3, -2),
                portion.closed(-2, 0),
                ]
    for a, b in zip(calls, expected):
        assert a == b


def test_RI_1():
    # Similar test with dates
    itvals = RecordIntervals()
    itvals(portion.closed(pdl.yesterday(), pdl.today()))
    calls = itvals(portion.closed(pdl.yesterday().add(days=-1), pdl.tomorrow()))
    expected = [portion.closedopen(pdl.yesterday().add(days=-1), pdl.yesterday()),
                portion.closed(pdl.yesterday(), pdl.today()),
                portion.openclosed(pdl.today(), pdl.tomorrow())]
    for a, b in zip(calls, expected):
        assert a == b


def test_RIP_0():
    # Two overlapping stored intervals
    itvals = RecordIntervalsPandas()
    itvals(pd.Interval(-2, 0))
    itvals(pd.Interval(-3, -2))
    itvals(pd.Interval(-6, -4))
    calls = itvals(pd.Interval(-5, -1))
    expected = [pd.Interval(-6, -4),
                pd.Interval(-4, -3),
                pd.Interval(-3, -2),
                pd.Interval(-2, 0),
                ]
    for a, b in zip(calls, expected):
        assert a == b


def test_RIP_subinterval_strat1():
    # Two overlapping stored intervals with subinterval set to True
    itvals = RecordIntervalsPandas(subintervals_requiredQ=True)
    itvals(pd.Interval(-2, 0))
    itvals(pd.Interval(-3, -2))
    itvals(pd.Interval(-6, -4))
    calls = itvals(pd.Interval(-5, -1))
    expected = [pd.Interval(-5, -3),
                pd.Interval(-3, -2),
                pd.Interval(-2, -1),
                ]
    for a, b in zip(calls, expected):
        assert a == b


def test_RIP_subinterval_strat2():
    # A strategy of dubious utility
    itvals = RecordIntervalsPandas(subintervals_requiredQ=True, subinterval_minQ=True)
    itvals(pd.Interval(-2, 0))
    itvals(pd.Interval(-3, -2))
    itvals(pd.Interval(-6, -4))
    calls = itvals(pd.Interval(-5, -1))
    expected = [pd.Interval(-5, -4),
                pd.Interval(-4, -3),
                pd.Interval(-3, -2),
                pd.Interval(-2, -1),
                ]
    for a, b in zip(calls, expected):
        assert a == b


def test_RIP_1():
    # test RecordIntervalsPandas with dates
    itvals = RecordIntervalsPandas()
    itvals(pd.Interval(pdl2pd(pdl.yesterday()),pdl2pd(pdl.today())))
    calls = itvals(pd.Interval(pdl2pd(pdl.yesterday().add(days=-1)), pdl2pd(pdl.tomorrow())))
    expected = [pd.Interval(pdl2pd(pdl.yesterday().add(days=-1)), pdl2pd(pdl.yesterday())),
                pd.Interval(pdl2pd(pdl.yesterday()), pdl2pd(pdl.today())),
                pd.Interval(pdl2pd(pdl.today()), pdl2pd(pdl.tomorrow()))]
    for a, b in zip(calls, expected):
        assert a == b
