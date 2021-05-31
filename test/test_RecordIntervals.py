import itertools
import portion
import pendulum as pdl

from CacheIntervals.utils import flatten
from CacheIntervals.Intervals import po2pd, pd2po

import logging
import daiquiri
import pandas as pd
import loguru
from CacheIntervals.utils.Dates import pdl2pd, pd2pdl, all2pdl
from CacheIntervals.RecordInterval import RecordIntervals, RecordIntervalsPandas

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


def test_RI_0():
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
    itvals = RecordIntervals()
    itvals(portion.closed(pdl.yesterday(), pdl.today()))
    calls = itvals(portion.closed(pdl.yesterday().add(days=-1), pdl.tomorrow()))
    expected = [portion.closedopen(pdl.yesterday().add(days=-1), pdl.yesterday()),
                portion.closed(pdl.yesterday(), pdl.today()),
                portion.openclosed(pdl.today(), pdl.tomorrow())]
    for a, b in zip(calls, expected):
        assert a == b


def test_RIP_0():
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
    itvals = RecordIntervalsPandas()
    itvals(pd.Interval(pdl2pd(pdl.yesterday()),pdl2pd(pdl.today())))
    calls = itvals(pd.Interval(pdl2pd(pdl.yesterday().add(days=-1)), pdl2pd(pdl.tomorrow())))
    expected = [pd.Interval(pdl2pd(pdl.yesterday().add(days=-1)), pdl2pd(pdl.yesterday())),
                pd.Interval(pdl2pd(pdl.yesterday()), pdl2pd(pdl.today())),
                pd.Interval(pdl2pd(pdl.today()), pdl2pd(pdl.tomorrow()))]
    for a, b in zip(calls, expected):
        assert a == b
