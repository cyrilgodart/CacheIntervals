# CacheIntervals: Memoization with interval parameters
#
# Copyright (C) Cyril Godart
#
# This file is part of CacheIntervals.
#
# @author = 'Cyril Godart'
# @email = 'cyril.godart@gmail.com'

import logging
from functools import reduce

import loguru
import daiquiri

daiquiri.setup(level=logging.WARNING)

import pandas as pd
import sqlite3
import time
import klepto

from CacheIntervals import MemoizationWithIntervals
from CacheIntervals.utils.Timer import Timer

name_db_file_test1 = "../test/test1.sqlite"
delay = 2


########################################################################################################
#
#                     Testing caching with concatenation-type operations
#
########################################################################################################

def get_records(conn, name_table, period=pd.Interval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2021, 1, 31))):
    time.sleep(delay)  # simulating a long SQL request
    query = f"Select * From {name_table} " \
            f"Where date(date) between date('{period.left.date()}') and date('{period.right.date()}')"
    # query   = f'Select * From {name_table} '
    df = pd.read_sql(query, conn)
    return df

cache_itvls_concat = MemoizationWithIntervals(
    [], ['period'],
    aggregation=pd.concat,
    debug=False,
    memoization=klepto.lru_cache(
        maxsize=500,
        cache=klepto.archives.dict_archive(),
        keymap=klepto.keymaps.stringmap(typed=False, flat=False)))

get_records_cached = cache_itvls_concat(get_records)
#               Global objects
name_csv_test1 = "test1.gz"
cnx_file = sqlite3.connect(name_db_file_test1)

def test_caching1():
    with Timer() as timer_no_cache:
        df_jan = get_records(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2021, 1, 31)))
    #   activate caching
    get_records_cached(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2021, 1, 31)))
    df_jan_cached = None
    with Timer() as timer_cache:
        df_jan_cached = get_records_cached(cnx_file, "test1",
                                           pd.Interval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2021, 1, 31)))

    assert df_jan_cached.equals(df_jan)
    assert timer_cache.interval < timer_no_cache.interval


########################################################################################################
#
#                     Testing access to cached function
#
########################################################################################################

def test_access_cached_function():
    get_records_cached(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 2, 1), pd.Timestamp(2021, 3, 31)))
    f_cached = get_records_cached(cnx_file, "test1", get_function_cachedQ=True)
    assert f_cached.info().hit > 0
    assert f_cached.info().miss > 0
    assert f_cached.info().load == 0


########################################################################################################
#
#                     Testing tolerance
#
########################################################################################################

cache_itvls_concat_with_tolerance = MemoizationWithIntervals(
    [], ['period'],
    aggregation=pd.concat,
    debug=False,
    memoization=klepto.lru_cache(
        maxsize=500,
        cache=klepto.archives.dict_archive(),
        keymap=klepto.keymaps.stringmap(typed=False, flat=False)),
    rounding = pd.Timedelta('1d')
)

get_records_cached_with_tolerance_1day = cache_itvls_concat_with_tolerance(get_records)


def test_caching_with_tolerance():
    with Timer() as timer_no_cache:
        df_jan = get_records(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2021, 1, 31)))
    #   activate caching
    get_records_cached_with_tolerance_1day(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 1, 1),
                                                                          pd.Timestamp(2021, 1, 31)))
    df_jan_cached = None
    with Timer() as timer_cache:
        df_jan_cached = get_records_cached_with_tolerance_1day(cnx_file, "test1",
                                           pd.Interval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2021, 2, 1)))

    assert df_jan_cached.equals(df_jan)
    assert timer_cache.interval < timer_no_cache.interval

########################################################################################################
#
#                     Testing caching with aggregation-type operations
#
########################################################################################################

def agg_cumul(listdf):
    listdf = [df for df in listdf if not (df is None) and not (df.empty)]
    if len(listdf):
        df = reduce(lambda x, y: x.add(y, fill_value=0), listdf)
    else:
        raise Exception("Nothing to aggregate")
    return df

def aggregate_records(conn, name_table, period=pd.Interval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2021, 1, 31))):
    time.sleep(delay)  # simulating a long SQL request
    query = f"Select sum(amount_in_eur) " \
            f"From {name_table} " \
            f"Where date(date) >= date('{period.left.date()}') and date(date) < date('{period.right.date()}')" \
            f"Group by currency"
    df = pd.read_sql(query, conn)
    return df

cache_itvls_agg = MemoizationWithIntervals(
    [],
    ['period'],
    aggregation=agg_cumul,
    debug=False,
    memoization=klepto.lru_cache(
        maxsize=500,
        cache=klepto.archives.dict_archive(),
        keymap=klepto.keymaps.stringmap(typed=False, flat=False)),
    subintervals_requiredQ=True # extra-kwarg are passed to RecordInterval constructor
)


aggregate_records_cached  = cache_itvls_agg(aggregate_records)

def test_caching_aggregation():
    with Timer() as timer_no_cache:
        df_janmar = aggregate_records(cnx_file,
                                      "test1",
                                      pd.Interval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2021, 4, 1)))
    #   activate caching
    aggregate_records_cached(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 1, 1),
                                                                          pd.Timestamp(2021, 2, 1)))
    aggregate_records_cached(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 2, 1),
                                                            pd.Timestamp(2021, 4, 1)))
    with Timer() as timer_cache:
        df_janmar_cached = aggregate_records_cached(cnx_file,
                                              "test1",
                                              pd.Interval(pd.Timestamp(2021, 1, 1),
                                                          pd.Timestamp(2021, 4, 1)))

    assert df_janmar_cached.equals(df_janmar)
    assert timer_cache.interval < timer_no_cache.interval
