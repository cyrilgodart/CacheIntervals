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
import numpy as np

import pandas as pd
import pendulum as pdl
import sqlite3
import time
import klepto
from datetime import date, datetime

from CacheIntervals import MemoizationWithIntervals
from CacheIntervals.utils.Timer import Timer


name_db_file_test1 = "../test/test1.sqlite"
delay = 2

def get_records(conn, name_table, period =  pd.Interval(pd.Timestamp(2021, 1,1), pd.Timestamp(2021, 1, 31))):
    time.sleep(delay)
    query = f"Select * From {name_table} Where date(date) between date('{period.left.date()}') and date('{period.right.date()}')"
    #query   = f'Select * From {name_table} '
    loguru.logger.debug(query)
    df = pd.read_sql(query, conn)
    return df


cache_itvls =MemoizationWithIntervals(
    [], ['period'],
    aggregation=pd.concat,
    debug=True,
    memoization=klepto.lru_cache(
        maxsize=500,
        cache=klepto.archives.dict_archive(),
        keymap=klepto.keymaps.stringmap(typed=False, flat=False)))

get_records_cached = cache_itvls(get_records)



cache_itvls_concat_with_tolerance = MemoizationWithIntervals(
    [], ['period'],
    aggregation=pd.concat,
    debug=False,
    memoization=klepto.lru_cache(
        maxsize=500,
        cache=klepto.archives.dict_archive(),
        keymap=klepto.keymaps.stringmap(typed=False, flat=False)),
    rounding = pdl.today()-pdl.yesterday()
)

get_records_cached_with_tolerance_1day = cache_itvls_concat_with_tolerance(get_records)


def caching_with_tolerance():
    with Timer() as timer_no_cache:
        df_jan = get_records(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2021, 1, 31)))
    #   activate caching
    get_records_cached_with_tolerance_1day(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 1, 1),
                                                                          pd.Timestamp(2021, 1, 31)))
    df_jan_cached = None
    with Timer() as timer_cache:
        df_jan_cached = get_records_cached_with_tolerance_1day(cnx_file, "test1",
                                           pd.Interval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2021, 2, 1)))

    loguru.logger.debug(f'\n{df_jan_cached.sort_values(by="date")}')
    assert timer_cache.interval < timer_no_cache.interval



def accesss_cached_function():
    get_records_cached(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 2, 1), pd.Timestamp(2021, 3, 31)))
    f_cached = get_records_cached(cnx_file, "test1", get_function_cachedQ=True)
    return f_cached.info().hit, f_cached.info().miss, f_cached.info().load


########################################################################################################
#
#                     Testing caching with aggregation-type operations
#
########################################################################################################

def agg_cumul(listdf):
    loguru.logger.debug(f'list dfs:{listdf}')
    listdf = [df for df in listdf if not (df is None) and not (df.empty)]
    if len(listdf):
        df = reduce(lambda x, y: x.add(y, fill_value=0), listdf)
    else:
        raise Exception("Nothing to aggregate")
    return df

def cumulate_records(conn, name_table, period=pd.Interval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2021, 1, 31))):
    time.sleep(delay)  # simulating a long SQL request
    query = f"Select currency, sum(amount_in_eur) " \
            f"From {name_table} " \
            f"Where date(date) >= date('{period.left.date()}') and date(date) < date('{period.right.date()}')" \
            f"Group by currency"
    loguru.logger.debug(query)
    df = pd.read_sql(query, conn)
    df = df.set_index('currency', drop=True)
    df.columns = ['total']
    df['total'] = df['total'].astype(float)
    return df

cache_itvls_agg = MemoizationWithIntervals(
    [],
    ['period'],
    aggregation=agg_cumul,
    debug=True,
    memoization=klepto.lru_cache(
        maxsize=500,
        cache=klepto.archives.dict_archive(),
        keymap=klepto.keymaps.stringmap(typed=False, flat=False)),
    subintervals_requiredQ=True # extra-kwarg are passed to RecordInterval constructor
)


cumulate_records_cached  = cache_itvls_agg(cumulate_records)


def caching_aggregation():
    with Timer() as timer_no_cache:
        df_janmar = cumulate_records(cnx_file,
                                      "test1",
                                      pd.Interval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2021, 4, 1)))
    #   activate caching
    df_jan = cumulate_records_cached(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 1, 1),
                                                        pd.Timestamp(2021, 2, 1)))
    df_febmar = cumulate_records_cached(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 2, 1),
                                                            pd.Timestamp(2021, 4, 1)))
    with Timer() as timer_cache:
        df_janmar_cached = cumulate_records_cached(cnx_file,
                                                    "test1",
                                                    pd.Interval(pd.Timestamp(2021, 1, 1),
                                                                pd.Timestamp(2021, 4, 1)))

    loguru.logger.debug(f'no cache: \n{df_janmar}')
    loguru.logger.debug(f'cached: \n{df_janmar_cached}')

    loguru.logger.debug(f'jan: \n{df_jan}')
    loguru.logger.debug(f'feb-mar:\n{df_febmar}')
    df_compare = pd.concat({'nocache': df_janmar, 'cache' : df_janmar_cached}, axis=1)
    df_compare = df_compare.assign(zediff = lambda x: x[('cache', 'total')] - x[('nocache', 'total')])
    df_compare = df_compare.assign(zediff = lambda x: x.zediff.apply(abs))
    loguru.logger.debug(f'diff :\n{df_compare[df_compare.zediff>1]}')
    assert np.isclose(df_janmar.total, df_janmar_cached.total, 0.1).all()

    assert timer_cache.interval < timer_no_cache.interval


if __name__ == '__main__':
        import logging
        import daiquiri
        daiquiri.setup(level=logging.DEBUG)
        name_csv_test1 = "test1.gz"


        cnx_file = sqlite3.connect(name_db_file_test1)
        if False:
            df = pd.read_sql('Select * from test1', cnx_file)
            loguru.logger.debug(f'DB content:\n{df[df.date<"2021-01-04"].groupby(["date","currency"]).sum()}')

        if False:
            df = get_records(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 1,1), pd.Timestamp(2021, 1, 31)))
            loguru.logger.debug(f'\n{df}')

            df = get_records_cached(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 1,1), pd.Timestamp(2021, 1, 31)))

            df = get_records_cached(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 1,1), pd.Timestamp(2021, 1, 31)))
            loguru.logger.debug(f'\n{df}')
        if False:
            caching_with_tolerance()
        if False:
            hits, miss, load = accesss_cached_function()
            loguru.logger.info(f'hits: {hits}, miss: {miss}, load: {load}')
        if True:
            df1 = cumulate_records(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 1,1), pd.Timestamp(2021, 2, 1)))
            loguru.logger.debug(f'Jan 1st result: {df1}')
            df2 = cumulate_records(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 2, 1), pd.Timestamp(2021, 4, 1)))
            loguru.logger.debug(f'Jan 2nd result: {df2}')
            df3 = cumulate_records(cnx_file, "test1", pd.Interval(pd.Timestamp(2021, 1, 1), pd.Timestamp(2021, 4, 1)))
            loguru.logger.debug(f'Jan 1st-2nd result: {df3}')
            # loguru.logger.debug(f'Df index: {df.index}')

            caching_aggregation()

