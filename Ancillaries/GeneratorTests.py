import random

import farsante
from mimesis import Business, Choice
from mimesis import Datetime


biz =lambda :random.choice(['EUR', 'JPY', 'CNH', 'USD'])
datetime = Datetime()
def rand_int(min_int, max_int):
    def some_rand_int():
        return random.randint(min_int, max_int)
    return some_rand_int

if __name__ == '__main__':
    import pandas
    import sqlite3
    import pathlib
    import loguru
    import pathlib

    path_test = pathlib.Path(__file__).parent.parent / "test"
    loguru.logger.debug(f'path test dir: {path_test}')

    name_db_file_test1 = "test1.sqlite"
    name_csv_test1 = "test1.gz"

    #cnx = sqlite3.connect(":memory:")
    cnx_file = sqlite3.connect(path_test / name_db_file_test1)

    df = farsante.pandas_df([
        lambda : datetime.date(start=2021,end=2021),
        biz,
        rand_int(1, 1e4)], 5000)
    df.columns = ['date', 'currency', 'amount_in_eur']
    df = df.sort_values(by='date')
    loguru.logger.debug(df)
    #df.to_csv(path_test/name_csv_test1, index=False, compression='gzip')
    df.to_sql('test1', cnx_file, if_exists='replace')


