import pendulum
import pandas
import datetime


def pdl2pd(dt_pdl):
    return pandas.to_datetime(dt_pdl.isoformat())

def pd2pdl(dt_pd):
    return pendulum.instance(dt_pd)

def all2pdl(dt):
    if isinstance(dt, pendulum.Date): return dt
    if isinstance(dt, pandas.Timestamp) or isinstance(dt, datetime.datetime): return pd2pdl(dt)
    raise (f'Unknown date type: {dt}')

if __name__ == '__main__':
    import loguru
    #                Testing pdl2pd
    if True:
        dt = pendulum.Date(2021, 4, 28)
        loguru.logger.info(pdl2pd(dt))
        dt = pendulum.DateTime(2021, 4, 28, 12, 1, 55, 12345)
        loguru.logger.info(type(dt))
        ts = pdl2pd(dt)
        loguru.logger.info(ts)
        loguru.logger.info(type(ts))
        dt = pendulum.DateTime(2021, 4, 28, 12, 1, 55)
        loguru.logger.info(type(dt))
        ts = pdl2pd(dt)
    #                Testing pd2pdl
    if True:
        ts = pandas.Timestamp(2021, 4, 28)
        loguru.logger.info(type(ts))
        dt = pd2pdl(ts)
        loguru.logger.info(type(dt))
        loguru.logger.info(dt)
        ts = pandas.Timestamp(2021, 4, 28, 12, 1, 55)
        loguru.logger.info(type(ts))
        dt = pd2pdl(ts)
        loguru.logger.info(type(dt))
        loguru.logger.info(dt)
