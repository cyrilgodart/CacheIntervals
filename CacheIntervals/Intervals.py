import portion as po
import numpy as np
import pandas as pd

def pd2po(i):
    '''
    convert a pandas interval to a portion interval
    :param i: a pandas interval 
    :return  a portion interval 
    ''' 

    if not isinstance(i, pd.Interval): raise Exception('Not a pandas interval')
    left = po.CLOSED if i.closed_left else po.OPEN
    right = po.CLOSED if i.closed_right else po.OPEN
    lower = i.left
    upper = i.right
    if lower==upper:
        if left == po.OPEN and right == po.OPEN:
            return po.empty()
        else:
            return po.singleton(lower)
    if left != right and left == po.CLOSED:
        return po.closedopen( lower, upper)
    if left != right and left == po.OPEN:
        return po.openclosed( lower, upper)
    if left == right and left == po.CLOSED:
        return po.closed( lower, upper)
    if left == right and left == po.OPEN:
        return po.open( lower, upper)

def infpo2np(bound):
    if bound==po.inf: return np.inf
    if bound==-po.inf: return -np.inf
    return bound

def po2pd(i):
    '''
    convert a portion atomic interval to a pandas Interval
    :param i: a portion atomic interval
    :return: a pandas interval
    '''
    if not isinstance(i, po.Interval) and not i.atomic: raise Exception('Not an atomic Portion Interval')
    if i.empty: return pd.Interval(0, 0, closed='neither')
    closed_left = True if i.left == po.CLOSED else False
    closed_right = True if i.right == po.CLOSED else False
    closed = 'neither'
    if closed_left and not closed_right:
        closed = 'left'
    if closed_right and not closed_left:
        closed = 'right'
    if closed_right and closed_left:
        closed = 'both'
    left = infpo2np(i.lower)
    right = infpo2np(i.upper)
    if closed is not None:
        return pd.Interval(left=left, right=right, closed=closed)


if __name__ == "__main__":
    import logging
    import daiquiri
    import pandas as pd
    daiquiri.setup(logging.DEBUG)
    logging.getLogger('requests_kerberos').setLevel(logging.WARNING)

    if True:
        print('Conversion from pandas to portion')
        print(pd2po(pd.Interval(0, 0, closed="neither")))
        print(pd2po(pd.Interval(1, 1, closed="left")))
        print(pd2po(pd.Interval(1, 1, closed="both")))
        print(pd2po(pd.Interval(0, 1, closed="neither")))
        print(pd2po(pd.Interval(0, 1, closed="left")))
        print(pd2po(pd.Interval(0, 1, closed="right")))
        print(pd2po(pd.Interval(0, 1, closed="both")))
    if True:
        print('Conversion from portion to pandas:')
        print(po2pd(po.open(po.inf, -po.inf)))
        print(po2pd(po.open(-po.inf, po.inf)))
        print(po2pd(po.open(0, 1)))
        print(po2pd(po.closedopen(0, 1)))
        print(po2pd(po.openclosed(0, 1)))
        print(po2pd(po.closed(0, 1)))
    
