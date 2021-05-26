import itertools


def pairwise(iterable):
    # See SO iterate a list as pair current next in python
    '''
    s -> (s0,s1), (s1, s2)
    '''
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a,b)

def last_it(iterable, length=-1, at_least_n_elts=0):
    '''
    Gets efficiently the last iterator
    :param length: length if known
    :param at_least_n_elts: safe jump if known
    '''
    if length >0:
        return next(itertools.islice(iterable, length-1, length))
    rest = itertools.islice(iterable, at_least_n_elts, None)
    last = next(rest)
    for last in rest:
        pass
    return last

##########################################################
#
# There is no native too to flatten a list of lists.  
# itertool chain flattens all iterables
# When working with nested list of tuples: not practical
#
##########################################################
def nested_item(depth, value):
    if depth <= 1:
        return [value]
    else:
        return [nested_item(depth -1, value)]

def nested_list(n):
   '''
   generate a nested list where the i'th item
   at depth i
   '''
   lis = []
   for i in range(n):
       if i==0:
           lis.append(i)
       else:
           lis.append(nested_item(i,i))

   return lis


def flatten(lis):
    '''
    Given a list, possibly nested to any level
    return it flattened
    '''
    new_lis = []
    for item in lis:
        if type(item) == type([]):
            new_lis.extend(flatten(item))
        else:
            new_lis.append(item)
    return new_lis

def chunks(lst, n):
    '''
    Yield successive n-sized chunks from list
    '''
    for i in range(0, len(lst), n):
          yield lst[i:i+n]



if __name__ == '__main__':
    import loguru
    if True:
        for b in chunks(range(1,150), 10):
            loguru.logger.info(b)
