import inspect
from collections import OrderedDict

# SO how can I read a function signature including default argument values 25076824
def get_signature(fn):
    params = inspect.signature(fn).parameters
    args = []
    kwargs = OrderedDict()
    for p in params.values():
        if p.default is p.empty:
            args.append(p.name)
        else:
            kwargs[p.name] = p.default
    return args, kwargs

class ArgsSolver:
    def __init__(self, f, split_args_kwargsQ=True):
       self.sig = get_signature(f)
       self.split_args_kwargsQ = split_args_kwargsQ

    def __call__(self, *args, **kwargs):
        args_and_values = OrderedDict()
        if self.split_args_kwargsQ:
            kwargs_and_values = OrderedDict()
            args_only_and_values = OrderedDict()
        n = len(args)
        i = 0
        for i, arg in enumerate(self.sig[0]):
            # First process args
            if self.split_args_kwargsQ:
                args_only_and_values[arg] = args[i]
            else:
                args_and_values[arg] = args[i]
        # process kwargs
        for kwarg in self.sig[1].keys():
            # 1. kwargs input as positional
            if n > 0 and i < n-1:
                i += 1
                if self.split_args_kwargsQ:
                    kwargs_and_values[kwarg] = args[i]
                else:
                    args_and_values[kwarg] = args[i]
            # 2. kwargs explicitly given
            elif kwarg in kwargs:
                if self.split_args_kwargsQ:
                    kwargs_and_values[kwarg] = kwargs[kwarg]
                else:
                    args_and_values[kwarg] = kwargs[kwarg]
            # 3. default value
            else:
                if self.split_args_kwargsQ:
                    kwargs_and_values[kwarg] = self.sig[1][kwarg]
                else:
                    args_and_values[kwarg] = self.sig[1][kwarg]
        if self.split_args_kwargsQ: return args_only_and_values, kwargs_and_values
        return args_and_values

if __name__ == '__main__':
    import loguru
    import daiquiri
    import pandas as pd
    import pendulum as pdl
    from toolboxcg.Dates import pdl2pd

    tsthreedaysago = pdl2pd(pdl.yesterday('UTC').add(days=-2))
    tstwodaysago = pdl2pd(pdl.yesterday('UTC').add(days=-1))
    tsyesterday = pdl2pd(pdl.yesterday('UTC'))
    tstoday = pdl2pd(pdl.today('UTC'))
    tstomorrow = pdl2pd(pdl.tomorrow('UTC'))
    tsintwodays = pdl2pd(pdl.tomorrow('UTC').add(days=1))
    tsinthreedays = pdl2pd(pdl.tomorrow('UTC').add(days=2))
    if False:
        def test_sig():
            def fn(a, b, c, d=3, e="abc"):
                pass

            assert get_signature(fn) == (
                ["a", "b", "c"], 
                OrderedDict([("d", 3), ("e", "abc") ])
            )
        test_sig()
    if False:
        def f(*args, **kwargs):
            loguru.logger.info(args)
            loguru.logger.info(kwargs)

        f('a', y1=1, y2=2)
        f('a', 1, y2=2)
    if False:
        i = 1
        ++i
        print(i)
        def test_arg_called():
            def fn(a, b,  k1='k', k2="l" ):
                pass

            find_args_fn = ArgsSolver(fn, split_args_kwargsQ=False)
            if True:
                args  = find_args_fn(1, 2, 3, 4)
                loguru.logger.info(args)
                assert args['a']==1 and args['b']==2 and args['k1']==3 and args['k2']==4

            if True:
                args = find_args_fn(1, 2, k1=3, k2=4)
                loguru.logger.info(args)
                assert args['a'] == 1 and args['b'] == 2 and args['k1'] == 3 and args['k2'] == 4

            if True:
                args = find_args_fn(1, 2, k2=4)
                loguru.logger.info(args)
                assert args['a'] == 1 and args['b'] == 2 and args['k1'] == 'k' and args['k2'] == 4

            if True:
                args = find_args_fn(1, 2, k1=3)
                loguru.logger.info(args)
                assert args['a'] == 1 and args['b'] == 2 and args['k1'] == 3 and args['k2'] == 'l'

            if True:
                args = find_args_fn(1, 2)
                loguru.logger.info(args)
                assert args['a'] == 1 and args['b'] == 2 and args['k1'] == 'k' and args['k2'] == 'l'
        test_arg_called()

    if False:
        def test_arg_kwargs_called():
            def fn(a, b, k1='k', k2="l"):
                pass

            find_args_fn = ArgsSolver(fn, split_args_kwargsQ=True)
            if True:
                args, kwargs = find_args_fn(1, 2, 3, 4)
                loguru.logger.info(args)
                loguru.logger.info(kwargs)
                assert args['a'] == 1 and args['b'] == 2 and kwargs['k1'] == 3 and kwargs['k2'] == 4

            if True:
                args, kwargs = find_args_fn(1, 2, k1=3, k2=4)
                loguru.logger.info(args)
                loguru.logger.info(kwargs)
                assert args['a'] == 1 and args['b'] == 2 and kwargs['k1'] == 3 and kwargs['k2'] == 4

            if True:
                args, kwargs = find_args_fn(1, 2, k2=4)
                loguru.logger.info(args)
                loguru.logger.info(kwargs)
                assert args['a'] == 1 and args['b'] == 2 and kwargs['k1'] == 'k' and kwargs['k2'] == 4

            if True:
                args, kwargs = find_args_fn(1, 2, k1=3)
                loguru.logger.info(args)
                loguru.logger.info(kwargs)
                assert args['a'] == 1 and args['b'] == 2 and kwargs['k1'] == 3 and kwargs['k2'] == 'l'

            if True:
                args, kwargs = find_args_fn(1, 2)
                loguru.logger.info(args)
                loguru.logger.info(kwargs)
                assert args['a'] == 1 and args['b'] == 2 and kwargs['k1'] == 'k' and kwargs['k2'] == 'l'
        test_arg_kwargs_called()
    if False:


        def function_with_interval_param(dummy1, dummy2, kdummy=1,
                                         interval=pd.Interval(tstwodaysago, tstomorrow)):
            print('**********************************')
            print(f'dummy1: {dummy1}, dummy2: {dummy2}')
            print(f'kdummy: {kdummy}')
            print(f'interval: {interval}')
            return 0
        find_args_fin = ArgsSolver(function_with_interval_param)
    if True:
        timenow = pdl.now()
        timenowplus5s = timenow.add(seconds=5)
        fiveseconds = timenowplus5s - timenow


        def function_with_interval_params(array=['USD/JPY'],
                                          period=pd.Interval(tsyesterday, pd.Timestamp.now(tz="UTC"))
                                          ):
            print('************* function called *********************')
            print(f'interval0: {period}')
            return (period)

        find_args_fin = ArgsSolver(function_with_interval_params)

        args,kwargs = find_args_fin(['USD/JPY'])
        print()
        print('==== First pass ===')

        print(
            f'Final result: {function_with_interval_params(array=["USD/JPY"], period=pd.Interval(tstoday, pd.Timestamp.now(tz="UTC")))}'
        )
