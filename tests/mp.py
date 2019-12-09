__author__ = "Altertech Group, https://www.altertech.com/"
__copyright__ = "Copyright (C) 2018-2019 Altertech Group"
__license__ = "Apache License 2.0"
__version__ = "0.7.6"

def test(*args, **kwargs):
    print('test mp method {} {}'.format(args, kwargs))
    return 999

def test_mp(a, x, **kwargs):
    return a + x

def test2(*args, **kwargs):
    return 999
