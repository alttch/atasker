def test(*args, **kwargs):
    print('test mp method {} {}'.format(args, kwargs))
    return 999

def test_mp(a, x, **kwargs):
    return a + x
