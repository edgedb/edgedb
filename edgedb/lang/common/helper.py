def dump(stuff):
    if (not (isinstance(stuff, str) or isinstance(stuff, int)
             or isinstance(stuff, list) or isinstance(stuff, dict)
             or isinstance(stuff, tuple) or isinstance(stuff, float)
             or isinstance(stuff, complex))):

        buf = ['%r : %s' % (stuff, str(stuff))]

        for name in dir(stuff):
            attr = getattr(stuff, name)

            if not hasattr(attr, '__call__'):
                buf.append('  -> %s : %s' % (name, attr))

        print('\n'.join(buf) + '\n')

    else:
        import pprint
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(stuff)


def cleandir(path):
    import os

    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))

        for name in dirs:
            os.rmdir(os.path.join(root, name))


def init_test_suite():
    import inspect
    import unittest

    _globals = inspect.stack()[1][0].f_globals

    def init():
        suite = unittest.TestSuite()
        for name, attr in _globals.items():
            if isinstance(attr, type) and issubclass(attr, unittest.TestCase):
                suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(attr))

        return suite

    _globals['suite'] = init
