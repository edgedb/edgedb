import py
import re


test_patterns = []


def pytest_addoption(parser):
    parser.addoption("--semantix-debug", dest="semantix_debug", action="append")
    parser.addoption("--tests", dest="test_patterns", action="append")


def pytest_configure(config):
    global test_patterns, semantix_debug

    patterns = []
    tp = config.getvalue('test_patterns')
    if tp:
        for t in tp:
            patterns.extend(t.split(","))

        test_patterns = [re.compile(p) for p in patterns]

    sd = config.getvalue('semantix_debug')
    if sd:
        debug = []

        for d in sd:
            debug.extend(d.split(","))

        import semantix.utils.debug
        semantix.utils.debug.enabled = True
        semantix.utils.debug.channels.update(debug)


def pytest_pycollect_makeitem(__multicall__, collector, name, obj):
    item = __multicall__.execute()
    result = item

    if isinstance(item, py.test.collect.Function):
        if test_patterns:
            func = item.obj

            name = func.__name__
            if name.startswith('test_'):
                name = name[5:]

            for p in test_patterns:
                if p.match(name):
                    func = getattr(func, '__func__', func)
                    setattr(func, 'testmask', py.test.mark.Marker('testmask'))
                    break

    return result
