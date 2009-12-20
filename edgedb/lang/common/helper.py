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


def dump_code_context(filename, lineno, dump_range=4):
    with open(filename, 'r') as file:
        source = file.read().split('\n')

    source_snippet = ''
    for j in range(max(0, lineno-dump_range), min(len(source), lineno+dump_range)):
        line = source[j] + '\n'

        if j == lineno:
            line = ' > ' + line
        else:
            line = ' | ' + line

        line = '{0:6}'.format(j) + line
        source_snippet += line

    return source_snippet
