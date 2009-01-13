__all__ = ['check']

def check(variable, type):
    if not isinstance(type, str):
        raise Exception('check_type: type parameter must be string')

    if variable is None:
        return True

    if type == 'str':
        return isinstance(variable, str)

    if type == 'int':
        return isinstance(variable, int)

    if type == 'float':
        return isinstance(variable, float)

    if type == 'bool':
        return isinstance(variable, bool)

    if type == 'list':
        return isinstance(variable, list)

    if type == 'none':
        return variable is None

    raise Exception('check_type: checking on unknown type: %s' % type)
