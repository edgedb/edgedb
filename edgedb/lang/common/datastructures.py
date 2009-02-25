import bisect

class GenericWrapperMeta(type):

    def __init__(cls, name, bases, dict):
        super().__init__(name, bases, dict)
        cls._sethook('read')
        cls._sethook('write')


class GenericWrapper(object, metaclass=GenericWrapperMeta):
    write_methods = ()
    read_methods = ()

    @classmethod
    def _sethook(cls, hookname):
        hookmethod = '_' + hookname + '_hook'
        if hasattr(cls, hookmethod):
            hook = getattr(cls, hookmethod)
            methods = getattr(cls, hookname + '_methods')

            if hook:
                for method in methods:
                    setattr(cls, method, cls._gethook(hook, method))

    @classmethod
    def _gethook(cls, hook, method):
        def hookbody(self, *args, **kwargs):
            hook(self, method, *args, **kwargs)
            original = getattr(cls.original_base, method)
            return original(self, *args, **kwargs)

        return hookbody


class SetWrapper(set, GenericWrapper):

    write_methods = ('update', 'intersection_update', 'difference_update', 'symmetric_difference_update',
                     'add', 'remove', 'discard', 'pop', 'clear')

    read_methods = ('__iter__', '__len__', '__contains__', 'isdisjoint', 'issubset', '__le__', '__lt__',
                    'issuperset', '__ge__', '__gt__', 'union', '__or__', 'intersection', '__and__',
                    'difference', '__sub__', 'symmetric_difference', '__xor__')

    original_base = set


class SortedList(list):
    """
    A list that maintains it's order by a given criteria
    """

    def __init__(self, data=None):
        if data is None:
            data = []

        super(SortedList, self).__init__(data)

        self.sort()
        self.appending = False
        self.extending = False
        self.inserting = False

    def append(self, x):
        if not self.appending:
            self.appending = True
            bisect.insort_right(self, x)
            self.appending = False
        else:
            super(SortedList, self).append(x)

    def extend(self, L):
        if not self.extending:
            self.extending = True
            for x in L:
                bisect.insort_right(self, x)
            self.extending = False
        else:
            super(SortedList, self).extend(L)

    def insert(self, i, x):
        if not self.inserting:
            self.inserting = True
            bisect.insort_right(self, x)
            self.inserting = False
        else:
            super(SortedList, self).insert(i, x)
