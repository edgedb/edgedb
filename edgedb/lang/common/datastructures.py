import bisect
import collections

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


class OrderedSet(collections.MutableSet):

    def __init__(self, iterable=None):
        self.map = collections.OrderedDict()
        if iterable is not None:
            self |= iterable

    def __del__(self):
        self.clear()

    def add(self, key):
        if key not in self.map:
            self.map[key] = True

    def discard(self, key):
        if key in self.map:
            self.map.pop(key)

    def popitem(self, last=True):
        key, value = self.map.popitem(last)
        return key

    def __len__(self):
        return len(self.map)

    def __contains__(self, key):
        return key in self.map

    def __iter__(self):
        return iter(list(self.map.keys()))

    def __reversed__(self):
        return reversed(self.map)

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def __eq__(self, other):
        if isinstance(other, OrderedSet):
            return len(self) == len(other) and list(self) == list(other)
        return not self.isdisjoint(other)
