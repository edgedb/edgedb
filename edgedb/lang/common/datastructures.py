import bisect

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
