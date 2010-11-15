##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools


class State(int):
    def __repr__(self):
        return '<%s (%d)>' % (self, int(self))

    def __str__(self):
        return getattr(self, 'name', 'unknown')


class StateGenerator:
    state_cls = State

    def __init__(self):
        self.state_iter = itertools.count()
        self.states = {}

    def gen_state(self, count=1):
        nums = itertools.islice(self.state_iter, count)

        result = []
        for num in nums:
            self.states[num] = self.state_cls(num)
            result.append(self.states[num])

        if count == 1:
            return result[0]
        return result


class StatesMeta(type):
    state_gen = StateGenerator()
    gen_state = state_gen.gen_state

    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)

        # If class has any new states defined then try to determine
        # their names.
        #
        for attrname, attr in dct.items():
            if isinstance(attr, State):
                if attr in StatesMeta.state_gen.states:
                    StatesMeta.state_gen.states[attr].name = '%s.%s' % (name, attrname)
