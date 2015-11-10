##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import functools
import copy

from .schema import Schema as FsmSchema

__all__ = ['FSMError', 'FSMMeta', 'FSM', 'FSMDescriptor', 'action']

class FSMError(Exception):
    pass

class FSMDescriptor(object):
    def __init__(self, data=None, validate=True):
        if data:
            if validate:
                self.data = FsmSchema.check(data)
            else:
                self.data = data
        else:
            self.data = dict()


class FSMMeta(type):
    queue = {}

    def __init__(cls, name, bases, dct):
        super(FSMMeta, cls).__init__(name, bases, dct)
        for c in bases:
            if hasattr(c, 'action_map'):
                cq = getattr(c, 'action_map')
                for t, l in cq.items():
                    if t not in FSMMeta.queue:
                        FSMMeta.queue[t] = l

        setattr(cls, 'action_map', FSMMeta.queue)
        FSMMeta.queue = {}

    @staticmethod
    def action(__action, stack=None, stack_autoselect=True):
        def wrap(func):
            if __action in FSMMeta.queue:
                raise FSMError('action redefinition: %s' % t);

            if stack is None:
                def wrap_fn(*args, **kwargs):
                    return func(*args, **kwargs)
            else:
                def wrap_fn(*args, **kwargs):
                    if 'self' not in kwargs:
                        return func(*args, **kwargs)

                    obj = kwargs['self']
                    if stack not in obj.stacks:
                        obj.stacks[stack] = {}

                    if stack_autoselect:
                        if 'action_param' in kwargs:
                            substack = kwargs['action_param']
                        else:
                            substack = 'default'

                        if substack not in obj.stacks[stack]:
                            obj.stacks[stack][substack] = []

                        s = obj.stacks[stack][substack]
                    else:
                        s = obj.stacks[stack]

                    kwargs['stack'] = s

                    return func(*args, **kwargs)

            FSMMeta.queue[__action] = wrap_fn
            return wrap_fn
        return wrap

action = FSMMeta.action

class FSM(object, metaclass=FSMMeta):
    def __init__(self, descriptor, input_feed=None):
        self.initial_state = descriptor['start']
        self.current_state = self.initial_state

        self.stacks = {}

        self.transitions = {}

        self.input = None
        self.input_queue = []

        if input_feed is None:
            self._input_feed = self.input_feed
        else:
            self._input_feed = input_feed

        for state in descriptor['transitions']:
            if state == '*':
                s = None
            else:
                s = state

            for transition in descriptor['transitions'][state]:
                for input, transition_data in transition.items():
                    if input == '*':
                        i = None
                    else:
                        i = input

                    td = copy.deepcopy(transition_data)

                    if 'actions' in td:
                        actions = []
                        for action in td['actions']:
                            action_name = action.keys()[0]
                            action_value = action.values()[0]

                            if action_name == 'custom':
                                exec('def _code(params):\n' \
                                     + '\n'.join(['    ' + line for line in action_value.split('\n') if line.strip()]))
                                action = _code
                            elif action_name in self.action_map:
                                action = functools.partial(self.action_map[action_name], self=self, action_param=action_value)
                            elif hasattr(self, action_name):
                                action = functools.partial(getattr(self, action_name), self=self)
                            else:
                                raise FSMError('undefined transition action: %s' % action_name)

                            actions.append(action)
                        td['actions'] = actions

                    self.transitions[(s, i)] = td

        if 'events' in descriptor:
            for event in descriptor['events']:
                self.add_event(event.keys()[0], event.values()[0])

        self.next_state = None

    def add_event(self, event, params):
        self.input_queue.append((event, params))

    def input_feed(self):
        raise NotImplementedError()

    def get_transition(self, state, input):
        if (state, input) in self.transitions:
            return self.transitions[(state, input)]
        elif (state, None) in self.transitions:
            return self.transitions[(state, None)]
        elif (None, input) in self.transitions:
            return self.transitions[(None, input)]
        elif (None, None) in self.transitions:
            return self.transitions[(None, None)]
        else:
            raise FSMError('undefined transition for (%s, %s)' %
                                (str(state), str(input)))

    def cycle(self, event, input):
        self.event = event
        self.input = input

        transition = self.get_transition(self.current_state, event)

        if 'new-state' not in transition or transition['new-state'] is None:
            self.next_state = self.current_state
        else:
            self.next_state = transition['new-state']

        if 'actions' in transition is not None:
            for action in transition['actions']:
                action(**self.input)

        self.current_state = self.next_state
        self.next_state = None

    def input_iterator(self):
        class iterator(object):
            def __init__(self, fsm):
                self.fsm = fsm
                self.input_feed = self.fsm._input_feed()

            def __iter__(self):
                return self

            def __next__(self):
                if len(self.fsm.input_queue):
                    result = self.fsm.input_queue.pop(0)
                else:
                    result = self.input_feed.next()

                if result is None:
                    raise StopIteration()

                return result

        return iterator(self)

    def run(self):
        for (event, input) in self.input_iterator():
            self.cycle(event, input)

    @action('push-state', stack='state')
    def _push_state(self, stack, **kwargs):
        stack.append(self.current_state)

    @action('pop-state', stack='state')
    def _pop_state(self, stack, **kwargs):
        self.next_state = stack.pop()

    @action('reemit')
    def _reemit(self, **kwargs):
        self.input_queue.append((self.event, self.input))
