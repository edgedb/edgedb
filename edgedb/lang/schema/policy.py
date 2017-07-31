##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.edgeql import ast as qlast

from . import delta as sd
from . import derivable
from . import inheriting
from . import name as sn
from . import named
from . import objects as so
from . import primary
from . import referencing


class Action(primary.PrimaryClass):
    _type = 'action'


class ActionSet(so.ClassSet, type=Action):
    pass


class Event(primary.PrimaryClass):
    _type = 'event'


class Policy(derivable.DerivableClass, primary.PrimaryClass):
    _type = 'policy'

    # Policy subject, i.e object in the schema to which
    # this policy is applied
    subject = so.Field(named.NamedClass, compcoef=0.714)
    # Event
    event = so.Field(Event, compcoef=0.429)
    # Actions in response to an event
    actions = so.Field(ActionSet, ActionSet, coerce=True, compcoef=0.86)

    def init_derived(self, schema, source, *, replace_original=None, **kwargs):
        policy = super().init_derived(schema, source, **kwargs)
        policy.subject = source

        return policy


class InternalPolicySubject(referencing.ReferencingClass):
    policy = referencing.RefDict(ref_cls=Policy, compcoef=0.857)

    def add_policy(self, policy, replace=False):
        self.add_classref('policy', policy, replace=replace)

    def del_policy(self, policy_name, schema):
        self.del_classref('policy', policy_name, schema)


class PolicySubject:
    def get_policy(self, schema, policy_cls, policy_key):
        return schema._policy_schema.get(policy_cls, policy_key)

    def materialize_policies(self, schema):
        self._merge_policies(schema, self.bases)

    def _merge_policies(self, schema, bases, force_first=False):
        seen = set()

        for base in bases:
            for event, policies in schema._policy_schema.iter(base):
                self_policies = schema._policy_schema.get(self, event)

                if (self_policies is None or
                        (force_first and (self, event) not in seen)):
                    schema._policy_schema.add(policies[-1], self)
                    seen.add((self, event))


class PolicySchema:
    def __init__(self):
        self._index = {}

    def add(self, policy, subject=None):
        if subject is None:
            subject = policy.subject

        event = policy.event

        try:
            subject_policies = self._index[subject]
        except KeyError:
            subject_policies = self._index[subject] = {}

        try:
            policy_stack = subject_policies[event]
        except KeyError:
            policy_stack = subject_policies[event] = []

        policy_stack.append(policy)

    def delete(self, policy):
        subject_policies = self._index[policy.subject]
        policy_stack = subject_policies[policy.event]
        policy_stack.remove(policy)

    def get_all(self, subject, event):
        try:
            subject_policies = self._index[subject]
        except KeyError:
            return None
        else:
            return subject_policies.get(event)

    def get(self, subject, event):
        stack = self.get_all(subject, event)

        if stack:
            return stack[-1]

    def iter(self, subject):
        try:
            subject_policies = self._index[subject]
        except KeyError:
            return ()
        else:
            return subject_policies.items()


class ActionCommandContext(sd.ClassCommandContext):
    pass


class ActionCommand(named.NamedClassCommand, schema_metaclass=Action,
                    context_class=ActionCommandContext):
    pass


class EventCommandContext(sd.ClassCommandContext):
    pass


class EventCommand(named.NamedClassCommand, schema_metaclass=Event,
                   context_class=EventCommandContext):
    pass


class PolicyCommandContext(sd.ClassCommandContext):
    pass


class InternalPolicySubjectCommandContext:
    # policy mixin
    pass


class CreateAction(named.CreateNamedClass, ActionCommand):
    astnode = qlast.CreateAction


class RenameAction(named.RenameNamedClass, ActionCommand):
    pass


class AlterAction(named.AlterNamedClass, ActionCommand):
    astnode = qlast.AlterAction


class DeleteAction(named.DeleteNamedClass, ActionCommand):
    astnode = qlast.DropAction


class CreateEvent(inheriting.CreateInheritingClass, EventCommand):
    astnode = qlast.CreateEvent


class RenameEvent(named.RenameNamedClass, EventCommand):
    pass


class RebaseEvent(inheriting.RebaseNamedClass, EventCommand):
    pass


class AlterEvent(inheriting.AlterInheritingClass, EventCommand):
    astnode = qlast.AlterEvent


class DeleteEvent(inheriting.DeleteInheritingClass, EventCommand):
    astnode = qlast.DropEvent


class PolicyCommand(
        referencing.ReferencedClassCommand,
        schema_metaclass=Policy,
        context_class=PolicyCommandContext,
        referrer_context_class=InternalPolicySubjectCommandContext):

    @classmethod
    def _classname_from_ast(cls, astnode, context, schema):
        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.classname
        event_name = sn.Name(module=astnode.event.module,
                             name=astnode.event.name)

        pnn = Policy.get_specialized_name(
            event_name, subject_name
        )

        pn = sn.Name(name=pnn, module=subject_name.module)

        return pn

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)
        if node.event is None:
            event_name = Policy.get_shortname(self.classname)
            node.event = qlast.ClassRef(
                name=event_name.name,
                module=event_name.module
            )

    def _apply_field_ast(self, context, node, op):
        if op.property == 'name':
            pass
        elif op.property == 'event':
            node.event = qlast.ClassRef(
                name=op.new_value.classname.name,
                module=op.new_value.classname.module
            )
        elif op.property == 'actions':
            node.actions = [qlast.ClassRef(
                name=a.classname.name,
                module=a.classname.module
            ) for a in op.new_value]
        else:
            pass


class CreatePolicy(PolicyCommand, named.CreateNamedClass):
    astnode = qlast.CreateLocalPolicy

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.classname

        cmd.update((
            sd.AlterClassProperty(
                property='subject',
                new_value=so.ClassRef(classname=subject_name)
            ),
            sd.AlterClassProperty(
                property='event',
                new_value=so.ClassRef(
                    classname=sn.Name(
                        module=astnode.event.module,
                        name=astnode.event.name
                    )
                )
            ),
            sd.AlterClassProperty(
                property='actions',
                new_value=so.ClassList(
                    so.ClassRef(
                        classname=sn.Name(
                            module=action.module,
                            name=action.name
                        )
                    )
                    for action in astnode.actions
                )
            )
        ))

        return cmd


class RenamePolicy(PolicyCommand, named.RenameNamedClass):
    pass


class AlterPolicy(PolicyCommand, named.AlterNamedClass):
    astnode = qlast.AlterLocalPolicy

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        cmd.update((
            sd.AlterClassProperty(
                property='actions',
                new_value=so.ClassList(
                    so.ClassRef(
                        classname=sn.Name(
                            module=action.module,
                            name=action.name
                        )
                    )
                    for action in astnode.actions
                )
            ),
        ))

        return cmd


class DeletePolicy(PolicyCommand, named.DeleteNamedClass):
    pass
