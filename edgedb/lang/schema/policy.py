##
# Copyright (c) 2008-2016 MagicStack Inc.
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


class ActionCommandContext(sd.PrototypeCommandContext):
    pass


class ActionCommand:
    context_class = ActionCommandContext

    @classmethod
    def _get_prototype_class(cls):
        return Action


class EventCommandContext(sd.PrototypeCommandContext):
    pass


class EventCommand:
    context_class = EventCommandContext

    @classmethod
    def _get_prototype_class(cls):
        return Event


class PolicyCommandContext(sd.PrototypeCommandContext):
    pass


class InternalPolicySubjectCommandContext:
    # policy mixin
    pass


class CreateAction(named.CreateNamedPrototype, ActionCommand):
    astnode = qlast.CreateActionNode


class RenameAction(named.RenameNamedPrototype, ActionCommand):
    pass


class AlterAction(named.AlterNamedPrototype, ActionCommand):
    astnode = qlast.AlterActionNode


class DeleteAction(named.DeleteNamedPrototype, ActionCommand):
    astnode = qlast.DropActionNode


class CreateEvent(named.CreateNamedPrototype, EventCommand):
    astnode = qlast.CreateEventNode

    @classmethod
    def _protobases_from_ast(cls, astnode, context):
        bases = super()._protobases_from_ast(astnode, context)
        if not bases:
            name = '{}::{}'.format(astnode.name.module, astnode.name.name)
            if name != 'std::event':
                bases = so.PrototypeList([
                    so.PrototypeRef(
                        prototype_name=sn.Name(
                            module='std',
                            name='event'
                        )
                    )
                ])

        return bases


class RenameEvent(named.RenameNamedPrototype, EventCommand):
    pass


class RebaseEvent(inheriting.RebaseNamedPrototype, EventCommand):
    pass


class AlterEvent(named.AlterNamedPrototype, EventCommand):
    astnode = qlast.AlterEventNode


class DeleteEvent(named.DeleteNamedPrototype, EventCommand):
    astnode = qlast.DropEventNode


class PolicyCommand(sd.PrototypeCommand):
    context_class = PolicyCommandContext

    @classmethod
    def _get_prototype_class(cls):
        return Policy

    @classmethod
    def _protoname_from_ast(cls, astnode, context):
        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.prototype_name
        event_name = sn.Name(module=astnode.event.module,
                             name=astnode.event.name)

        pnn = Policy.generate_specialized_name(
            subject_name, event_name
        )

        pn = sn.Name(name=pnn, module=subject_name.module)

        return pn

    def _apply_fields_ast(self, context, node):
        super()._apply_fields_ast(context, node)
        if node.event is None:
            event_name = Policy.normalize_name(self.prototype_name)
            node.event = qlast.PrototypeRefNode(
                name=event_name.name,
                module=event_name.module
            )

    def _apply_field_ast(self, context, node, op):
        if op.property == 'name':
            pass
        elif op.property == 'event':
            node.event = qlast.PrototypeRefNode(
                name=op.new_value.prototype_name.name,
                module=op.new_value.prototype_name.module
            )
        elif op.property == 'actions':
            node.actions = [qlast.PrototypeRefNode(
                name=a.prototype_name.name,
                module=a.prototype_name.module
            ) for a in op.new_value]
        else:
            pass


class CreatePolicy(PolicyCommand, named.CreateNamedPrototype):
    astnode = qlast.CreateLocalPolicyNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        cmd = super()._cmd_tree_from_ast(astnode, context)

        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.prototype_name

        cmd.update((
            sd.AlterPrototypeProperty(
                property='subject',
                new_value=so.PrototypeRef(prototype_name=subject_name)
            ),
            sd.AlterPrototypeProperty(
                property='event',
                new_value=so.PrototypeRef(
                    prototype_name=sn.Name(
                        module=astnode.event.module,
                        name=astnode.event.name
                    )
                )
            ),
            sd.AlterPrototypeProperty(
                property='actions',
                new_value=so.PrototypeList(
                    so.PrototypeRef(
                        prototype_name=sn.Name(
                            module=action.module,
                            name=action.name
                        )
                    )
                    for action in astnode.actions
                )
            )
        ))

        return cmd

    def apply(self, protoschema, context):
        context = context or sd.CommandContext()

        policy = named.CreateNamedPrototype.apply(self, protoschema, context)

        subject_ctx = context.get(InternalPolicySubjectCommandContext)
        msg = "Policy commands must be run in subject context"
        assert subject_ctx, msg
        policy.subject = subject_ctx.proto
        subject_ctx.proto.add_policy(policy)

        return policy


class RenamePolicy(PolicyCommand, named.RenameNamedPrototype):
    def apply(self, schema, context):
        policy = super().apply(schema, context)

        subject_ctx = context.get(InternalPolicySubjectCommandContext)
        msg = "Policy commands must be run in subject context"
        assert subject_ctx, msg

        subject = subject_ctx.proto

        norm = Policy.normalize_name
        cur_name = norm(self.prototype_name)
        new_name = norm(self.new_name)

        local = subject.local_policy.pop(cur_name, None)
        if local:
            subject.local_policy[new_name] = local

        inherited = subject.policy.pop(cur_name, None)
        if inherited is not None:
            subject.policy[new_name] = inherited

        return policy


class AlterPolicy(PolicyCommand, named.AlterNamedPrototype):
    astnode = qlast.AlterLocalPolicyNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        cmd = super()._cmd_tree_from_ast(astnode, context)

        cmd.update((
            sd.AlterPrototypeProperty(
                property='actions',
                new_value=so.PrototypeList(
                    so.PrototypeRef(
                        prototype_name=sn.Name(
                            module=action.module,
                            name=action.name
                        )
                    )
                    for action in astnode.actions
                )
            ),
        ))

        return cmd

    def apply(self, schema, context):
        subject_ctx = context.get(InternalPolicySubjectCommandContext)
        msg = "Policy commands must be run in subject context"
        assert subject_ctx, msg

        with context(PolicyCommandContext(self, None)):
            return super().apply(schema, context)


class DeletePolicy(PolicyCommand, named.DeleteNamedPrototype):
    def apply(self, protoschema, context):
        subject_ctx = context.get(InternalPolicySubjectCommandContext)
        msg = "Policy commands must be run in subject context"
        assert subject_ctx, msg
        subject = subject_ctx.proto
        subject.delete_policy(self.prototype_name, subject, protoschema)
        return super().apply(protoschema, context)


class Action(primary.Prototype):
    _type = 'action'

    delta_driver = sd.DeltaDriver(
        create=CreateAction,
        alter=AlterAction,
        rename=RenameAction,
        delete=DeleteAction
    )


class ActionSet(so.PrototypeSet, type=Action):
    pass


class Event(primary.Prototype):
    _type = 'event'

    delta_driver = sd.DeltaDriver(
        create=CreateEvent,
        alter=AlterEvent,
        rename=RenameEvent,
        delete=DeleteEvent
    )


class Policy(derivable.DerivablePrototype):
    _type = 'policy'

    delta_driver = sd.DeltaDriver(
        create=CreatePolicy,
        alter=AlterPolicy,
        rename=RenamePolicy,
        delete=DeletePolicy
    )

    # Policy subject, i.e object in the schema to which
    # this policy is applied
    subject = so.Field(named.NamedPrototype, compcoef=0.714)
    # Event
    event = so.Field(Event, compcoef=0.429)
    # Actions in response to an event
    actions = so.Field(ActionSet, ActionSet, coerce=True, compcoef=0.86)

    def init_derived(self, schema, source, *, replace_original=None,
                                              **kwargs):

        policy = super().init_derived(schema, source, **kwargs)
        policy.subject = source

        return policy


class InternalPolicySubject(referencing.ReferencingPrototype):
    policy = referencing.RefDict(ref_cls=Policy, compcoef=0.857)

    def add_policy(self, policy, replace=False):
        self.add_protoref('policy', policy, replace=replace)

    def del_policy(self, policy_name, protoschema):
        self.del_protoref('policy', policy_name, protoschema)


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
