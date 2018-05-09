#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from edgedb.lang.edgeql import ast as qlast

from . import delta as sd
from . import derivable
from . import inheriting
from . import name as sn
from . import named
from . import objects as so
from . import referencing


class Action(inheriting.InheritingObject):
    _type = 'action'


class ActionSet(so.ObjectSet, type=Action):
    pass


class Event(inheriting.InheritingObject):
    _type = 'event'


class Policy(derivable.DerivableObject):
    _type = 'policy'

    # Policy subject, i.e object in the schema to which
    # this policy is applied
    subject = so.Field(named.NamedObject, compcoef=0.714)
    # Event
    event = so.Field(Event, compcoef=0.429)
    # Actions in response to an event
    actions = so.Field(ActionSet, ActionSet, coerce=True, compcoef=0.86)

    def init_derived(self, schema, source, *, replace_original=None, **kwargs):
        policy = super().init_derived(schema, source, **kwargs)
        policy.subject = source

        return policy


class InternalPolicySubject(referencing.ReferencingObject):
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


class ActionCommandContext(sd.ObjectCommandContext):
    pass


class ActionCommand(named.NamedObjectCommand, schema_metaclass=Action,
                    context_class=ActionCommandContext):
    pass


class EventCommandContext(sd.ObjectCommandContext):
    pass


class EventCommand(named.NamedObjectCommand, schema_metaclass=Event,
                   context_class=EventCommandContext):
    pass


class PolicyCommandContext(sd.ObjectCommandContext):
    pass


class InternalPolicySubjectCommandContext:
    # policy mixin
    pass


class CreateAction(named.CreateNamedObject, ActionCommand):
    astnode = qlast.CreateAction


class RenameAction(named.RenameNamedObject, ActionCommand):
    pass


class AlterAction(named.AlterNamedObject, ActionCommand):
    astnode = qlast.AlterAction


class DeleteAction(named.DeleteNamedObject, ActionCommand):
    astnode = qlast.DropAction


class CreateEvent(inheriting.CreateInheritingObject, EventCommand):
    astnode = qlast.CreateEvent


class RenameEvent(named.RenameNamedObject, EventCommand):
    pass


class RebaseEvent(inheriting.RebaseNamedObject, EventCommand):
    pass


class AlterEvent(inheriting.AlterInheritingObject, EventCommand):
    astnode = qlast.AlterEvent


class DeleteEvent(inheriting.DeleteInheritingObject, EventCommand):
    astnode = qlast.DropEvent


class PolicyCommand(
        referencing.ReferencedObjectCommand,
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
            node.event = qlast.ObjectRef(
                name=event_name.name,
                module=event_name.module
            )

    def _apply_field_ast(self, context, node, op):
        if op.property == 'name':
            pass
        elif op.property == 'event':
            node.event = qlast.ObjectRef(
                name=op.new_value.classname.name,
                module=op.new_value.classname.module
            )
        elif op.property == 'actions':
            node.actions = [qlast.ObjectRef(
                name=a.classname.name,
                module=a.classname.module
            ) for a in op.new_value]
        else:
            pass


class CreatePolicy(PolicyCommand, named.CreateNamedObject):
    astnode = qlast.CreateLocalPolicy

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        parent_ctx = context.get(sd.CommandContextToken)
        subject_name = parent_ctx.op.classname

        cmd.update((
            sd.AlterObjectProperty(
                property='subject',
                new_value=so.ObjectRef(classname=subject_name)
            ),
            sd.AlterObjectProperty(
                property='event',
                new_value=so.ObjectRef(
                    classname=sn.Name(
                        module=astnode.event.module,
                        name=astnode.event.name
                    )
                )
            ),
            sd.AlterObjectProperty(
                property='actions',
                new_value=so.ObjectList(
                    so.ObjectRef(
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


class RenamePolicy(PolicyCommand, named.RenameNamedObject):
    pass


class AlterPolicy(PolicyCommand, named.AlterNamedObject):
    astnode = qlast.AlterLocalPolicy

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = super()._cmd_tree_from_ast(astnode, context, schema)

        cmd.update((
            sd.AlterObjectProperty(
                property='actions',
                new_value=so.ObjectList(
                    so.ObjectRef(
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


class DeletePolicy(PolicyCommand, named.DeleteNamedObject):
    pass
