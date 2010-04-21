##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib

from semantix import caos
from semantix.caos import delta
from semantix.utils import datastructures
from semantix.utils.lang import yaml
from semantix.utils.algos import persistent_hash

from .common import StructMeta


class DeltaMeta(type(yaml.Object), delta.DeltaMeta, StructMeta):
    pass


class Delta(yaml.Object, adapts=delta.Delta, metaclass=DeltaMeta):
    def construct(self):
        fields = {}
        for f, fdesc in type(self)._fields.items():
            v = self.data.get(f)
            if v is not None:
                if f in ('id', 'parent_id', 'checksum'):
                    fields[f] = int(v, 16)
                else:
                    fields[f] = v

        datastructures.Struct.__init__(self, **fields)

    @classmethod
    def represent(cls, data):
        result = StructMeta.represent(cls, data)

        result['id'] = '%x' % result['id']
        if result['parent_id']:
            result['parent_id'] = '%x' % result['parent_id']
        result['checksum'] = '%x' % result['checksum']

        return result

    """
    @classmethod
    def get_yaml_validator_config(cls):
        config = StructMeta.get_yaml_validator_config(cls)

        for f in ('id', 'parent_id', 'checksum'):
            config[f]['type'] = 'str'

        return config
    """


class DeltaSet(yaml.Object, adapts=delta.DeltaSet):
    def items(self):
        return (('deltas', self),)

    def construct(self):
        delta.DeltaSet.__init__(self, self.data.values())

    @classmethod
    def represent(cls, data):
        return {d.id: d for d in data.deltas}


class CommandMeta(type(yaml.Object), type(delta.Command), StructMeta):
    pass


class Command(yaml.Object, adapts=delta.Command, metaclass=CommandMeta):
    def adapt_value(self, field, value):
        if not isinstance(value, field.type):
            adapter = yaml.ObjectMeta.get_adapter(field.type[0])
            if adapter:
                value = adapter(None, value)
                constructor = getattr(value, 'construct', None)
                if constructor:
                    constructor()
            else:
                value = field.adapt(value)
        return value

    @classmethod
    def represent(cls, data):
        key = '%s.%s' % (cls.__module__, cls.__name__)
        return {key: cls.represent_command(data)}

    @classmethod
    def represent_command(cls, data):
        result = {}
        if data.ops:
            for op in data:
                if isinstance(op, delta.AlterPrototypeProperty):
                    if 'properties' not in result:
                        result['properties'] = []
                    result['properties'].append(op)
                else:
                    if 'ops' not in result:
                        result['ops'] = []
                    result['ops'].append(op)

        for f in type(data)._fields:
            result[f] = getattr(data, f)
        return result

    def construct(self):
        fields = {}
        for f, fdesc in type(self)._fields.items():
            v = self.data.get(f)
            if v is not None:
                fields[f] = self.adapt_value(fdesc, v)

        delta.Command.__init__(self, **fields)
        self.ops = datastructures.OrderedSet(self.data['ops'])

        if self.data['properties']:
            for prop in self.data['properties']:
                for prop_name, (old_value, new_value) in prop.items():
                    field = self.prototype_class._fields.get(prop_name)
                    if field:
                        if old_value is not None:
                            old_value = self.adapt_value(field, old_value)
                        if new_value is not None:
                            new_value = self.adapt_value(field, new_value)

                    self.ops.add(delta.AlterPrototypeProperty(property=prop_name,
                                                              old_value=old_value,
                                                              new_value=new_value))


class AlterRealm(Command, adapts=delta.AlterRealm):
    pass


class PrototypeCommand(Command, adapts=delta.PrototypeCommand):
    @classmethod
    def represent_command(cls, data):
        result = super().represent_command(data)
        if hasattr(data, 'prototype_class'):
            result['prototype_class'] = '%s.%s' % (data.prototype_class.__module__,
                                                   data.prototype_class.__name__)
        return result

    def construct(self):
        self.data['prototype_name'] = caos.Name(self.data['prototype_name'])

        if 'prototype_class' in self.data:
            module, _, name = self.data['prototype_class'].rpartition('.')
            self.data['prototype_class'] = getattr(importlib.import_module(module), name)

        properties = self.data.get('properties')
        if properties:
            for prop in properties:
                for prop_name, prop_values in prop.items():
                    if prop_name == 'base':
                        if prop_values[1]:
                            if isinstance(prop_values[1], tuple):
                                prop_values[1] = tuple(caos.Name(n) for n in prop_values[1])
                            else:
                                prop_values[1] = caos.Name(prop_values[1])

        return super().construct()


class CreatePrototype(PrototypeCommand, adapts=delta.CreatePrototype):
    pass


class RenamePrototype(Command, adapts=delta.RenamePrototype):
    pass


class AlterPrototype(PrototypeCommand, adapts=delta.AlterPrototype):
    pass


class AlterPrototypeProperty(Command, adapts=delta.AlterPrototypeProperty):
    @classmethod
    def represent(cls, data):
        result = {
            data.property: [data.old_value, data.new_value]
        }

        return result


class DeletePrototype(Command, adapts=delta.DeletePrototype):
    pass


class AtomModCommand(Command, adapts=delta.AtomModCommand):
    @classmethod
    def represent_command(cls, data):
        result = super().represent_command(data)
        result['mod_class'] = '%s.%s' % (data.mod_class.__module__,
                                         data.mod_class.__name__)
        return result

    def construct(self):
        module, _, name = self.data['mod_class'].rpartition('.')
        self.data['mod_class'] = getattr(importlib.import_module(module), name)
        super().construct()


class LinkConstraintCommand(Command, adapts=delta.LinkConstraintCommand):
    @classmethod
    def represent_command(cls, data):
        result = super().represent_command(data)
        result['constraint_class'] = '%s.%s' % (data.constraint_class.__module__,
                                                data.constraint_class.__name__)
        return result

    def construct(self):
        module, _, name = self.data['constraint_class'].rpartition('.')
        self.data['constraint_class'] = getattr(importlib.import_module(module), name)
        super().construct()


class CreateAtom(CreatePrototype, adapts=delta.CreateAtom):
    pass


class CreateAtomMod(AtomModCommand, adapts=delta.CreateAtomMod):
    @classmethod
    def represent_command(cls, data):
        result = super().represent_command(data)
        result['mod_data'] = data.mod_data
        return result


class RenameAtom(RenamePrototype, adapts=delta.RenameAtom):
    pass


class AlterAtom(AlterPrototype, adapts=delta.AlterAtom):
    pass


class DeleteAtom(DeletePrototype, adapts=delta.DeleteAtom):
    pass


class DeleteAtomMod(AtomModCommand, adapts=delta.DeleteAtomMod):
    pass


class CreateConcept(CreatePrototype, adapts=delta.CreateConcept):
    pass


class RenameConcept(RenamePrototype, adapts=delta.RenameConcept):
    pass


class AlterConcept(AlterPrototype, adapts=delta.AlterConcept):
    pass


class DeleteConcept(DeletePrototype, adapts=delta.DeleteConcept):
    pass



class CreateLinkSet(CreatePrototype, adapts=delta.CreateLinkSet):
    pass


class RenameLinkSet(RenamePrototype, adapts=delta.RenameLinkSet):
    pass


class AlterLinkSet(AlterPrototype, adapts=delta.AlterLinkSet):
    pass


class DeleteLinkSet(DeletePrototype, adapts=delta.DeleteLinkSet):
    pass



class CreateLink(CreatePrototype, adapts=delta.CreateLink):
    pass


class RenameLink(RenamePrototype, adapts=delta.RenameLink):
    pass


class AlterLink(AlterPrototype, adapts=delta.AlterLink):
    pass


class DeleteLink(DeletePrototype, adapts=delta.DeleteLink):
    pass


class CreateLinkConstraint(LinkConstraintCommand, adapts=delta.CreateLinkConstraint):
    @classmethod
    def represent_command(cls, data):
        result = super().represent_command(data)
        result['constraint_data'] = data.constraint_data
        return result


class DeleteLinkConstraint(LinkConstraintCommand, adapts=delta.DeleteLinkConstraint):
    pass


class CreateLinkProperty(CreatePrototype, adapts=delta.CreateLinkProperty):
    pass


class RenameLinkProperty(RenamePrototype, adapts=delta.RenameLinkProperty):
    pass


class AlterLinkProperty(AlterPrototype, adapts=delta.AlterLinkProperty):
    pass


class DeleteLinkProperty(DeletePrototype, adapts=delta.DeleteLinkProperty):
    pass
