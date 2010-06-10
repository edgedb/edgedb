##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix import caos
from semantix.caos import delta, proto
from semantix.utils import datastructures, helper
from semantix.utils.lang import yaml

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
        return StructMeta.adapt_value(field, value)

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
        prototype_class = self.data.get('prototype_class')
        if prototype_class:
            self.data['prototype_class'] = helper.get_object(prototype_class)

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


class NamedPrototypeCommand(PrototypeCommand, adapts=delta.NamedPrototypeCommand):
    def construct(self):
        self.data['prototype_name'] = caos.Name(self.data['prototype_name'])
        return super().construct()


class CreatePrototype(PrototypeCommand, adapts=delta.CreatePrototype):
    pass


class CreateSimplePrototype(CreatePrototype, adapts=delta.CreateSimplePrototype):
    @classmethod
    def represent_command(cls, data):
        result = super().represent_command(data)
        result['prototype_data'] = data.prototype_data
        return result


class CreateNamedPrototype(NamedPrototypeCommand, adapts=delta.CreateNamedPrototype):
    pass


class RenameNamedPrototype(NamedPrototypeCommand, adapts=delta.RenameNamedPrototype):
    pass


class AlterNamedPrototype(NamedPrototypeCommand, adapts=delta.AlterNamedPrototype):
    pass


class AlterPrototypeProperty(Command, adapts=delta.AlterPrototypeProperty):
    @classmethod
    def represent(cls, data):
        result = {
            data.property: [data.old_value, data.new_value]
        }

        return result


class DeleteNamedPrototype(NamedPrototypeCommand, adapts=delta.DeleteNamedPrototype):
    pass


class AlterDefault(Command, adapts=delta.AlterDefault):
    def construct(self):
        adapter = yaml.ObjectMeta.get_adapter(proto.DefaultSpec)
        assert adapter, 'could not find YAML adapter for proto.DefaultSpec'

        for f in ('old_value', 'new_value'):
            if self.data[f]:
                val = []
                for spec in self.data[f]:
                    spec = adapter.resolve(spec)(None, spec)
                    spec.construct()
                    val.append(spec)
                self.data[f] = val

        super().construct()


class AtomConstraintCommand(PrototypeCommand, adapts=delta.AtomConstraintCommand):
    pass


class SourceIndexCommand(PrototypeCommand, adapts=delta.SourceIndexCommand):
    pass


class LinkConstraintCommand(PrototypeCommand, adapts=delta.LinkConstraintCommand):
    pass


class LinkSearchConfigurationCommand(PrototypeCommand, adapts=delta.LinkSearchConfigurationCommand):
    pass


class CreateAtom(CreateNamedPrototype, adapts=delta.CreateAtom):
    pass


class CreateAtomConstraint(AtomConstraintCommand, CreateSimplePrototype, adapts=delta.CreateAtomConstraint):
    pass


class AlterAtomConstraint(AtomConstraintCommand, CreateSimplePrototype, adapts=delta.AlterAtomConstraint):
    pass


class DeleteAtomConstraint(AtomConstraintCommand, adapts=delta.DeleteAtomConstraint):
    pass


class RenameAtom(RenameNamedPrototype, adapts=delta.RenameAtom):
    pass


class AlterAtom(AlterNamedPrototype, adapts=delta.AlterAtom):
    pass


class DeleteAtom(DeleteNamedPrototype, adapts=delta.DeleteAtom):
    pass


class CreateSourceIndex(SourceIndexCommand, adapts=delta.CreateSourceIndex):
    pass


class AlterSourceIndex(SourceIndexCommand, adapts=delta.AlterSourceIndex):
    pass


class DeleteSourceIndex(SourceIndexCommand, adapts=delta.DeleteSourceIndex):
    pass


class CreateConcept(CreateNamedPrototype, adapts=delta.CreateConcept):
    pass


class RenameConcept(RenameNamedPrototype, adapts=delta.RenameConcept):
    pass


class AlterConcept(AlterNamedPrototype, adapts=delta.AlterConcept):
    pass


class DeleteConcept(DeleteNamedPrototype, adapts=delta.DeleteConcept):
    pass



class CreateLinkSet(CreateNamedPrototype, adapts=delta.CreateLinkSet):
    pass


class RenameLinkSet(RenameNamedPrototype, adapts=delta.RenameLinkSet):
    pass


class AlterLinkSet(AlterNamedPrototype, adapts=delta.AlterLinkSet):
    pass


class DeleteLinkSet(DeleteNamedPrototype, adapts=delta.DeleteLinkSet):
    pass



class CreateLink(CreateNamedPrototype, adapts=delta.CreateLink):
    pass


class RenameLink(RenameNamedPrototype, adapts=delta.RenameLink):
    pass


class AlterLink(AlterNamedPrototype, adapts=delta.AlterLink):
    pass


class DeleteLink(DeleteNamedPrototype, adapts=delta.DeleteLink):
    pass


class CreateLinkConstraint(LinkConstraintCommand, CreateSimplePrototype,
                           adapts=delta.CreateLinkConstraint):
    pass


class DeleteLinkConstraint(LinkConstraintCommand, adapts=delta.DeleteLinkConstraint):
    pass


class CreateLinkProperty(CreateNamedPrototype, adapts=delta.CreateLinkProperty):
    pass


class RenameLinkProperty(RenameNamedPrototype, adapts=delta.RenameLinkProperty):
    pass


class AlterLinkProperty(AlterNamedPrototype, adapts=delta.AlterLinkProperty):
    pass


class DeleteLinkProperty(DeleteNamedPrototype, adapts=delta.DeleteLinkProperty):
    pass


class CreateLinkSearchConfiguration(LinkSearchConfigurationCommand, CreatePrototype,
                                    adapts=delta.CreateLinkSearchConfiguration):
    pass


class AlterLinkSearchConfiguration(LinkSearchConfigurationCommand,
                                   adapts=delta.AlterLinkSearchConfiguration):
    pass


class DeleteLinkSearchConfiguration(LinkSearchConfigurationCommand,
                                    adapts=delta.DeleteLinkSearchConfiguration):
    pass
