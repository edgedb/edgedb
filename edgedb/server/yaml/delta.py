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
from semantix.utils.lang.yaml.struct import MixedStructMeta


class DeltaMeta(type(yaml.Object), delta.DeltaMeta, MixedStructMeta):
    pass


class Delta(yaml.Object, adapts=delta.Delta, metaclass=DeltaMeta):
    def __sx_setstate__(self, data):
        fields = {}
        for f, fdesc in type(self)._fields.items():
            v = data.get(f)
            if v is not None:
                if f in ('id', 'parent_id', 'checksum'):
                    fields[f] = int(v, 16)
                else:
                    fields[f] = v

        datastructures.MixedStruct.__init__(self, **fields)

    @classmethod
    def __sx_getstate__(cls, data):
        result = MixedStructMeta.__sx_getstate__(cls, data)

        result['id'] = '%x' % result['id']
        if result['parent_id']:
            result['parent_id'] = '%x' % result['parent_id']
        result['checksum'] = '%x' % result['checksum']

        return result

    """
    @classmethod
    def get_yaml_validator_config(cls):
        config = MixedStructMeta.get_yaml_validator_config(cls)

        for f in ('id', 'parent_id', 'checksum'):
            config[f]['type'] = 'str'

        return config
    """


class DeltaSet(yaml.Object, adapts=delta.DeltaSet):
    def items(self):
        return (('deltas', self),)

    def __sx_setstate__(self, data):
        delta.DeltaSet.__init__(self, data.values())

    @classmethod
    def __sx_getstate__(cls, data):
        return {d.id: d for d in data.deltas}


class CommandMeta(type(yaml.Object), type(delta.Command), MixedStructMeta):
    pass


class Command(yaml.Object, adapts=delta.Command, metaclass=CommandMeta):
    def adapt_value(self, field, value):
        if isinstance(value, str) and isinstance(field.type[0], caos.types.PrototypeClass):
            value = caos.name.Name(value)
            value = proto.PrototypeRef(prototype_name=value)

        elif isinstance(value, proto.PrototypeRef) \
                                and isinstance(field.type[0], caos.types.PrototypeClass):
            pass

        else:
            value = MixedStructMeta.adapt_value(field, value)

        return value

    @classmethod
    def __sx_getstate__(cls, data):
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

    def __sx_setstate__(self, data):
        fields = {}
        for f, fdesc in type(self)._fields.items():
            v = data.get(f)
            if v is not None:
                fields[f] = self.adapt_value(fdesc, v)

        delta.Command.__init__(self, **fields)
        self.ops = datastructures.OrderedSet(data['ops'])

        if data['properties']:
            for prop in data['properties']:
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

    def __sx_setstate__(self, data):
        prototype_class = data.get('prototype_class')
        if prototype_class:
            data['prototype_class'] = helper.get_object(prototype_class)

        properties = data.get('properties')
        if properties:
            for prop in properties:
                for prop_name, prop_values in prop.items():
                    if prop_name == 'base':
                        if prop_values[1]:
                            if isinstance(prop_values[1], tuple):
                                prop_values[1] = tuple(caos.Name(n) for n in prop_values[1])
                            else:
                                prop_values[1] = caos.Name(prop_values[1])

        return super().__sx_setstate__(data)


class NamedPrototypeCommand(PrototypeCommand, adapts=delta.NamedPrototypeCommand):
    def __sx_setstate__(self, data):
        data['prototype_name'] = caos.Name(data['prototype_name'])
        return super().__sx_setstate__(data)



class CreateModule(NamedPrototypeCommand, adapts=delta.CreateModule):
    pass


class AlterModule(NamedPrototypeCommand, adapts=delta.AlterModule):
    pass


class DeleteModule(NamedPrototypeCommand, adapts=delta.DeleteModule):
    pass


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


class RebaseNamedPrototype(NamedPrototypeCommand, adapts=delta.RebaseNamedPrototype):
    pass


class AlterNamedPrototype(NamedPrototypeCommand, adapts=delta.AlterNamedPrototype):
    pass


class AlterPrototypeProperty(Command, adapts=delta.AlterPrototypeProperty):
    @classmethod
    def __sx_getstate__(cls, data):
        if isinstance(data.old_value, set):
            old_value = list(data.old_value)
        else:
            old_value = data.old_value

        if isinstance(data.new_value, set):
            new_value = list(data.new_value)
        else:
            new_value = data.new_value

        result = {
            data.property: [old_value, new_value]
        }

        return result


class DeleteNamedPrototype(NamedPrototypeCommand, adapts=delta.DeleteNamedPrototype):
    pass


class AlterDefault(Command, adapts=delta.AlterDefault):
    def __sx_setstate__(self, data):
        adapter = yaml.ObjectMeta.get_adapter(proto.DefaultSpec)
        assert adapter, 'could not find YAML adapter for proto.DefaultSpec'

        for f in ('old_value', 'new_value'):
            if data[f]:
                val = []
                for spec in data[f]:
                    d = spec
                    spec = adapter.resolve(spec)(spec)
                    spec.__sx_setstate__(d)
                    val.append(spec)
                data[f] = val

        super().__sx_setstate__(data)


class AtomConstraintCommand(PrototypeCommand, adapts=delta.AtomConstraintCommand):
    pass


class SourceIndexCommand(PrototypeCommand, adapts=delta.SourceIndexCommand):
    pass


class PointerConstraintCommand(PrototypeCommand, adapts=delta.PointerConstraintCommand):
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


class RebaseAtom(RebaseNamedPrototype, adapts=delta.RebaseAtom):
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


class RebaseConcept(RebaseNamedPrototype, adapts=delta.RebaseConcept):
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


class RebaseLink(RebaseNamedPrototype, adapts=delta.RebaseLink):
    pass


class AlterLink(AlterNamedPrototype, adapts=delta.AlterLink):
    pass


class DeleteLink(DeleteNamedPrototype, adapts=delta.DeleteLink):
    pass


class CreatePointerConstraint(PointerConstraintCommand, CreateSimplePrototype,
                           adapts=delta.CreatePointerConstraint):
    pass


class DeletePointerConstraint(PointerConstraintCommand, adapts=delta.DeletePointerConstraint):
    pass


class CreateLinkProperty(CreateNamedPrototype, adapts=delta.CreateLinkProperty):
    pass


class RenameLinkProperty(RenameNamedPrototype, adapts=delta.RenameLinkProperty):
    pass


class RebaseLinkProperty(RebaseNamedPrototype, adapts=delta.RebaseLinkProperty):
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


class CreateComputable(CreateNamedPrototype, adapts=delta.CreateComputable):
    pass


class RenameComputable(RenameNamedPrototype, adapts=delta.RenameComputable):
    pass


class AlterComputable(AlterNamedPrototype, adapts=delta.AlterComputable):
    pass


class DeleteComputable(DeleteNamedPrototype, adapts=delta.DeleteComputable):
    pass
