##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections.abc

from importkit import yaml
from importkit.import_ import get_object, ObjectImportError
from importkit import context as lang_context

from metamagic import caos
from metamagic.caos import types as caos_types
from metamagic.caos import delta, proto
from metamagic.utils import datastructures
from metamagic.utils.datastructures import typed
from metamagic.utils.lang.yaml.struct import MixedStructMeta


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
        return {int(d.id): d for d in data.deltas}


class CommandMeta(type(yaml.Object), type(delta.Command), MixedStructMeta):
    pass


class Command(yaml.Object, adapts=delta.Command, metaclass=CommandMeta):
    def adapt_value(self, field, value):
        FieldType = field.type[0]
        Name = caos.name.Name

        if (isinstance(value, str)
                and isinstance(FieldType, caos.types.PrototypeClass)
                and hasattr(FieldType, 'name')):
            value = proto.PrototypeRef(prototype_name=Name(value))

        elif isinstance(value, proto.PrototypeRef) \
                and isinstance(FieldType, caos.types.PrototypeClass):
            pass

        elif issubclass(FieldType, typed.AbstractTypedMapping) \
                and issubclass(FieldType.valuetype,
                               (caos.proto.PrototypeOrNativeClass,
                                caos.types.ProtoObject)):

            ElementType = FieldType.valuetype
            RefType = ElementType.ref_type

            vals = {}
            for k, v in value.items():
                if isinstance(v, str):
                    v = RefType(prototype_name=Name(v))
                elif (isinstance(v, proto.PrototypeRef)
                            and not isinstance(v, RefType)):
                    v = RefType(prototype_name=Name(v.prototype_name))
                vals[k] = v

            value = FieldType(vals)

        elif (issubclass(FieldType, (typed.AbstractTypedSequence,
                                    typed.AbstractTypedSet))
                and issubclass(FieldType.type,
                               (caos.proto.PrototypeOrNativeClass,
                                caos.types.ProtoObject))):

            ElementType = field.type[0].type
            RefType = ElementType.ref_type

            is_named = issubclass(ElementType, caos.proto.NamedPrototype)

            vals = []
            for v in value:
                if isinstance(v, str) and is_named:
                    v = RefType(prototype_name=Name(v))
                elif (isinstance(v, proto.PrototypeRef)
                            and not isinstance(v, RefType)):
                    v = RefType(prototype_name=Name(v.prototype_name))
                vals.append(v)

            value = FieldType(vals)

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

        # compat_mode is set when deltas are loaded in repo upgrade context.
        # In this case, we need to relax struct validation requirements
        context = lang_context.SourceContext.from_object(self)
        icontext = context.document.import_context
        compat_mode = getattr(icontext, 'compat_mode', False)

        delta.Command.__init__(self, _relaxrequired_=compat_mode, **fields)
        self.ops = datastructures.OrderedSet(data['ops'])

        if data['properties']:
            for prop in data['properties']:
                for prop_name, (old_value, new_value) in prop.items():
                    # Backwards compat
                    if prop_name == 'base':
                        prop_name = 'bases'

                    field = self.prototype_class._fields.get(prop_name)
                    if field:
                        if prop_name == 'bases':
                            if new_value is not None and not isinstance(new_value, (list, tuple)):
                                new_value = [new_value]

                            if old_value is not None and not isinstance(old_value, (list, tuple)):
                                old_value = [old_value]

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
            data['prototype_class'] = get_object(prototype_class)

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


class Ghost(PrototypeCommand, adapts=delta.Ghost):
    def __sx_setstate__(self, data):
        prototype_class = data.get('prototype_class')
        if prototype_class:
            try:
                get_object(prototype_class)
            except ObjectImportError:
                data['prototype_class'] = 'metamagic.caos.proto.BasePrototype'

        return super().__sx_setstate__(data)

    @classmethod
    def get_yaml_validator_config(cls):
        return {'=': {'type': 'any'}}


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
    @staticmethod
    def _is_derived_ref(value):
        return (isinstance(value, caos.proto.PrototypeRef) and
                type(value) is not caos.proto.PrototypeRef)

    @classmethod
    def _reduce_refs(cls, value):
        if isinstance(value, (typed.AbstractTypedSequence,
                              typed.AbstractTypedSet)):
            result = []
            for item in value:
                if cls._is_derived_ref(item):
                    item = caos.proto.PrototypeRef(
                                prototype_name=item.prototype_name)
                result.append(item)

        elif isinstance(value, typed.AbstractTypedMapping):
            result = {}
            for key, item in value.items():
                if cls._is_derived_ref(item):
                    item = caos.proto.PrototypeRef(
                                prototype_name=item.prototype_name)
                result[key] = item

        elif cls._is_derived_ref(value):
            result = caos.proto.PrototypeRef(
                            prototype_name=value.prototype_name)

        else:
            result = value

        return result

    @classmethod
    def _normalize_value(cls, value):
        # Make sure derived object refs do not leak out.
        # This is possible when a delta is unserialized and then
        # serialized back.
        value = cls._reduce_refs(value)

        return value

    @classmethod
    def __sx_getstate__(cls, data):
        old_value = cls._normalize_value(data.old_value)
        new_value = cls._normalize_value(data.new_value)

        return {data.property: [old_value, new_value]}


class DeleteNamedPrototype(NamedPrototypeCommand, adapts=delta.DeleteNamedPrototype):
    pass


# NOTE: Deprecated. For delta compatibility.
class AlterDefault(Command, adapts=delta.AlterDefault):
    def __sx_setstate__(self, data):
        for f in ('old_value', 'new_value'):
            if data[f]:
                val = proto.DefaultSpecList()
                for spec in data[f]:
                    if isinstance(spec, dict):
                        spec = caos_types.ExpressionText(spec['query'])
                    val.append(spec)
                data[f] = val

        super().__sx_setstate__(data)


class AttributeCommand(PrototypeCommand, adapts=delta.AttributeCommand):
    pass


class AttributeValueCommand(PrototypeCommand, adapts=delta.AttributeValueCommand):
    pass


class ConstraintCommand(PrototypeCommand, adapts=delta.ConstraintCommand):
    pass


class SourceIndexCommand(PrototypeCommand, adapts=delta.SourceIndexCommand):
    pass


class LinkSearchConfigurationCommand(PrototypeCommand, adapts=delta.LinkSearchConfigurationCommand):
    pass


class CreateAttribute(AttributeCommand, adapts=delta.CreateAttribute):
    pass


class RenameAttribute(AttributeCommand, adapts=delta.RenameAttribute):
    pass


class AlterAttribute(AttributeCommand, adapts=delta.AlterAttribute):
    pass


class DeleteAttribute(AttributeCommand, adapts=delta.DeleteAttribute):
    pass


class CreateAttributeValue(AttributeValueCommand, adapts=delta.CreateAttributeValue):
    pass


class RenameAttributeValue(AttributeValueCommand, adapts=delta.RenameAttributeValue):
    pass


class AlterAttributeValue(AttributeValueCommand, adapts=delta.AlterAttributeValue):
    pass


class DeleteAttributeValue(AttributeValueCommand, adapts=delta.DeleteAttributeValue):
    pass


class CreateConstraint(ConstraintCommand, adapts=delta.CreateConstraint):
    pass


class RenameConstraint(ConstraintCommand, adapts=delta.RenameConstraint):
    pass


class AlterConstraint(ConstraintCommand, adapts=delta.AlterConstraint):
    pass


class DeleteConstraint(ConstraintCommand, adapts=delta.DeleteConstraint):
    pass


class CreateAtom(CreateNamedPrototype, adapts=delta.CreateAtom):
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


class RenameSourceIndex(SourceIndexCommand, adapts=delta.RenameSourceIndex):
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


class CreateAction(CreateNamedPrototype, adapts=delta.CreateAction):
    pass


class RenameAction(RenameNamedPrototype, adapts=delta.RenameAction):
    pass


class AlterAction(AlterNamedPrototype, adapts=delta.AlterAction):
    pass


class DeleteAction(DeleteNamedPrototype, adapts=delta.DeleteAction):
    pass


class CreateEvent(CreateNamedPrototype, adapts=delta.CreateEvent):
    pass


class RenameEvent(RenameNamedPrototype, adapts=delta.RenameEvent):
    pass


class RebaseEvent(RebaseNamedPrototype, adapts=delta.RebaseEvent):
    pass


class AlterEvent(AlterNamedPrototype, adapts=delta.AlterEvent):
    pass


class DeleteEvent(DeleteNamedPrototype, adapts=delta.DeleteEvent):
    pass


class CreatePolicy(CreateNamedPrototype, adapts=delta.CreatePolicy):
    pass


class RenamePolicy(RenameNamedPrototype, adapts=delta.RenamePolicy):
    pass


class AlterPolicy(AlterNamedPrototype, adapts=delta.AlterPolicy):
    pass


class DeletePolicy(DeleteNamedPrototype, adapts=delta.DeletePolicy):
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


class CreateLinkSearchConfiguration(LinkSearchConfigurationCommand,
                                    adapts=delta.CreateLinkSearchConfiguration):
    pass


class AlterLinkSearchConfiguration(LinkSearchConfigurationCommand,
                                   adapts=delta.AlterLinkSearchConfiguration):
    pass


class DeleteLinkSearchConfiguration(LinkSearchConfigurationCommand,
                                    adapts=delta.DeleteLinkSearchConfiguration):
    pass
