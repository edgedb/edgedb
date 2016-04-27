##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from importkit import yaml
from importkit.import_ import get_object, ObjectImportError
from importkit import context as lang_context

from metamagic.caos import caosql

from metamagic.caos.schema import atoms as s_atoms
from metamagic.caos.schema import attributes as s_attrs
from metamagic.caos.schema import concepts as s_concepts
from metamagic.caos.schema import constraints as s_constr
from metamagic.caos.schema import delta as sd
from metamagic.caos.schema import indexes as s_indexes
from metamagic.caos.schema import inheriting as s_inheriting
from metamagic.caos.schema import links as s_links
from metamagic.caos.schema import lproperties as s_lprops
from metamagic.caos.schema import modules as s_mod
from metamagic.caos.schema import name as sn
from metamagic.caos.schema import named as s_named
from metamagic.caos.schema import objects as s_obj
from metamagic.caos.schema import policy as s_policy
from metamagic.caos.schema import realm as s_realm

from metamagic.utils import datastructures
from metamagic.utils.datastructures import typed
from metamagic.utils.lang.yaml.struct import MixedStructMeta


class DeltaMeta(type(yaml.Object), sd.DeltaMeta, MixedStructMeta):
    pass


class Delta(yaml.Object, adapts=sd.Delta, metaclass=DeltaMeta):
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
        from metamagic.caos.backends import yaml as yaml_objs

        result = MixedStructMeta.__sx_getstate__(cls, data)

        result['id'] = '%x' % result['id']
        if result['parent_id']:
            result['parent_id'] = '%x' % result['parent_id']
        result['checksum'] = '%x' % result['checksum']

        result['script'] = yaml_objs.ExpressionText('\n\n'.join(
            cls.write_command(cmd) for cmd in data.deltas[0].ops
        ))
        del result['deltas']

        return result

    @classmethod
    def write_command(cls, cmd):
        qltree = cmd.get_ast()
        if qltree is not None:
            return caosql.generate_source(caosql.optimize(qltree))
        else:
            return ''


class DeltaSet(yaml.Object, adapts=sd.DeltaSet):
    def items(self):
        return (('deltas', self),)

    def __sx_setstate__(self, data):
        sd.DeltaSet.__init__(self, data.values())

    @classmethod
    def __sx_getstate__(cls, data):
        return {int(d.id): d for d in data.deltas}


class CommandMeta(type(yaml.Object), type(sd.Command), MixedStructMeta):
    pass


class Command(yaml.Object, adapts=sd.Command, metaclass=CommandMeta):
    def adapt_value(self, field, value):
        FieldType = field.type[0]
        Name = sn.Name

        if (isinstance(value, str)
                and isinstance(FieldType, s_obj.PrototypeClass)
                and hasattr(FieldType, 'name')):
            value = s_obj.PrototypeRef(prototype_name=Name(value))

        elif (isinstance(value, s_obj.PrototypeRef)
                and isinstance(FieldType, s_obj.PrototypeClass)):
            pass

        elif (issubclass(FieldType, typed.AbstractTypedMapping)
                and issubclass(FieldType.valuetype, s_obj.ProtoObject)):

            ElementType = FieldType.valuetype
            RefType = ElementType.ref_type

            vals = {}
            for k, v in value.items():
                if isinstance(v, str):
                    v = RefType(prototype_name=Name(v))
                elif (isinstance(v, s_obj.PrototypeRef)
                            and not isinstance(v, RefType)):
                    v = RefType(prototype_name=Name(v.prototype_name))
                vals[k] = v

            value = FieldType(vals)

        elif (issubclass(FieldType, (typed.AbstractTypedSequence,
                                    typed.AbstractTypedSet))
                and issubclass(FieldType.type, s_obj.ProtoObject)):

            ElementType = field.type[0].type
            RefType = ElementType.ref_type

            is_named = issubclass(ElementType, s_named.NamedPrototype)

            vals = []
            for v in value:
                if isinstance(v, str) and is_named:
                    v = RefType(prototype_name=Name(v))
                elif (isinstance(v, s_obj.PrototypeRef)
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
                if isinstance(op, sd.AlterPrototypeProperty):
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

        sd.Command.__init__(self, _relaxrequired_=compat_mode, **fields)
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

                    self.ops.add(sd.AlterPrototypeProperty(property=prop_name,
                                                              old_value=old_value,
                                                              new_value=new_value))


class AlterRealm(Command, adapts=s_realm.AlterRealm):
    pass


class PrototypeCommand(Command, adapts=sd.PrototypeCommand):
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
                                prop_values[1] = tuple(sn.Name(n) for n in prop_values[1])
                            else:
                                prop_values[1] = sn.Name(prop_values[1])

        return super().__sx_setstate__(data)


class Ghost(PrototypeCommand, adapts=sd.Ghost):
    def __sx_setstate__(self, data):
        prototype_class = data.get('prototype_class')
        if prototype_class:
            try:
                get_object(prototype_class)
            except ObjectImportError:
                data['prototype_class'] = 'metamagic.caos.schema.objects.BasePrototype'

        return super().__sx_setstate__(data)

    @classmethod
    def get_yaml_validator_config(cls):
        return {'=': {'type': 'any'}}


class NamedPrototypeCommand(PrototypeCommand, adapts=s_named.NamedPrototypeCommand):
    def __sx_setstate__(self, data):
        data['prototype_name'] = sn.Name(data['prototype_name'])
        return super().__sx_setstate__(data)



class CreateModule(NamedPrototypeCommand, adapts=s_mod.CreateModule):
    pass


class AlterModule(NamedPrototypeCommand, adapts=s_mod.AlterModule):
    pass


class DeleteModule(NamedPrototypeCommand, adapts=s_mod.DeleteModule):
    pass


class CreatePrototype(PrototypeCommand, adapts=sd.CreatePrototype):
    pass


class CreateSimplePrototype(CreatePrototype, adapts=sd.CreateSimplePrototype):
    @classmethod
    def represent_command(cls, data):
        result = super().represent_command(data)
        result['prototype_data'] = data.prototype_data
        return result


class CreateNamedPrototype(NamedPrototypeCommand, adapts=s_named.CreateNamedPrototype):
    pass


class RenameNamedPrototype(NamedPrototypeCommand, adapts=s_named.RenameNamedPrototype):
    pass


class RebaseNamedPrototype(NamedPrototypeCommand, adapts=s_inheriting.RebaseNamedPrototype):
    pass


class AlterNamedPrototype(NamedPrototypeCommand, adapts=s_named.AlterNamedPrototype):
    pass


class AlterPrototypeProperty(Command, adapts=sd.AlterPrototypeProperty):
    @staticmethod
    def _is_derived_ref(value):
        return (isinstance(value, s_obj.PrototypeRef) and
                type(value) is not s_obj.PrototypeRef)

    @classmethod
    def _reduce_refs(cls, value):
        if isinstance(value, (typed.AbstractTypedSequence,
                              typed.AbstractTypedSet,
                              list, set)):
            result = []
            for item in value:
                if cls._is_derived_ref(item):
                    item = s_obj.PrototypeRef(
                                prototype_name=item.prototype_name)
                result.append(item)

        elif isinstance(value, typed.AbstractTypedMapping):
            result = {}
            for key, item in value.items():
                if cls._is_derived_ref(item):
                    item = s_obj.PrototypeRef(
                                prototype_name=item.prototype_name)
                result[key] = item

        elif cls._is_derived_ref(value):
            result = s_obj.PrototypeRef(
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


class DeleteNamedPrototype(NamedPrototypeCommand, adapts=s_named.DeleteNamedPrototype):
    pass


class AttributeCommand(PrototypeCommand, adapts=s_attrs.AttributeCommand):
    pass


class AttributeValueCommand(PrototypeCommand, adapts=s_attrs.AttributeValueCommand):
    pass


class ConstraintCommand(PrototypeCommand, adapts=s_constr.ConstraintCommand):
    pass


class SourceIndexCommand(PrototypeCommand, adapts=s_indexes.SourceIndexCommand):
    pass


class CreateAttribute(AttributeCommand, adapts=s_attrs.CreateAttribute):
    pass


class RenameAttribute(AttributeCommand, adapts=s_attrs.RenameAttribute):
    pass


class AlterAttribute(AttributeCommand, adapts=s_attrs.AlterAttribute):
    pass


class DeleteAttribute(AttributeCommand, adapts=s_attrs.DeleteAttribute):
    pass


class CreateAttributeValue(AttributeValueCommand, adapts=s_attrs.CreateAttributeValue):
    pass


class RenameAttributeValue(AttributeValueCommand, adapts=s_attrs.RenameAttributeValue):
    pass


class AlterAttributeValue(AttributeValueCommand, adapts=s_attrs.AlterAttributeValue):
    pass


class DeleteAttributeValue(AttributeValueCommand, adapts=s_attrs.DeleteAttributeValue):
    pass


class CreateConstraint(ConstraintCommand, adapts=s_constr.CreateConstraint):
    pass


class RenameConstraint(ConstraintCommand, adapts=s_constr.RenameConstraint):
    pass


class AlterConstraint(ConstraintCommand, adapts=s_constr.AlterConstraint):
    pass


class DeleteConstraint(ConstraintCommand, adapts=s_constr.DeleteConstraint):
    pass


class CreateAtom(CreateNamedPrototype, adapts=s_atoms.CreateAtom):
    pass


class RenameAtom(RenameNamedPrototype, adapts=s_atoms.RenameAtom):
    pass


class RebaseAtom(RebaseNamedPrototype, adapts=s_atoms.RebaseAtom):
    pass


class AlterAtom(AlterNamedPrototype, adapts=s_atoms.AlterAtom):
    pass


class DeleteAtom(DeleteNamedPrototype, adapts=s_atoms.DeleteAtom):
    pass


class CreateSourceIndex(SourceIndexCommand, adapts=s_indexes.CreateSourceIndex):
    pass


class RenameSourceIndex(SourceIndexCommand, adapts=s_indexes.RenameSourceIndex):
    pass


class AlterSourceIndex(SourceIndexCommand, adapts=s_indexes.AlterSourceIndex):
    pass


class DeleteSourceIndex(SourceIndexCommand, adapts=s_indexes.DeleteSourceIndex):
    pass


class CreateConcept(CreateNamedPrototype, adapts=s_concepts.CreateConcept):
    pass


class RenameConcept(RenameNamedPrototype, adapts=s_concepts.RenameConcept):
    pass


class RebaseConcept(RebaseNamedPrototype, adapts=s_concepts.RebaseConcept):
    pass


class AlterConcept(AlterNamedPrototype, adapts=s_concepts.AlterConcept):
    pass


class DeleteConcept(DeleteNamedPrototype, adapts=s_concepts.DeleteConcept):
    pass


class CreateAction(CreateNamedPrototype, adapts=s_policy.CreateAction):
    pass


class RenameAction(RenameNamedPrototype, adapts=s_policy.RenameAction):
    pass


class AlterAction(AlterNamedPrototype, adapts=s_policy.AlterAction):
    pass


class DeleteAction(DeleteNamedPrototype, adapts=s_policy.DeleteAction):
    pass


class CreateEvent(CreateNamedPrototype, adapts=s_policy.CreateEvent):
    pass


class RenameEvent(RenameNamedPrototype, adapts=s_policy.RenameEvent):
    pass


class RebaseEvent(RebaseNamedPrototype, adapts=s_policy.RebaseEvent):
    pass


class AlterEvent(AlterNamedPrototype, adapts=s_policy.AlterEvent):
    pass


class DeleteEvent(DeleteNamedPrototype, adapts=s_policy.DeleteEvent):
    pass


class CreatePolicy(CreateNamedPrototype, adapts=s_policy.CreatePolicy):
    pass


class RenamePolicy(RenameNamedPrototype, adapts=s_policy.RenamePolicy):
    pass


class AlterPolicy(AlterNamedPrototype, adapts=s_policy.AlterPolicy):
    pass


class DeletePolicy(DeleteNamedPrototype, adapts=s_policy.DeletePolicy):
    pass


class CreateLink(CreateNamedPrototype, adapts=s_links.CreateLink):
    pass


class RenameLink(RenameNamedPrototype, adapts=s_links.RenameLink):
    pass


class RebaseLink(RebaseNamedPrototype, adapts=s_links.RebaseLink):
    pass


class AlterLink(AlterNamedPrototype, adapts=s_links.AlterLink):
    pass


class DeleteLink(DeleteNamedPrototype, adapts=s_links.DeleteLink):
    pass


class CreateLinkProperty(CreateNamedPrototype, adapts=s_lprops.CreateLinkProperty):
    pass


class RenameLinkProperty(RenameNamedPrototype, adapts=s_lprops.RenameLinkProperty):
    pass


class RebaseLinkProperty(RebaseNamedPrototype, adapts=s_lprops.RebaseLinkProperty):
    pass


class AlterLinkProperty(AlterNamedPrototype, adapts=s_lprops.AlterLinkProperty):
    pass


class DeleteLinkProperty(DeleteNamedPrototype, adapts=s_lprops.DeleteLinkProperty):
    pass
