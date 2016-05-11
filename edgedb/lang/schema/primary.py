##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib
import types

from metamagic.utils.nlang import morphology
from metamagic.utils.functional import get_safe_attrname

from . import name as sn
from . import objects as so
from . import referencing
from . import schema as s_schema
from . import types as s_types


class Prototype(referencing.ReferencingPrototype):
    title = so.Field(morphology.WordCombination, default=None, compcoef=0.909)
    description = so.Field(str, default=None, compcoef=0.909)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._class_template = None

    def __getstate__(self):
        state = super().__getstate__()
        state['_class_template'] = None
        return state

    def _clear_caches(self):
        self._class_template = None

    def get_class_template(self, proto_schema):
        """
            Return a tuple (name, bases, classdict, metaclass) to be used to
            construct a class representing a schema object
        """
        if self._class_template is None:
            bases = self.get_class_base()

            dct = {'__module__': self.name.module, '__sx_prototype__': self,
                   '__sx_protoschema__': proto_schema}

            name = self.get_class_name(proto_schema)

            self._class_template = (name, bases, dct,
                                    self.get_metaclass(proto_schema))

        return self._class_template

    def get_metaclass(self, schema):
        return type

    def get_class_name(self, schema):
        return get_safe_attrname(self.name.name, dir(types.ModuleType))

    def get_mro(self, full_mro=False):
        if full_mro:
            bases = self.get_class_base()
        else:
            bases = self.bases or tuple()

        mros = [[self]]

        for base in bases:
            if isinstance(base, so.BasePrototype):
                mros.append(base.get_mro(full_mro=full_mro))
            else:
                if full_mro:
                    mros.append(list(base.__mro__))
                else:
                    mros.append([base])

        return self._merge_mro(mros)

    def get_class_base(self):
        bases = list(self.bases) if self.bases is not None else []

        impl = s_types.BaseTypeMeta.get_implementation(self.name)
        if impl is not None:
            bases.append(impl)

        mixins = s_types.BaseTypeMeta.get_mixins(self.name)
        if mixins:
            bases.extend(mixins)

        return tuple(bases)

    def get_type_property(self, name, schema):
        from . import lproperties as lprops

        if name == 'id':
            atom_name = 'metamagic.caos.builtins.int'
        else:
            atom_name = 'metamagic.caos.builtins.str'

        target = schema.get(atom_name)

        return lprops.TypeProperty(source=self, target=target,
                                   name=sn.Name(module='type', name=name))

    def __call__(self, proto_schema, *, session=None, subclass_name=None,
                       attrs=None, metadata=None, cache=True, mixins=None,
                       newargs={}):
        """Produce a class from a given object prototype in a given schema.

        :param proto_schema:
            The prototype schema.

        :param session:
            Session

        :param subclass_name:
            An optional subclass name suffix to append to
            the root class name.

        :param attrs:
            An optional dict with additional class attributes
            (default=None).

        :param metadata:
            Class metadata.

        :param bool cache:
            If True (default), the created class will be cached
            and returned in further calls on the same prototype
            and ``subclass_name''.

        :param mixins:
            An optional list of extra parent classes.

        :param dict newargs:
            An optional mapping of arguments that will be passed
            to class' metaclass new() method.
        """

        name = self.name

        clsname, bases, dct, metaclass = self.get_class_template(proto_schema)

        if session is not None and cache:
            result = session.get_cached_class((name, clsname, subclass_name))

            if result is not None:
                return result

        assert metaclass is not None and metaclass is not type

        from metamagic.caos.schemaloaders import import_ as sl

        global_protoschema = sl.get_global_proto_schema()
        root_cache_in_schema = proto_schema is not global_protoschema

        if cache or subclass_name is not None:
            if root_cache_in_schema:
                rootcls = proto_schema.get_cached_class((name, clsname, None))
            else:
                clsmod = importlib.import_module(self.name.module)
                rootcls = getattr(clsmod, clsname, None)
        else:
            rootcls = None

        if rootcls is None:
            ##
            # Create, initialize and cache the root class.
            #
            rootbases = metaclass.init_root_bases(bases, proto_schema)
            rootcls = metaclass.__new__(metaclass, clsname, rootbases, dct)
            metaclass.init_root_class(rootcls)
            metaclass.new(rootcls)

            if cache:
                if root_cache_in_schema:
                    proto_schema.update_class_cache(
                        (name, clsname, None), rootcls)
                else:
                    setattr(clsmod, clsname, rootcls)

        if session is None and subclass_name is None:
            return rootcls

        if mixins:
            bases = tuple(mixins) + (rootcls,)
        else:
            bases = (rootcls,)

        # Class metadata must always be present and must never be inherited.
        if metadata is None or isinstance(metadata, dict):
            metadata_cls = metaclass.get_metadata_class()
            metadata_init = {'session': session}
            if metadata:
                metadata_init.update(metadata)
            metadata = metadata_cls(**metadata_init)

        metadata.subclass_suffix = subclass_name or 'default'
        metadata.cached = cache
        metadata.mixins = mixins

        subdct = {'_class_metadata': metadata,
                  '__module__': dct['__module__']}
        if attrs:
            subdct.update(attrs)

        subclsname = clsname + '_' + str(subclass_name or 'default')
        result = metaclass.__new__(metaclass, subclsname, bases, subdct)

        # Give the class type an opportunity to perform
        # additional custom construction.
        metaclass.new(result, **newargs)

        if cache and session is not None:
            session.update_class_cache((name, clsname, subclass_name), result)

        return result
