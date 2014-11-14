##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import base64
import functools
import hashlib
import importlib
import itertools
import logging
import sys
import types

from metamagic.utils.algos import topological
from metamagic.utils.datastructures import OrderedSet
from metamagic.utils.functional import adapter
from metamagic.utils.lang.import_.loader import SourceLoader

from . import exceptions


class RuntimeAdapterDefinitionError(exceptions.DefinitionError):
    pass


class LanguageRuntimeMeta(type):
    runtimes = {}
    default_runtimes = {}

    def __new__(mcls, name, bases, dct, *, languages=None, abstract=False,
                                                           default=False):
        runtime = super().__new__(mcls, name, bases, dct)
        runtime._name_suffix = mcls._get_runtime_name_suffix(runtime)
        runtime._modcache = set()

        if (languages is not None and
                not isinstance(languages, (tuple, list, set))):
            languages = [languages]

        if not abstract and languages:
            for language in languages:
                try:
                    runtimes_for_lang = mcls.runtimes[language]
                except KeyError:
                    runtimes_for_lang = mcls.runtimes[language] = []

            runtimes_for_lang.append(runtime)

        runtime.abstract = abstract

        if default and languages:
            for language in languages:
                try:
                    ex_def_runtime = mcls.default_runtimes[language]
                except KeyError:
                    mcls.default_runtimes[language] = runtime
                else:
                    ex_runtime_name = '{}.{}'.format(
                        ex_def_runtime.__module__, ex_default_runtime.__name__)
                    lang_name = '{}.{}'.format(
                        language.__module__, language.__name__)
                    msg = ('"{}" has already been registered as a default ' +
                           'runtime for "{}"')
                    raise RuntimeError(msg.format(ex_runtime_name, lang_name))

        return runtime

    def __init__(cls, name, bases, dct, *, languages=None, abstract=False,
                                                           default=False):
        super().__init__(name, bases, dct)

    @classmethod
    def get_default_runtime(mcls, language):
        for lang in language.__mro__:
            try:
                return mcls.default_runtimes[lang]
            except KeyError:
                pass

    @classmethod
    def _get_runtime_name_suffix(mcls, cls):
        name = '{}.{}'.format(cls.__module__, cls.__name__)
        name_sig = hashlib.md5(name.encode()).digest()
        name_hash = base64.urlsafe_b64encode(name_sig).decode()
        name_hash = name_hash.strip('=').replace('-', '_')
        return '{}_{}'.format(name_hash, cls.__name__)


class LanguageRuntime(metaclass=LanguageRuntimeMeta, abstract=True):
    logger = logging.getLogger('metamagic.lang.runtime')

    @classmethod
    def new_derivative(cls, mod):
        raise NotImplementedError

    @classmethod
    def get_derivative_mod_name(cls, module):
        return '{}:{}'.format(module.__name__, cls._name_suffix)

    @classmethod
    def obtain_derivative(cls, module, constructor=None):
        if constructor is None:
            constructor = cls.new_derivative
        else:
            constructor = functools.partial(constructor, runtime=cls)

        try:
            derivatives = module.__mm_runtime_derivatives__
        except AttributeError:
            derivatives = module.__mm_runtime_derivatives__ = {}

        try:
            derivative = derivatives[cls]
        except KeyError:
            try:
                derivative = constructor(module)
            except NotImplementedError:
                derivative = None
            else:
                # Copy over parent's module tags and resource parent
                # link so that the derivative is published properly.
                #
                try:
                    derivative.__mm_module_tags__ = \
                                frozenset(module.__mm_module_tags__)
                except AttributeError:
                    pass

                derivative.__mm_runtime__ = cls

        return derivative

    @classmethod
    def add_derivative(cls, module, derivative):
        try:
            derivatives = module.__mm_runtime_derivatives__
        except AttributeError:
            derivatives = module.__mm_runtime_derivatives__ = {}

        derivatives[cls] = derivative

        return derivative

    @classmethod
    def get_derivative(cls, module, consider_inheritance=True):
        try:
            derivatives = module.__mm_runtime_derivatives__
        except AttributeError:
            return None

        if consider_inheritance:
            runtimes = [r for r in cls.__mro__
                          if issubclass(r, LanguageRuntime)]
        else:
            runtimes = (cls,)

        for runtime in runtimes:
            try:
                return derivatives[runtime]
            except KeyError:
                pass

    @classmethod
    def get_derivative_constructor(cls, module, adapters):
        constructors = set()

        if adapters:
            for adapter in adapters:
                try:
                    constructor = adapter.new_derivative
                except AttributeError:
                    pass
                else:
                    if callable(constructor):
                        constructors.add(adapter)

        if len(constructors) == 0:
            # No custom constructor
            return None
        elif len(constructors) == 1:
            # Single constructor, OK
            return list(constructors)[0].new_derivative
        else:
            # Too many constructors defined, ambiguous
            msg = 'multiple derivative constructors'
            details = ('new_derivative() is defined in multiple runtime ' +
                      'adapters for {}: {}').format(
                      type(module), ', '.join(str(a) for a in constructors))
            raise RuntimeAdapterDefinitionError(msg, details=details)

    @classmethod
    def load_module(cls, module):
        derivatives = {}
        code_counter = {}

        mod_adapters = cls.get_adapters(module)

        if mod_adapters:
            constructor = cls.get_derivative_constructor(module, mod_adapters)

            for mod_adapter_cls in mod_adapters:
                runtime = mod_adapter_cls.runtime

                derivative_tag = runtime.get_adapter_tag(mod_adapter_cls)

                try:
                    derivative = derivatives[runtime]
                except KeyError:
                    derivative = runtime.obtain_derivative(
                                    module, constructor=constructor)
                    derivatives[runtime] = derivative

                if not derivative.has_derivative_tag(derivative_tag):
                    mod_adapter = mod_adapter_cls(module)

                    source = mod_adapter.get_source()
                    if source:
                        source = source.encode('utf-8')

                    if source:
                        try:
                            derivative.__sx_resource_source_value__ += source
                        except AttributeError:
                            derivative.__sx_resource_source_value__ = source

                    deps = mod_adapter.get_dependencies()
                    if deps:
                        derivative.__mm_imported_modules__.update(deps)

                    runtime_deps = mod_adapter.get_runtime_dependencies()
                    if runtime_deps:
                        derivative.__mm_runtime_dependencies__.update(
                                                                runtime_deps)

                    derivative.add_derivative_tag(derivative_tag)

                    try:
                        code_counter[runtime].append(source or b'')
                    except KeyError:
                        code_counter[runtime] = [source or b'']
        else:
            attr_derivatives = {}
            classes = {}
            rest = {}

            for attr_name, attr in module.__dict__.items():
                if (isinstance(attr, type) and
                        attr.__module__ == module.__name__):
                    classes[attr_name] = attr
                else:
                    rest[attr_name] = attr

            classes_set = set(classes.values())
            class_names = {c.__name__ for c in classes_set}

            class_g = {}

            for attr_name, attr in classes.items():
                in_module_parents = set(attr.__mro__[1:]) & classes_set
                deps = {p.__name__ for p in in_module_parents}
                explicit_deps = getattr(attr, '__mm_dependencies__', None)
                if explicit_deps:
                    explicit_deps = set(explicit_deps) & class_names
                    deps.update(explicit_deps)

                class_g[attr.__name__] = {'item': attr, 'deps': deps}

            sorted_classes = list(topological.sort(class_g))

            dct = itertools.chain([(c.__name__, c) for c in sorted_classes],
                                  rest.items())

            for attr_name, attr in dct:
                if isinstance(attr, types.ModuleType):
                    continue

                if (isinstance(attr, type)
                        and attr.__module__ != module.__name__):
                    continue

                adapters = cls.get_adapters(attr)
                if not adapters:
                    continue

                constructor = cls.get_derivative_constructor(module, adapters)

                for adapter_cls in adapters:
                    runtime = adapter_cls.runtime

                    try:
                        derivative = attr_derivatives[runtime]
                    except KeyError:
                        derivative = runtime.obtain_derivative(
                                        module, constructor=constructor)
                        attr_derivatives[runtime] = derivative

                    derivative_tag = (runtime.get_adapter_tag(adapter_cls) +
                                        '__attr__' + attr_name)

                    if not derivative.has_derivative_tag(derivative_tag):
                        adapter = adapter_cls(module, attr_name, attr)

                        source = adapter.get_source()
                        if source:
                            source = source.encode('utf-8')

                            try:
                                derivative.__sx_resource_source_value__ += \
                                                                source
                            except AttributeError:
                                derivative.__sx_resource_source_value__ = \
                                                                source

                        deps = adapter.get_dependencies()
                        if deps:
                            derivative.__mm_imported_modules__.update(deps)

                        runtime_deps = adapter.get_runtime_dependencies()
                        if runtime_deps:
                            derivative.__mm_runtime_dependencies__.update(
                                        runtime_deps)

                        derivative.add_derivative_tag(derivative_tag)

                        try:
                            code_counter[runtime].append(source or b'')
                        except KeyError:
                            code_counter[runtime] = [source or b'']

            # Put implicit dependencies between attribute
            # derivatives based on runtime hierarchy.
            attr_runtimes = set(attr_derivatives)
            for runtime, derivative in attr_derivatives.items():
                for parent in set(runtime.__mro__[1:]) & attr_runtimes:
                    derivative.__mm_imported_modules__.add(
                                        attr_derivatives[parent])

            derivatives.update(attr_derivatives)

        if code_counter:
            bits = []

            for runtime, sources in code_counter.items():
                bits.append('{} derivative bit{} ({} bytes) for {}'.format(
                    len(sources), 's' if len(sources) > 1 else '',
                    sum(len(s) for s in sources), runtime.__name__
                ))

            cls.logger.debug('import: {}: generated {}'.format(
                module.__name__, ', '.join(bits)
            ))

        for runtime, derivative in derivatives.items():
            sources = getattr(derivative, '__sx_resource_source_value__', None)
            if sources or isinstance(derivative, EmptyDerivative):
                runtime.add_derivative(module, derivative)

        return module

    @classmethod
    def get_adapter_tag(cls, adapter):
        return '{}.{}'.format(adapter.__module__, adapter.__name__)

    @classmethod
    def _get_adapters(cls, value):
        for runtime in [r for r in cls.__mro__
                          if issubclass(r, LanguageRuntime) and not r.abstract]:
            adapters = LanguageRuntimeAdapterMeta.get_adapter(
                                        value, runtime=runtime)
            if adapters:
                return adapters, runtime

        return (None, None)

    @classmethod
    def get_adapters(cls, value):
        return cls._get_adapters(value)[0]


class LanguageRuntimeAdapterMeta(adapter.MultiAdapter):
    def __new__(mcls, name, bases, clsdict, *,
                      runtime, adapts=None, pure=True, **kwargs):
        clsdict['runtime'] = runtime
        return super().__new__(mcls, name, bases, clsdict, adapts=adapts,
                               pure=pure, adapterargs={'runtime': runtime},
                               **kwargs)

    def __init__(cls, name, bases, clsdict, *,
                      runtime, adapts=None, pure=True, **kwargs):
        super().__init__(name, bases, clsdict, adapts=adapts, pure=pure,
                         adapterargs={'runtime': runtime}, **kwargs)


class LanguageRuntimeAdapter(metaclass=LanguageRuntimeAdapterMeta,
                             runtime=None):
    def __init__(self, module, attr_name=None, attr_value=None):
        self.module = module
        self.attr_name = attr_name
        self.attr_value = attr_value

    def collect_candidate_imports(self):
        try:
            imports = [sys.modules[m] for m in self.module.__sx_imports__]
        except AttributeError:
            imports = ()

        return imports

    def collect_compatible_imports(self):
        new_imports = OrderedSet()

        imports = self.collect_candidate_imports()
        for impmod in imports:
            deriv = load_module_for_runtime(impmod.__name__, self.runtime)
            if deriv is not None:
                new_imports.add(deriv)

        return new_imports

    def get_dependencies(self):
        return self.collect_compatible_imports()

    def get_runtime_dependencies(self):
        return OrderedSet()

    def get_source(self):
        return None


class RuntimeDerivative(types.ModuleType):
    def __init__(self, name):
        super().__init__(name)
        self._derivative_tags = set()
        self.__mm_imported_modules__ = set()
        self.__mm_runtime_dependencies__ = set()

    def add_derivative_tag(self, tag):
        self._derivative_tags.add(tag)

    def has_derivative_tag(self, tag):
        return tag in self._derivative_tags


class EmptyDerivative(RuntimeDerivative):
    pass


class DynamicallyGeneratedDerivative(EmptyDerivative):
    """A derivative that is generated at runtime"""

    def build(self):
        raise NotImplementedError


class ActiveDerivative:
    pass


def get_compatible_runtimes(module, tags=None, consider_derivatives=False,
                                               include_ancestors=False):
    """Return a set of runtimes this module is compatible with"""

    if tags is None:
        try:
            tags = module.__mm_module_tags__
        except AttributeError:
            pass

    runtimes = set()

    try:
        runtimes.add(module.__mm_runtime__)
    except AttributeError:
        pass

    lang = getattr(module, '__language__', None)

    if lang:
        default_runtime = LanguageRuntimeMeta.get_default_runtime(lang)

        if tags:
            for tag in tags:
                try:
                    tag_runtimes = tag.target_runtimes
                except AttributeError:
                    pass
                else:
                    if default_runtime:
                        for tag_runtime in tag_runtimes:
                            if issubclass(tag_runtime, default_runtime):
                                runtimes.add(tag_runtime)

        if not runtimes and default_runtime:
            runtimes.add(default_runtime)

    if consider_derivatives:
        derivatives = getattr(module, '__mm_runtime_derivatives__', {})
        runtimes.update(derivatives.keys())

    if runtimes and include_ancestors:
        runtimes = {c for r in runtimes
                      for c in r.__mro__ if issubclass(c, LanguageRuntime)
                                        and not getattr(c, 'abstract', False)}

    return runtimes


def runtimes_compatible(runtimes1, runtimes2):
    """Check if a set of runtimes is satisfied by another set"""

    if not runtimes1:
        return True

    runtimes2 = {c for r2 in runtimes2 for c in r2.__mro__
                                       if not getattr(c, 'abstract', False)}

    # For each runtime of module1 there exists at least
    # one subclass in runtimes of module2, or vice-versa.
    return all({c for c in r1.__mro__ if not getattr(c, 'abstract', False)}
                & runtimes2 for r1 in runtimes1)


def get_runtime_map(runtimes, module, tags=()):
    """Return a mapping of module and its derivatives that satisfy
    a specified set of runtimes"""

    result = {}
    modruntimes = get_compatible_runtimes(module, tags=tags,
                                          include_ancestors=True)

    runtimes_full = []

    for runtime in runtimes:
        runtimes_full.append([c for c in runtime.__mro__
                                if not getattr(c, 'abstract', False)])

    used = set()

    for i, runtime_mro in enumerate(runtimes_full):
        if i not in used:
            derivative = runtime_mro[0].get_derivative(module)
            if derivative is not None:
                result[derivative] = runtime_mro[0]

    for i, runtime_mro in enumerate(runtimes_full):
        if set(runtime_mro) & modruntimes and i not in used:
            try:
                result[module].add(runtime_mro[0])
            except KeyError:
                result[module] = {runtime_mro[0]}

            used.add(i)

    return result


def load_module_for_runtimes(modname, runtimes):
    # Make sure the module is imported regularly first
    try:
        mod = sys.modules[modname]
    except KeyError:
        mod = importlib.import_module(modname)

    try:
        loaded_runtimes = mod.__mm_loaded_runtimes__
    except AttributeError:
        loaded_runtimes = mod.__mm_loaded_runtimes__ = set()

    runtimes_to_load = set(runtimes) - loaded_runtimes

    if runtimes_to_load:
        for runtime in runtimes:
            runtime.load_module(mod)
            loaded_runtimes.add(runtime)

        for dep in getattr(mod, '__sx_imports__', ()):
            if SourceLoader.is_deptracked(dep):
                load_module_for_runtimes(dep, runtimes)

    return get_runtime_map(runtimes, mod)


def load_module_for_runtime(modname, runtime):
    result = load_module_for_runtimes(modname, (runtime,))
    if result:
        return next(iter(result))


class WebRuntime(LanguageRuntime):
    """Base runtime for Web resources (stylesheets, media, etc)"""
