##
# Copyright (c) 2008-2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import base64
import hashlib
import functools
import logging
import os

from metamagic.utils.functional import adapter

from .exceptions import LanguageError
from .loader import LanguageSourceFileLoader
from .import_ import finder


class LanguageMeta(type):
    languages = []

    def __new__(cls, name, bases, dct, *, register=True):
        lang = super(LanguageMeta, cls).__new__(cls, name, bases, dct)
        if register:
            LanguageMeta.languages.append(lang)
            finder.update_finders()
        return lang

    def __init__(cls, name, bases, dct, *, register=True):
        super().__init__(name, bases, dct)

    @staticmethod
    def recognize_file(filename, try_append_extension=False, is_package=False):
        result = None

        for lang in LanguageMeta.languages:
            file_ = lang.recognize_file(filename, try_append_extension, is_package)
            if file_:
                if result is not None:
                    raise ImportError('ambiguous module import: %s, languages in conflict: %s' % \
                                                (filename, (lang, result[0])))
                result = (lang, file_)

        return result

    def get_loader(cls):
        return cls.loader

    @classmethod
    def get_loaders(cls):
        for lang in LanguageMeta.languages:
            yield (functools.partial(lang.loader, language=lang),
                   ['.' + ext for ext in lang.file_extensions])


class Language(metaclass=LanguageMeta, register=False):
    loader = LanguageSourceFileLoader
    file_extensions = ()
    proxy_module_cls = None

    @classmethod
    def recognize_file(cls, filename, try_append_extension=False, is_package=False):
        if is_package:
            filename = os.path.join(filename, '__init__')

        if try_append_extension:
            for ext in cls.file_extensions:
                if os.path.exists(filename + '.' + ext):
                    return filename + '.' + ext

        elif os.path.exists(filename):
            for ext in cls.file_extensions:
                if filename.endswith('.' + ext):
                    return filename

    @classmethod
    def load_code(cls, stream, context):
        raise NotImplementedError

    @classmethod
    def execute_code(cls, code, context):
        raise NotImplementedError

    @classmethod
    def validate_code(cls, code):
        pass

    @classmethod
    def get_language_version(cls):
        return 0

    @classmethod
    def _get_compatible_runtimes(cls, tags):
        default_runtime = LanguageRuntimeMeta.get_default_runtime(cls)

        runtimes = set()

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

        return runtimes

    @classmethod
    def get_compatible_runtimes(cls, module, tags=None, consider_derivatives=False):
        if module.__language__ is not cls:
            raise ValueError('{} language is not {}'.format(module, cls))

        if tags is None:
            try:
                tags = module.__mm_module_tags__
            except AttributeError:
                pass

        runtimes = set()

        if tags:
            runtimes = cls._get_compatible_runtimes(tags)

        if not runtimes:
            default_runtime = LanguageRuntimeMeta.get_default_runtime(cls)
            if default_runtime:
                runtimes.add(default_runtime)

        if consider_derivatives:
            derivatives = getattr(module, '__mm_runtime_derivatives__', {})
            runtimes.update(derivatives.keys())

        return runtimes

    @classmethod
    def get_target_runtimes(cls, module):
        runtimes = set()

        try:
            tags = module.__mm_module_tags__
        except AttributeError:
            default_runtime = LanguageRuntimeMeta.get_default_runtime(cls)
            if default_runtime is not None:
                runtimes.add(default_runtime)
        else:
            for tag in tags:
                try:
                    tag_runtimes = tag.target_runtimes
                except AttributeError:
                    pass
                else:
                    runtimes.update(tag_runtimes)

        return runtimes

    @classmethod
    def get_derivatives(cls, module):
        derivatives = []

        runtimes = cls.get_target_runtimes(module)
        for runtime in runtimes:
            derivative = runtime.get_derivative(module)
            if derivative:
                derivatives.append(derivative)

        return derivatives


class LanguageRuntimeMeta(type):
    runtimes = {}
    default_runtimes = {}

    def __new__(mcls, name, bases, dct, *, languages=None, abstract=False, default=False):
        runtime = super().__new__(mcls, name, bases, dct)
        runtime._name_suffix = mcls._get_runtime_name_suffix(runtime)
        runtime._modcache = set()

        if languages is not None and not isinstance(languages, (tuple, list, set)):
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
                    ex_runtime_name = '{}.{}'.format(ex_def_runtime.__module__,
                                                     ex_default_runtime.__name__)
                    lang_name = '{}.{}'.format(language.__module__, language.__name__)
                    msg = '"{}" has already been registered as a default runtime for "{}"'
                    raise RuntimeError(msg.format(ex_runtime_name, lang_name))

        return runtime

    def __init__(cls, name, bases, dct, *, languages=None, abstract=False, default=False):
        super().__init__(name, bases, dct)

    @classmethod
    def get_default_runtime(mcls, language):
        return mcls.default_runtimes.get(language)

    @classmethod
    def _get_runtime_name_suffix(mcls, cls):
        name = '{}.{}'.format(cls.__module__, cls.__name__)
        name_hash = base64.urlsafe_b64encode(hashlib.md5(name.encode()).digest()).decode()
        name_hash = name_hash.strip('=').replace('-', '_')
        return '{}_{}'.format(name_hash, cls.__name__)


class LanguageRuntime(metaclass=LanguageRuntimeMeta, abstract=True):
    logger = logging.getLogger('metamagic.lang.runtime')
    compatible_runtimes = ()

    @classmethod
    def new_derivative(cls, mod):
        raise NotImplementedError

    @classmethod
    def get_derivative_mod_name(cls, module):
        return '{}:{}'.format(module.__name__, cls._name_suffix)

    @classmethod
    def obtain_derivative(cls, module):
        try:
            derivatives = module.__mm_runtime_derivatives__
        except AttributeError:
            derivatives = module.__mm_runtime_derivatives__ = {}

        try:
            derivative = derivatives[cls]
        except KeyError:
            try:
                derivative = cls.new_derivative(module)
            except NotImplementedError:
                derivative = None
            else:
                # Copy over parent's module tags and resource parent link so that the
                # derivative is published properly.
                #
                try:
                    derivative.__mm_module_tags__ = frozenset(module.__mm_module_tags__)
                except AttributeError:
                    pass

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
            runtimes = [r for r in cls.__mro__ if issubclass(r, LanguageRuntime)]
        else:
            runtimes = (cls,)

        for runtime in runtimes:
            try:
                return derivatives[runtime]
            except KeyError:
                pass

    @classmethod
    def load_module(cls, module, loader):
        if module.__name__ in cls._modcache:
            return

        cls._modcache.add(module.__name__)

        derivative = cls.obtain_derivative(module)

        if derivative is None:
            return

        new_code_counter = 0

        mod_adapters = cls.get_adapters(module)
        if mod_adapters:
            for mod_adapter in mod_adapters:
                derivative_tag = cls.get_adapter_tag(mod_adapter)

                if not derivative.has_derivative_tag(derivative_tag):
                    source = mod_adapter(module, derivative).get_source()
                    if source:
                        derivative.__sx_resource_append_source__(source.encode('utf-8'))
                    derivative.add_derivative_tag(derivative_tag)
                    new_code_counter += 1
        else:
            for attr_name, attr in module.__dict__.items():
                if not isinstance(attr, type):
                    continue

                adapters = cls.get_adapters(attr)

                if adapters:
                    for adapter in adapters:
                        if attr.__module__ != module.__name__:
                            continue

                        derivative_tag = cls.get_adapter_tag(adapter) + '__attr__' + attr_name

                        if not derivative.has_derivative_tag(derivative_tag):
                            adapter = adapter(module, derivative, attr_name, attr)
                            source = adapter.get_source()
                            if source:
                                derivative.__sx_resource_append_source__(source.encode('utf-8'))
                            derivative.add_derivative_tag(derivative_tag)
                            new_code_counter += 1

        if new_code_counter:
            cls.logger.debug('import: {}: generated {} derivative bit{} ({} bytes) for {}'
                             .format(module.__name__, new_code_counter,
                                     's' if new_code_counter > 1 else '',
                                     len(derivative.__sx_resource_source_value__),
                                     cls.__name__))

        if derivative.__sx_resource_source_value__:
            cls.add_derivative(module, derivative)

        return module

    @classmethod
    def get_adapter_tag(cls, adapter):
        return '{}.{}'.format(adapter.__module__, adapter.__name__)


class LanguageRuntimeAdapterMeta(adapter.MultiAdapter):
    def __new__(mcls, name, bases, clsdict, *, runtime, adapts=None, pure=True, **kwargs):
        return super().__new__(mcls, name, bases, clsdict, adapts=adapts, pure=pure,
                               adapterargs={'runtime': runtime}, **kwargs)

    def __init__(cls, name, bases, clsdict, *, runtime, adapts=None, pure=True, **kwargs):
        super().__init__(name, bases, clsdict, adapts=adapts, pure=pure,
                         adapterargs={'runtime': runtime}, **kwargs)


class RuntimeDerivative:
    def __init__(self):
        self._derivative_tags = set()

    def add_derivative_tag(self, tag):
        self._derivative_tags.add(tag)

    def has_derivative_tag(self, tag):
        return tag in self._derivative_tags


class ObjectError(Exception):
    def __init__(self, msg, context=None, code=None, note=None):
        self.msg = msg
        self.context = context
        self.code = code
        self.note = note

    def __str__(self):
        return self.msg


class Object:
    def __sx_setstate__(self, data):
        pass
