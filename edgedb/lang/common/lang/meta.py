##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class LanguageMeta(type):
    languages = []

    def __new__(cls, name, bases, dct):
        lang = super(LanguageMeta, cls).__new__(cls, name, bases, dct)
        if 'recognize_file' in dct:
            LanguageMeta.languages.append(lang)
        return lang

    @staticmethod
    def recognize_file(filename, try_append_extension=False, is_package=False):
        for lang in LanguageMeta.languages:
            myfile = lang.recognize_file(filename, try_append_extension, is_package)
            if myfile:
                return (lang, myfile)
        return None


class Language(object, metaclass=LanguageMeta):
    pass


class SourcePoint(object):
    def __init__(self, line, column, pointer):
        self.line = line
        self.column = column
        self.pointer = pointer


class SourceContext(object):
    def __init__(self, name, buffer, start, end, document=None):
        self.name = name
        self.buffer = buffer
        self.start = start
        self.end = end
        self.document = document

    def __str__(self):
        return '%s line:%d col:%d' % (self.name, self.start.line, self.start.column)


class ObjectError(Exception):
    def __init__(self, msg, context=None, code=None, note=None):
        self.msg = msg
        self.context = context
        self.code = code
        self.note = note

    def __str__(self):
        return self.msg


class Object(object):
    def __init__(self, context, data):
        self.context = context
        self.data = data

    def construct(self):
        pass


class DocumentContext(object):
    def __init__(self, module=None, import_context=None):
        self.module = module
        self.import_context = import_context
        self.imports = {}
