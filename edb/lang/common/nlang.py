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


from edb.lang.common.persistent_hash import persistent_hash


class NaturalLanguageObject(object):
    pass


class GrammaticalCategoryMeta(type):
    index = {}

    def __init__(cls, name, bases, dct):
        super(GrammaticalCategoryMeta, cls).__init__(name, bases, dct)

        name = dct.get('name')
        if name:
            idx = GrammaticalCategoryMeta.index
            if name in idx:
                raise Exception(
                    'Grammatical category "%s" is already used by %s.%s' %
                    (name, idx[name].__module__, idx[name].__name__))
            else:
                idx[name] = cls
        else:
            raise Exception('Missing required grammatical category name')


def is_valid_category(category):
    return category in GrammaticalCategoryMeta.index


def form(category, value):
    cls = GrammaticalCategoryMeta.index.get(category)
    if cls:
        return cls(value)
    else:
        raise Exception(
            'reference to an invalid grammatical category: %s' % category)


def forms(mapping):
    for k, v in mapping.items():
        yield form(k, v)


class GrammaticalCategory(object, metaclass=GrammaticalCategoryMeta):
    name = 'category'

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

    def __repr__(self):
        return '<%s %r>' % (self.__class__.name, self.value)

    def __eq__(self, other):
        if isinstance(other, GrammaticalCategory):
            return self.value == other.value
        elif isinstance(other, str):
            return self.value == other
        else:
            return False

    def __hash__(self):
        return hash(self.__class__.name + ':' + self.value)

    def persistent_hash(self):
        return persistent_hash(self.__class__.name + ':' + self.value)


class GrammaticalNumber(GrammaticalCategory):
    name = 'number'

    def format_count(self, count):
        pass


class Singular(GrammaticalNumber):
    name = 'singular'


class Plural(GrammaticalNumber):
    name = 'plural'


class WordCombination(NaturalLanguageObject):
    def __new__(cls, value):
        self = super().__new__(cls)

        if isinstance(value, str):
            value = {Singular(value)}
        elif isinstance(value, dict):
            value = forms(value)

        self.forms = {}

        for form in value:
            assert isinstance(form, GrammaticalCategory), type(form)
            self.forms[form.__class__.name] = form.__class__(form.value)

        self.value = self.forms.get('singular')
        if not self.value:
            self.value = next(iter(self.forms.values()))

        return self

    def __getnewargs__(self):
        return (tuple(self.forms.values()), )

    def __getstate__(self):
        return {}

    def __getattr__(self, attribute):
        value = self.forms.get(attribute)

        if not value:
            raise AttributeError(
                '%s is not defined for %r' % (attribute, self))

        return value

    def as_dict(self):
        return {n: str(v) for n, v in self.forms.items()}

    @classmethod
    def from_dict(cls, dct):
        forms = set()

        for key, value in dct.items():
            forms.add(form(key, value))
        return cls(forms)

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self.value)

    def __eq__(self, other):
        if isinstance(other, WordCombination):
            return self.forms == other.forms
        elif isinstance(other, str):
            return self.value == other
        else:
            return False

    def __hash__(self):
        return hash(frozenset(self.forms.values()))

    def persistent_hash(self):
        return persistent_hash(frozenset(self.forms.values()))

    __mm_serialize__ = as_dict
