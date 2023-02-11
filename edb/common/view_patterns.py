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

"""Hacky implementation of "view patterns" with Python match

A "view pattern" is one that does some transformation on the data
being matched before attempting to match it. This can be super useful,
as it allows writing "helper functions" for pattern matching.

We provide a class, ViewPattern, that can be subclassed with custom
`match` methods that performs a transformation on the scrutinee,
returning a transformed value or raising NoMatch if a match is not
possible.

For example, you could write:
  @dataclasses.dataclass
  class IntPair:
      lhs: int
      rhs: int

  class sum_view(ViewPattern[int], targets=(IntPair,)):
      @staticmethod
      def match(obj: object) -> int:
          match obj:
              case IntPair(lhs, rhs):
                  return lhs + rhs
          raise view_patterns.NoMatch

and then write code like:

  match IntPair(lhs=10, rhs=15):
      case sum_view(10):
          print("NO!")
      case sum_view(25):
          print("YES!")

----

To understand how this is implemented, we first discuss how pattern
matching a value `v` against a pattern like `C(<expr>)` is performed:
 1. isinstance(v, C) is called. If it is False, the match fails
 2. C.__match_args__ is fetched; it should contain a tuple of
    attribute names to be used for positional matching.
 3. In our case, there should be only one attribute in it, `attr`,
    and v.attr is fetched. If fetching v.attr raises AttributeError,
    the match fails.

Our implementation strategy, then, is:
 a. Overload C's isinstance check by implementing `__instancecheck__`
    in a metaclass. Return True if the instance is an instance of
    one of the target classes.
 b. Make C's __match_args__ `('_view_result_<unique_name>',)`
 c. Arrange for `_view_result_<unique_name>` on the matched object to
    call match and return that value. If match raises NoMatch, transform
    it into AttributeError, so that the match fails.

Calling match from the *getter* lets us avoid the need to save the
value somewhere between steps a and c, but requires us to install one
method per view in the scrutinee's class.

Hopefully Python will add __match__ and we can delete all this code!
"""

from typing import Generic, TypeVar

_T = TypeVar('_T')


class NoMatch(Exception):
    pass


class ViewPatternMeta(type):
    def __new__(mcls, name, bases, clsdict, *, targets=(), **kwargs):
        cls = super().__new__(mcls, name, bases, clsdict, **kwargs)

        @property  # type: ignore
        def _view_result_getter(self):
            try:
                return cls.match(self)
            except NoMatch:
                raise AttributeError

        fname = f'_view_result_{cls.__module__}.{cls.__qualname__}'
        mangled = fname.replace("___", "___3_").replace(".", "___")

        cls.__match_args__ = (mangled,)  # type: ignore
        cls._view_result_getter = _view_result_getter
        cls._targets = targets

        # Install the getter onto all target classes
        for target in targets:
            setattr(target, mangled, _view_result_getter)

        return cls

    def __instancecheck__(self, instance):
        return isinstance(instance, self._targets)


class ViewPattern(Generic[_T], metaclass=ViewPatternMeta):
    __match_args__ = ('result',)
    result: _T

    @classmethod
    def match(cls, obj: object) -> _T:
        raise NoMatch
