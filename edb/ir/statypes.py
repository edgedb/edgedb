#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations
from typing import *

import re

from edb import errors


class Duration:

    _pg_simple_parser = re.compile(r'''
        ^
        \s*
        (
            (?P<sign>(\+|\-)?)
        )
        (
            (?P<hours>\d+)
        )
        :
        (
            (?P<minutes>\d+)?
            (
                :(?P<seconds>\d+)
                (
                    \.(?P<milliseconds>\d{0,3})
                    (?P<microseconds>\d{0,3})
                    (?P<submicro>\d*)
                )?
            )?
        )?
        \s*
        $
    ''', re.X)

    _pg_parser = re.compile(r'''
        (
            (
                \s*
                (?P<hours>(\+|\-)?\d+)
                \s*
                (h|hr|hrs|hour|hours)
                \s*
            )
            |
            (
                \s*
                (?P<minutes>(\+|\-)?\d+)
                \s*
                (m|min|mins|minute|minutes)
                \s*
            )
            |
            (
                \s*
                (?P<milliseconds>(\+|\-)?\d+)
                \s*
                (ms | (millisecon(s|d|ds)?))  # '12 millisecon' is valid
                \s*
            )
            |
            (
                \s*
                (?P<microseconds>(\+|\-)?\d+)
                \s*
                (us | (microsecond(s)?))
                \s*
            )
            |
            (
                \s*
                (?P<seconds>(\+|\-)?\d+)
                (
                    (\s* $) | (\s* (s|sec|secs|second|seconds))
                )
                \s*
            )
        )(?=$ | \d | \s)
        |
        (
            \s*
            (?P<error>.+)
        )
    ''', re.X | re.I)

    _iso_parser = re.compile(r'''
        ^
        PT
        (
            (?P<hours>(\+|\-)?\d+) H
        )?
        (
            (?P<minutes>(\+|\-)?\d+) M
        )?
        (
            (
                (?P<secsign>\+|\-)?
                (?P<seconds>\d+)
                (
                    \.
                    (?P<microseconds>\d+)
                )?
            ) S
        )?
        $
    ''', re.X)

    _value: int  # microseconds

    def __init__(
        self,
        pg_text: str = '',
        /,
        *,
        microseconds: Optional[int] = None
    ) -> None:
        if pg_text == '' and microseconds is not None:
            self._value = microseconds
        else:
            self._value = self._us_from_pg_text(pg_text)

    def _us_from_pg_text(self, input: str, /) -> int:
        try:
            seconds = int(input)
        except ValueError:
            pass
        else:
            return seconds * 1000 * 1000

        m = self._pg_simple_parser.match(input)
        if m is not None:
            value = 0
            parsed = m.groupdict()
            if parsed['hours']:
                hours = int(parsed['hours'])
                if 0 <= hours <= 2147483647:
                    value += hours * 3600_000_000
                else:
                    raise errors.NumericOutOfRangeError(
                        'interval field value out of range')
            if parsed['minutes']:
                mins = int(parsed['minutes'])
                if 0 <= mins <= 59:
                    value += mins * 60_000_000
                else:
                    raise errors.NumericOutOfRangeError(
                        'interval field value out of range')
            if parsed['seconds']:
                secs = int(parsed['seconds'])
                if 0 <= secs <= 59:
                    value += secs * 1_000_000
                else:
                    raise errors.NumericOutOfRangeError(
                        'interval field value out of range')
            if parsed['milliseconds']:
                value += int(parsed['milliseconds']) * 1_000
            if parsed['microseconds']:
                value += int(parsed['microseconds'].ljust(3, '0'))
            if parsed['submicro'] and int(parsed['submicro'][:1]) >= 5:
                value += 1
            if parsed['sign'] == '-':
                value = -value

            return value

        if (parsed_iso := self._parse_iso8601(input)) is not None:
            return parsed_iso

        value = 0
        seen: set[str] = set()
        for m in self._pg_parser.finditer(input):
            filtered = {
                k: v for k, v in m.groupdict().items()
                if v is not None
            }
            if len(filtered) != 1:
                raise errors.InvalidValueError(
                    'invalid input syntax for type std::duration')

            kind, val = next(iter(filtered.items()))
            if kind == 'error':
                raise errors.InvalidValueError(
                    f'invalid input syntax for type std::duration: '
                    f'unable to parse {val!r}')
            if kind in seen:
                raise errors.InvalidValueError(
                    f'invalid input syntax for type std::duration: '
                    f'the {kind!r} component has been specified '
                    f'more than once')
            seen.add(kind)

            intval = int(val)
            if kind == 'hours':
                value += intval * 3600_000_000
            elif kind == 'minutes':
                value += intval * 60_000_000
            elif kind == 'seconds':
                value += intval * 1_000_000
            elif kind == 'milliseconds':
                value += intval * 1_000
            elif kind == 'microseconds':
                value += intval

        return value

    @classmethod
    def _parse_iso8601(cls, input: str, /) -> Optional[int]:
        m = cls._iso_parser.match(input)
        if not m:
            return None

        value = 0
        if m['hours']:
            value += int(m['hours']) * 3600_000_000
        if m['minutes']:
            value += int(m['minutes']) * 60_000_000

        secsign = -1 if m['secsign'] == '-' else +1
        if m['seconds']:
            value += int(m['seconds']) * 1_000_000 * secsign
        if m['microseconds']:
            ms = m['microseconds'][:6]
            ms = ms.ljust(6, '0')
            value += int(ms) * secsign

        return value

    @classmethod
    def from_iso8601(cls, input: str, /) -> Duration:
        val = cls._parse_iso8601(input)
        if val is None:
            raise errors.InvalidValueError(
                f'invalid input syntax for type std::duration: '
                f'cannot parse {input!r} as ISO 8601')
        return cls(microseconds=val)

    @classmethod
    def from_microseconds(cls, input: int, /) -> Duration:
        return cls(microseconds=input)

    def to_microseconds(self) -> int:
        return self._value

    def to_iso8601(self) -> str:
        neg = '-' if self._value < 0 else ''
        seconds, usecs = divmod(abs(self._value), 1_000_000)
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        ret = ['PT']
        if hours:
            ret.append(f'{neg}{hours}H')
        if minutes:
            ret.append(f'{neg}{minutes}M')
        if seconds or usecs:
            if usecs:
                ret.append(f"{neg}{seconds}.")
                ret.append(f"{str(usecs).rjust(6, '0')}"[:6].rstrip('0'))
            else:
                ret.append(f'{neg}{seconds}')
            ret.append('S')
        if ret == ['PT']:
            ret.append('0S')
        return ''.join(ret)

    def __repr__(self) -> str:
        return f'<statypes.Duration {self.to_iso8601()!r}>'
